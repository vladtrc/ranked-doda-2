from data_parse import parse_dota_file
from data_to_duckdb import load_matches_into_duckdb
from impact import calculate_impacts
from rating import calculate_ranked_mmr
from save_table import ReportSection, save_tables_tailwind_html

matches = parse_dota_file('data.txt')
print(f"Parsed {len(matches)} matches")

conn = load_matches_into_duckdb(matches)
# match(match_id:int64, date_time:timestamp, duration:varchar,duration_sec:int32, radiant_kills:int32, dire_kills:int32, winning_team:varchar)
# player_result(match_id:int64, player_name:varchar, team:varchar, position:int32, net_worth:int32, kills:int32, deaths:int32, assists:int32)

calculate_impacts(conn)
# impact_result(match_id:int64, player_name:varchar, impact:double)

calculate_ranked_mmr(conn)
# rating_result(match_id:int64, player_name:varchar, pool:double, th_skew:double, pr_skew:double, rating_before:double, rating_after:double, rating_diff:double)
conn.sql("""
create or replace table leaderboard as
with latest_rating as (
  select rr.player_name, arg_max(rr.rating_after, m.date_time) as rating
  from rating_result rr
  join "match" m using (match_id)
  group by rr.player_name
),
agg_pos_counts as (
  select player_name, position, count(*) as cnt
  from player_result
  group by player_name, position
),
agg_pos_totals as (
  select player_name, sum(cnt) as total
  from agg_pos_counts
  group by player_name
),
pos_props as (
  select c.player_name, c.position, c.cnt, t.total,
         c.cnt * 1.0 / nullif(t.total, 0) as p
  from agg_pos_counts c
  join agg_pos_totals t using (player_name)
),
agg_two_pos as (
  select player_name,
         string_agg(cast(position as varchar), '/' order by position) as two_pos
  from pos_props
  where p >= 0.30
  group by player_name
  having count(*) = 2
),
agg_top_pos as (
  select player_name, arg_max(position, cnt) as top_pos
  from pos_props
  group by player_name
),
pos_mode as (
  select tp.player_name, coalesce(tw.two_pos, cast(tp.top_pos as varchar)) as pos
  from agg_top_pos tp
  left join agg_two_pos tw using (player_name)
),
agg_kda_gold as (
  select pr.player_name,
         cast(round(avg(pr.net_worth)) as bigint) as gold,
         round(avg(pr.kills), 1)   as avg_k,
         round(avg(pr.deaths), 1)  as avg_d,
         round(avg(pr.assists), 1) as avg_a
  from player_result pr
  group by pr.player_name
),
agg_win_stats as (
  select pr.player_name,
         count(*) as games,
         sum(case when pr.team = m.winning_team then 1 else 0 end) as wins
  from player_result pr
  join "match" m using (match_id)
  group by pr.player_name
),
per_match as (
  select pr.player_name, m.date_time,
         case when pr.team = m.winning_team then 'W' else 'L' end as r
  from player_result pr
  join "match" m using (match_id)
),
ranked as (
  select player_name, date_time, r,
         row_number() over (partition by player_name order by date_time desc) as rn
  from per_match
),
agg_form_10 as (
  select player_name, string_agg(r, '' order by date_time) as form
  from ranked
  where rn <= 10
  group by player_name
),
assembled as (
  select lr.player_name as name,
         lr.rating,
         pm.pos,
         kg.gold,
         kg.avg_k, kg.avg_d, kg.avg_a,
         ws.wins as w,
         ws.games as g,
         f.form
  from latest_rating lr
  left join pos_mode pm on pm.player_name = lr.player_name
  left join agg_kda_gold kg on kg.player_name = lr.player_name
  left join agg_win_stats ws on ws.player_name = lr.player_name
  left join agg_form_10 f on f.player_name = lr.player_name
),
-- compute threshold N*
per_player as (
  select name as player_name, g as games from assembled
),
thresholds as (
  select n as N,
         (select count(*) from per_player p where p.games >= n) as players
  from generate_series(1, (select coalesce(max(games),1) from per_player)) as t(n)
),
chosen as (
  -- smallest N where players <= 50; if none, default to 1
  select coalesce( (select N from thresholds where players <= 50 order by N asc limit 1), 1 ) as N
),
presentation as (
  select
    w - (g - w) as WL,
    cast(rating as bigint) as rating,
    name,
    case
      when length(pos) = 1 then printf(' %s ', pos)
      else pos
    end as pos,
    printf('%2i/%2i/%2i %s',
      cast(round(avg_k) as int),
      cast(round(avg_d) as int),
      cast(round(avg_a) as int),
      case
        when gold <= 5000 then '3k'
        when gold <= 15000 then '10k'
        else '20k'
      end
    ) as kda_gold,
    printf('%i%%/%i', cast(round(100.0 * w / nullif(g, 0)) as int), g) as wins,
    coalesce(form, '') as last10
  from assembled
  where g >= (select N from chosen)
)
select 
    row_number() over (order by WL desc) as n, 
    case 
        when WL >= 0 
            then concat('+', cast(WL as varchar))
        else cast(WL as varchar)
    end as WL,
    rating,
    name,
    pos,
    kda_gold,
    wins,
    last10
from presentation
order by WL desc;
""")

# team net worth
conn.sql("""
create or replace table _team_networth as
select match_id, team, sum(net_worth) as team_net_worth
from player_result
group by 1,2;
""")

# per-player with impact and negative_impact
conn.sql("""
create or replace table _per_player as
select
  pr.match_id,
  pr.player_name,
  pr.team,
  pr.position as pos,
  pr.kills as kill,
  pr.assists as assist,
  pr.deaths as death,
  pr.net_worth as networth,
  tm.team_net_worth,
  pr.net_worth / nullif(tm.team_net_worth, 0) as net_worth_percentage,
  m.date_time as finished_at,
  case when pr.team = m.winning_team then 1 else 0 end as is_winner,
  (pr.kills + pr.assists/2.0 + pr.position) / coalesce(nullif(pow(pr.deaths, 0.2), 0), 1) as impact,
  pow(pr.deaths, 1.2) / (coalesce(nullif((pr.kills + pr.assists/2.0 + pr.position), 0), 1) *
                         nullif(pr.net_worth / nullif(tm.team_net_worth,0),0)) as negative_impact
from player_result pr
join _team_networth tm using (match_id, team)
join "match" m using (match_id);
""")

# team sums
conn.sql("""
create or replace table _team_sums as
select match_id, team,
       sum(impact) as team_impact,
       sum(negative_impact) as team_negative_impact
from _per_player
group by 1,2;
""")

# metrics with carried and ruined
conn.sql("""
create or replace table _per_player_metrics as
select
  i.match_id,
  i.player_name,
  i.team,
  i.pos,
  i.kill, i.assist, i.death, i.networth,
  cast(100 * i.impact / nullif(ts.team_impact,0) as int) as carried,
  cast(100 * i.negative_impact / nullif(ts.team_negative_impact,0) as int) as ruined,
  i.finished_at,
  case when i.is_winner=1 then 'W' else 'L' end as result
from _per_player i
join _team_sums ts using (match_id, team);
""")

# Win/Lose streaks per player at each match time
conn.sql("""
create or replace table _streaks as
with seq as (
  select
    pr.player_name,
    m.match_id,
    m.date_time,
    case when pr.team = m.winning_team then 1 else 0 end as is_winner
  from player_result pr
  join "match" m using (match_id)
),
lagged as (
  select *,
         lag(is_winner) over (partition by player_name order by date_time) as prev_w
  from seq
),
chg as (
  select *,
         case when prev_w is null or prev_w <> is_winner then 1 else 0 end as changed
  from lagged
),
grp as (
  select *,
         sum(changed) over (partition by player_name order by date_time
                            rows between unbounded preceding and current row) as grp_id
  from chg
),
lens as (
  select *,
         row_number() over (partition by player_name, grp_id order by date_time desc) as rn_desc,
         count(*)    over (partition by player_name, grp_id) as streak_len
  from grp
)
select player_name, match_id, is_winner, streak_len
from lens
where rn_desc = 1;
""")

# Attach streaks and rating diff
conn.sql("""
create or replace table _raw_extremes as
select
  pm.pos,
  pm.kill,
  pm.assist,
  pm.death,
  pm.networth,
  pm.ruined,
  pm.carried,
  cast(coalesce(rr.rating_diff, 0) as int) as rating_diff,
  case when st.is_winner=1 then st.streak_len end as winstreak,
  case when st.is_winner=0 then st.streak_len end as losestreak,
  pm.player_name as name,
  cast(pm.finished_at as date) as finished_at,
  pm.result
from _per_player_metrics pm
left join _streaks st on st.player_name = pm.player_name and st.match_id = pm.match_id
left join rating_result rr on rr.player_name = pm.player_name and rr.match_id = pm.match_id;
""")

# Aggregate max/min per position and pick latest occurrence on ties
conn.sql("""
create or replace table leaderboard_pos_extremes as
with p as (select distinct pos from _raw_extremes),
agg as (
  select
    pos,
    max(rating_diff) as max_rating_diff,
    min(rating_diff) as min_rating_diff,
    max(winstreak)   as max_winstreak,
    max(losestreak)  as max_losestreak,
    max(kill)        as max_kill,
    min(kill)        as min_kill,
    max(assist)      as max_assist,
    min(assist)      as min_assist,
    min(death)       as min_death,
    max(death)       as max_death,
    max(networth)    as max_networth,
    min(networth)    as min_networth,
    max(carried)     as max_carried,
    min(carried)     as min_carried,
    min(ruined)      as min_ruined,
    max(ruined)      as max_ruined
  from _raw_extremes
  group by pos
),
pick as (
  select
    a.pos,

    -- helpers to format "name | value | result | date"
    (select name || ' | ' || a.max_rating_diff || ' | ' || r.result || ' | ' || cast(r.finished_at as varchar)
     from _raw_extremes r
     where r.pos=a.pos and r.rating_diff=a.max_rating_diff
     order by r.finished_at desc limit 1) as max_rating_diff,

    (select name || ' | ' || a.min_rating_diff || ' | ' || r.result || ' | ' || cast(r.finished_at as varchar)
     from _raw_extremes r
     where r.pos=a.pos and r.rating_diff=a.min_rating_diff
     order by r.finished_at desc limit 1) as min_rating_diff,

    (select name || ' | ' || a.max_winstreak || ' | ' || r.result || ' | ' || cast(r.finished_at as varchar)
     from _raw_extremes r
     where r.pos=a.pos and r.winstreak=a.max_winstreak
     order by r.finished_at desc limit 1) as max_winstreak,

    (select name || ' | ' || a.max_losestreak || ' | ' || r.result || ' | ' || cast(r.finished_at as varchar)
     from _raw_extremes r
     where r.pos=a.pos and r.losestreak=a.max_losestreak
     order by r.finished_at desc limit 1) as max_losestreak,

    (select name || ' | ' || a.max_kill || ' | ' || r.result || ' | ' || cast(r.finished_at as varchar)
     from _raw_extremes r
     where r.pos=a.pos and r.kill=a.max_kill
     order by r.finished_at desc limit 1) as max_kill,

    (select name || ' | ' || a.min_kill || ' | ' || r.result || ' | ' || cast(r.finished_at as varchar)
     from _raw_extremes r
     where r.pos=a.pos and r.kill=a.min_kill
     order by r.finished_at desc limit 1) as min_kill,

    (select name || ' | ' || a.max_assist || ' | ' || r.result || ' | ' || cast(r.finished_at as varchar)
     from _raw_extremes r
     where r.pos=a.pos and r.assist=a.max_assist
     order by r.finished_at desc limit 1) as max_assist,

    (select name || ' | ' || a.min_assist || ' | ' || r.result || ' | ' || cast(r.finished_at as varchar)
     from _raw_extremes r
     where r.pos=a.pos and r.assist=a.min_assist
     order by r.finished_at desc limit 1) as min_assist,

    (select name || ' | ' || a.min_death || ' | ' || r.result || ' | ' || cast(r.finished_at as varchar)
     from _raw_extremes r
     where r.pos=a.pos and r.death=a.min_death
     order by r.finished_at desc limit 1) as min_death,

    (select name || ' | ' || a.max_death || ' | ' || r.result || ' | ' || cast(r.finished_at as varchar)
     from _raw_extremes r
     where r.pos=a.pos and r.death=a.max_death
     order by r.finished_at desc limit 1) as max_death,

    (select name || ' | ' || a.max_networth || ' | ' || r.result || ' | ' || cast(r.finished_at as varchar)
     from _raw_extremes r
     where r.pos=a.pos and r.networth=a.max_networth
     order by r.finished_at desc limit 1) as max_networth,

    (select name || ' | ' || a.min_networth || ' | ' || r.result || ' | ' || cast(r.finished_at as varchar)
     from _raw_extremes r
     where r.pos=a.pos and r.networth=a.min_networth
     order by r.finished_at desc limit 1) as min_networth,

    (select name || ' | ' || a.max_carried || ' | ' || r.result || ' | ' || cast(r.finished_at as varchar)
     from _raw_extremes r
     where r.pos=a.pos and r.carried=a.max_carried
     order by r.finished_at desc limit 1) as max_carried,

    (select name || ' | ' || a.min_carried || ' | ' || r.result || ' | ' || cast(r.finished_at as varchar)
     from _raw_extremes r
     where r.pos=a.pos and r.carried=a.min_carried
     order by r.finished_at desc limit 1) as min_carried,

    (select name || ' | ' || a.min_ruined || ' | ' || r.result || ' | ' || cast(r.finished_at as varchar)
     from _raw_extremes r
     where r.pos=a.pos and r.ruined=a.min_ruined
     order by r.finished_at desc limit 1) as min_ruined,

    (select name || ' | ' || a.max_ruined || ' | ' || r.result || ' | ' || cast(r.finished_at as varchar)
     from _raw_extremes r
     where r.pos=a.pos and r.ruined=a.max_ruined
     order by r.finished_at desc limit 1) as max_ruined

  from agg a
)
select * from pick
order by pos;
""")

# Pretty prints similar to Spark sections
print("Топ игроков по статам:")
conn.sql("SELECT * FROM leaderboard ORDER BY n").show(max_rows=50, max_width=200)

print("Особо отличившиеся")
conn.sql("""
         select pos,
                max_rating_diff,
                max_winstreak,
                max_kill,
                max_assist,
                min_death,
                max_networth,
                max_carried,
                min_ruined
         from leaderboard_pos_extremes
         order by pos
         """).show(max_rows=50, max_width=500)

print("Не особо отличившиеся...")
conn.sql("""
         select pos,
                min_rating_diff,
                max_losestreak,
                min_kill,
                min_assist,
                max_death,
                min_networth,
                min_carried,
                max_ruined
         from leaderboard_pos_extremes
         order by pos
         """).show(max_rows=50, max_width=500)

sections = [
    ReportSection(
        title="Топ игроков по статам",
        sql="SELECT * FROM leaderboard ORDER BY n",
        note="Sorted by WL. Dynamic N threshold."
    ),
    ReportSection(
        title="Особо отличившиеся",
        sql="""
            select pos,
                   max_rating_diff,
                   max_winstreak,
                   max_kill,
                   max_assist,
                   min_death,
                   max_networth,
                   max_carried,
                   min_ruined
            from leaderboard_pos_extremes
            order by pos
            """
    ),
    ReportSection(
        title="Не особо отличившиеся...",
        sql="""
            select pos,
                   min_rating_diff,
                   max_losestreak,
                   min_kill,
                   min_assist,
                   max_death,
                   min_networth,
                   min_carried,
                   max_ruined
            from leaderboard_pos_extremes
            order by pos
            """
    ),
]

save_tables_tailwind_html(conn, sections, "dota_report.html", title="Dota Leaderboards")
