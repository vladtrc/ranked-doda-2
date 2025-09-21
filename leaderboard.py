from data_to_duckdb import load_matches_into_duckdb
from impact import calculate_impacts
from data_parse import parse_dota_file

from rating import calculate_ranked_mmr

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
select row_number() over (order by WL desc) as n, *
from presentation
order by WL desc;
""")

conn.sql("SELECT * FROM leaderboard ORDER BY n").show(max_rows=50, max_width=200)
