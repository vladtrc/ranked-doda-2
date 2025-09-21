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

# parameter: set to a player name to see only their performance. set to None for all.
name = "de"  # e.g., "Dendi" or None

# ---------- team rating deltas per match ----------
team_sql = f"""
with
  params as (
    select {('null', f"'{name}'")[name is not None]}::varchar as name
  ),

  raw_match as (
    select
      match_id,
      date_time,
      winning_team
    from "match"
  ),

  raw_player as (
    select
      match_id,
      player_name,
      team,
      position,
      net_worth,
      kills,
      deaths,
      assists
    from player_result
  ),

  raw_rating as (
    select
      match_id,
      player_name,
      pool,
      th_skew,
      pr_skew,
      rating_before,
      rating_after,
      rating_diff,
      team_share
    from rating_result
  ),

  join_player_match as (
    select
      p.match_id,
      m.date_time,
      p.team,
      p.player_name
    from raw_player p
    join raw_match m using(match_id)
  ),

  join_player_full as (
    select
      jpm.date_time,
      jpm.match_id,
      jpm.team,
      jpm.player_name,
      rr.pool,
      rr.th_skew,
      rr.pr_skew,
      rr.rating_diff
    from join_player_match jpm
    left join raw_rating rr
      on jpm.match_id = rr.match_id
     and jpm.player_name = rr.player_name
  ),

  filtered as (
    select
      jpf.*
    from join_player_full jpf, params
    where params.name is null or jpf.player_name = params.name
  ),

  agg_team_ratings as (
    select
      date_time,
      match_id,
      team,
      printf('%.2f', any_value(th_skew)) as th_skew,
      printf('%.2f', any_value(pr_skew)) as pr_skew,
      sum(rating_diff)                 as rating_diff,
      any_value(pool)                  as pool
    from filtered
    group by date_time, match_id, team
  )

select
  date_time,
  match_id,
  team,
  th_skew,
  pr_skew,
  rating_diff,
  pool
from agg_team_ratings
order by date_time, match_id, team
"""
conn.sql(team_sql).show(max_rows=100, max_width=200)

# ---------- per-player details (optionally filtered by `name`) ----------
player_sql = f"""
with
  params as (
    select {('null', f"'{name}'")[name is not None]}::varchar as name
  ),

  raw_match as (
    select
      match_id,
      date_time,
      winning_team
    from "match"
  ),

  raw_player as (
    select
      match_id,
      player_name,
      team,
      position,
      net_worth,
      kills,
      deaths,
      assists
    from player_result
  ),

  raw_impact as (
    select
      match_id,
      player_name,
      impact
    from impact_result
  ),

  raw_rating as (
    select
      match_id,
      player_name,
      pool,
      th_skew,
      pr_skew,
      rating_before,
      rating_after,
      rating_diff,
      team_share
    from rating_result
  ),

  join_player_match as (
    select
      m.date_time,
      p.match_id,
      p.team,
      p.position,
      p.player_name,
      p.kills,
      p.deaths,
      p.assists,
      p.net_worth
    from raw_player p
    join raw_match m using(match_id)
  ),

  join_full as (
    select
      jpm.date_time,
      jpm.team,
      jpm.position,
      jpm.player_name,
      jpm.match_id,
      jpm.kills,
      jpm.deaths,
      jpm.assists,
      jpm.net_worth,
      ri.impact,
      rr.pool,
      rr.th_skew,
      rr.pr_skew,
      rr.team_share,
      rr.rating_before,
      rr.rating_after,
      rr.rating_diff,
      rm.winning_team
    from join_player_match jpm
    left join raw_impact ri
      on jpm.match_id = ri.match_id
     and jpm.player_name = ri.player_name
    left join raw_rating rr
      on jpm.match_id = rr.match_id
     and jpm.player_name = rr.player_name
    join raw_match rm
      on jpm.match_id = rm.match_id
  ),

  filtered as (
    select
      jf.*
    from join_full jf, params
    where params.name is null or jf.player_name = params.name
  )

select
  date_time,
  case
    when team = 'radiant' and team = winning_team then 'rad w'
    when team = 'radiant' and team <> winning_team then 'rad l'
    when team = 'dire'    and team = winning_team then 'dir w'
    when team = 'dire'    and team <> winning_team then 'dir l'
  end as team,
  position,
  player_name as player,
  kills::varchar || '/' || deaths::varchar || '/' || assists::varchar as kda,
  net_worth as networth,
  round(impact, 3) as impact,
  printf('%.2f', th_skew) as th_skew,
  printf('%.2f', pr_skew) as pr_skew,
  pool::bigint as pool,
  printf('%.3f', team_share) as team_share,
  printf('%i -> %i', rating_before::bigint, rating_after::bigint) as rating_before_after,
  rating_diff::bigint as rating_diff
from filtered
order by date_time, team, position
"""
conn.sql(player_sql).show(max_rows=50, max_width=200)

# ---------- total rating diff (optionally filtered by `name`) ----------
total_sql = f"""
with
  params as (
    select {('null', f"'{name}'")[name is not None]}::varchar as name
  )
select
  sum(rating_diff) as total_rating_diff,
  avg(rating_diff) as avg_rating_diff
from rating_result rr, params
where params.name is null or rr.player_name = params.name
"""
conn.sql(total_sql).show()
