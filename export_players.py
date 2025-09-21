import pandas as pd
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
df = conn.sql("""
              select
                  p.player_name as name,
                  string_agg(distinct cast(p.position as varchar), ',') as played_pos,
                  cast(min(m.date_time) as date) as first_match_date,
                  cast(max(m.date_time) as date) as last_match_date,
                  printf('%.1f/%.1f/%.1f',
                         avg(p.kills),
                         avg(p.deaths),
                         avg(p.assists)) as kda
              from player_result p
                       join "match" m using(match_id)
              group by p.player_name
              order by name
              """).to_df()

df.to_csv("players_summary.tsv", sep="\t", index=False)
