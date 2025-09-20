import logging

from data_to_duckdb import load_matches_into_duckdb
from impact import calculate_impacts
from parse_data import parse_dota_file

matches = parse_dota_file('data.txt')
print(f"Parsed {len(matches)} matches")

conn = load_matches_into_duckdb(matches)
# match(match_id:int64, date_time:timestamp, duration:varchar,duration_sec:int32, radiant_kills:int32, dire_kills:int32, winning_team:varchar)
# player_result(match_id:int64, player_name:varchar, team:varchar, position:int32, net_worth:int32, kills:int32, deaths:int32, assists:int32)

calculate_impacts(conn)
# impact_result(match_id:int64, player_name:varchar, impact:double)

conn.sql("SELECT * FROM impact_result LIMIT 5").show()
conn.sql("SELECT * FROM match LIMIT 5").show()
conn.sql("SELECT * FROM player_result LIMIT 5").show()

conn.sql("""
         SELECT m.match_id,
                m.date_time,
                pr.team,
                pr.player_name,
                pr.position,
                pr.kills   AS k,
                pr.deaths  AS d,
                pr.assists AS a,
                pr.net_worth,
                ir.impact
         FROM match AS m
                  JOIN player_result AS pr USING (match_id)
                  JOIN impact_result AS ir USING (match_id, player_name)
         ORDER BY m.date_time DESC, m.match_id DESC, pr.team, pr.position
         """).show(max_rows=50, max_width=200)  # 50 rows, full column width
