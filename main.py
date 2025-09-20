from data_to_duckdb import load_matches_into_duckdb
from parse_data import parse_dota_file

matches = parse_dota_file('data.txt')
print(f"Parsed {len(matches)} matches")

conn = load_matches_into_duckdb(matches)
conn.sql("SELECT * FROM match LIMIT 5").show()
conn.sql("""
         SELECT m.match_id, m.date_time, p.player_name, p.kills
         FROM player_result p
                  JOIN match m USING (match_id)
         ORDER BY p.kills DESC LIMIT 10
         """).show()
