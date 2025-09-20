from data_to_duckdb import load_matches_into_duckdb
from parse_data import parse_dota_file, Match
from dataclasses import asdict
from typing import List
import duckdb

matches = parse_dota_file('data.txt')
print(f"Parsed {len(matches)} matches")

conn = load_matches_into_duckdb(matches)
conn.sql("SELECT * FROM matches LIMIT 5").show()
conn.sql("""
         SELECT m.match_id, m.date_time, p.player_name, p.kills
         FROM players p JOIN matches m USING(match_id)
         ORDER BY p.kills DESC LIMIT 10
         """).show()

