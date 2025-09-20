from data_to_duckdb import load_matches_into_duckdb
from impact import calculate_impacts, debug_impacts
from parse_data import parse_dota_file

matches = parse_dota_file('data.txt')
print(f"Parsed {len(matches)} matches")

conn = load_matches_into_duckdb(matches)

calculate_impacts(conn)
debug_impacts(conn)  # optional

# conn.sql("SELECT * FROM impact_result LIMIT 50").show()

