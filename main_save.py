from data_parse import parse_dota_file, save_to_csv

matches = parse_dota_file('data.txt')
print(f"Parsed {len(matches)} matches")
save_to_csv(matches)