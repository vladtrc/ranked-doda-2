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
             -- min/max/avg/std for impact and rating_diff at four groupings
             CREATE OR REPLACE VIEW stats_impact_rating AS
             WITH base AS (
               SELECT position AS pos, team, impact, rating_diff
               FROM v_player_match
             )
             SELECT
                 'overall' AS grp, NULL::INTEGER AS pos, NULL::VARCHAR AS team,
                 COUNT(impact)    AS n_impact,
                 MIN(impact)      AS min_impact,
                 MAX(impact)      AS max_impact,
                 AVG(impact)      AS avg_impact,
                 STDDEV_SAMP(impact) AS std_impact,
                 COUNT(rating_diff)    AS n_rating,
                 MIN(rating_diff)      AS min_rating,
                 MAX(rating_diff)      AS max_rating,
                 AVG(rating_diff)      AS avg_rating,
                 STDDEV_SAMP(rating_diff) AS std_rating
             FROM base
             UNION ALL
             SELECT
                 'by_pos' AS grp, pos, NULL::VARCHAR AS team,
                 COUNT(impact), MIN(impact), MAX(impact), AVG(impact), STDDEV_SAMP(impact),
                 COUNT(rating_diff), MIN(rating_diff), MAX(rating_diff), AVG(rating_diff), STDDEV_SAMP(rating_diff)
             FROM base
             GROUP BY pos
             UNION ALL
             SELECT
                 'by_team' AS grp, NULL::INTEGER AS pos, team,
                 COUNT(impact), MIN(impact), MAX(impact), AVG(impact), STDDEV_SAMP(impact),
                 COUNT(rating_diff), MIN(rating_diff), MAX(rating_diff), AVG(rating_diff), STDDEV_SAMP(rating_diff)
             FROM base
             GROUP BY team
             UNION ALL
             SELECT
                 'by_team_pos' AS grp, pos, team,
                 COUNT(impact), MIN(impact), MAX(impact), AVG(impact), STDDEV_SAMP(impact),
                 COUNT(rating_diff), MIN(rating_diff), MAX(rating_diff), AVG(rating_diff), STDDEV_SAMP(rating_diff)
             FROM base
             GROUP BY pos, team
             """).show(max_rows=50, max_width=200)

# quick browse
conn.sql("""
         SELECT
             date_time,
             CASE
                 WHEN team = 'radiant' AND team = winning_team THEN 'rad w'
                 WHEN team = 'radiant' AND team <> winning_team THEN 'rad l'
                 WHEN team = 'dire'    AND team = winning_team THEN 'dir w'
                 ELSE 'dir l'
                 END AS team_outcome,
             position, player_name,
             kills::VARCHAR || '/' || deaths::VARCHAR || '/' || assists::VARCHAR AS kda,
             impact,
             rating_before::BIGINT AS rb, rating_after::BIGINT AS ra, rating_diff::BIGINT AS rd
         FROM v_player_match
         ORDER BY date_time DESC, team_outcome, position
         """).show(max_rows=50, max_width=200)

