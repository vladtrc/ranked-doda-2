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
         select m.date_time                           as date_time,
                rr.match_id                           as match_id,
                pr.team                               as team,
                printf('%.2f', any_value(rr.th_skew)) as th_skew,
                printf('%.2f', any_value(rr.pr_skew)) as pr_skew,
                sum(rr.rating_diff)                   as rating_diff,
                any_value(rr.pool)                    as pool
         from rating_result rr
                  join player_result pr
                       on rr.match_id = pr.match_id
                           and rr.player_name = pr.player_name
                  join "match" m
                       on rr.match_id = m.match_id
         group by m.date_time, rr.match_id, pr.team
         order by m.date_time, rr.match_id, pr.team
         """).show(max_rows=100, max_width=200)

sql = """
      SELECT
          m.date_time AS date_time,
          CASE
              WHEN pr.team = 'radiant' AND pr.team = m.winning_team THEN 'RAD W'
              WHEN pr.team = 'radiant' AND pr.team <> m.winning_team THEN 'RAD L'
              WHEN pr.team = 'dire'    AND pr.team = m.winning_team THEN 'DIR W'
              WHEN pr.team = 'dire'    AND pr.team <> m.winning_team THEN 'DIR L'
          END AS team,
          pr.position AS position,
  pr.player_name AS player,
  pr.kills::VARCHAR || '/' || pr.deaths::VARCHAR || '/' || pr.assists::VARCHAR AS kda,
  pr.net_worth AS networth,
  ROUND(ir.impact, 3) AS impact,
  printf('%.2f', rr.th_skew) AS th_skew,
  printf('%.2f', rr.pr_skew) AS pr_skew,
  rr.pool::BIGINT AS pool,
  printf('%.3f', rr.team_share) AS team_share,
  printf('%i -> %i', rr.rating_before::BIGINT, rr.rating_after::BIGINT) AS rating_before_after,
  rr.rating_diff::BIGINT AS rating_diff
      FROM player_result pr
          JOIN match m USING(match_id)
          LEFT JOIN impact_result ir
      ON pr.match_id = ir.match_id AND pr.player_name = ir.player_name
          LEFT JOIN rating_result rr
          ON pr.match_id = rr.match_id AND pr.player_name = rr.player_name
      ORDER BY m.date_time, pr.team, pr.position
      """
conn.sql(sql).show(max_rows=100, max_width=200)

conn.sql("""
CREATE OR REPLACE TABLE leaderboard AS
WITH latest_rating AS (
    SELECT
        rr.player_name,
        arg_max(rr.rating_after, m.date_time) AS rating
    FROM rating_result rr
    JOIN "match" m USING (match_id)
    GROUP BY rr.player_name
),
pos_mode AS (
    SELECT
        player_name,
        arg_max(position, cnt) AS pos
    FROM (
        SELECT player_name, position, COUNT(*) AS cnt
        FROM player_result
        GROUP BY player_name, position
    )
    GROUP BY player_name
),
kda_gold AS (
    SELECT
        pr.player_name,
        CAST(ROUND(AVG(pr.net_worth)) AS BIGINT) AS gold,
        ROUND(AVG(pr.kills), 1)   AS avg_k,
        ROUND(AVG(pr.deaths), 1)  AS avg_d,
        ROUND(AVG(pr.assists), 1) AS avg_a
    FROM player_result pr
    GROUP BY pr.player_name
),
win_stats AS (
    SELECT
        pr.player_name,
        COUNT(*) AS games,
        SUM(CASE WHEN pr.team = m.winning_team THEN 1 ELSE 0 END) AS wins
    FROM player_result pr
    JOIN "match" m USING (match_id)
    GROUP BY pr.player_name
),
assembled AS (
    SELECT
        lr.player_name AS name,
        lr.rating,
        pm.pos,
        kg.gold,
        printf('%.1f/%.1f/%.1f', kg.avg_k, kg.avg_d, kg.avg_a) AS kda,
        printf('%i%%/%i games',
               CAST(ROUND(100.0 * ws.wins / NULLIF(ws.games,0)) AS INT),
               ws.games) AS wins
    FROM latest_rating lr
    LEFT JOIN pos_mode  pm USING (player_name)
    LEFT JOIN kda_gold  kg USING (player_name)
    LEFT JOIN win_stats ws USING (player_name)
)
SELECT
    ROW_NUMBER() OVER (ORDER BY rating DESC) AS N,
    CAST(rating AS BIGINT) AS rating,
    name,
    pos,
    gold,
    kda,
    wins
FROM assembled
ORDER BY rating DESC
LIMIT 50;
""")

conn.sql("SELECT * FROM leaderboard ORDER BY N").show(max_rows=50, max_width=200)

conn.sql("SELECT sum(rating_diff) FROM rating_result").show()

