from data_to_duckdb import load_matches_into_duckdb
from impact import calculate_impacts
from parse_data import parse_dota_file

# ---------- ingest ----------
matches = parse_dota_file('data.txt')
print(f"Parsed {len(matches)} matches")
conn = load_matches_into_duckdb(matches)

# match(match_id:int64, date_time:timestamp, duration:varchar,duration_sec:int32, radiant_kills:int32, dire_kills:int32, winning_team:varchar)
# player_result(match_id:int64, player_name:varchar, team:varchar, position:int32, net_worth:int32, kills:int32, deaths:int32, assists:int32)
calculate_impacts(conn)  # impact_result(match_id:int64, player_name:varchar, impact:double)

conn.sql("""
         SELECT m.match_id, m.date_time, pr.team, pr.player_name, pr.position,
                pr.k AS k, pr.d AS d, pr.a AS a, pr.net_worth, ir.impact
         FROM (
                  SELECT match_id, date_time FROM match
              ) m
                  JOIN (
             SELECT match_id, team, player_name, position, kills AS k, deaths AS d, assists AS a, net_worth
             FROM player_result
         ) pr USING (match_id)
                  JOIN impact_result ir USING (match_id, player_name)
         ORDER BY m.date_time DESC, m.match_id DESC, pr.team, pr.position
         """).show(max_rows=50, max_width=200)

# ---------- audits ----------

# 1) Role neutrality: per-role mean ≈ 0
conn.execute("""
             CREATE OR REPLACE VIEW audit_role_neutrality AS
             SELECT
                 pr.position AS role,
                 COUNT(*) AS n,
                 AVG(ir.impact) AS mean_impact,
                 STDDEV_SAMP(ir.impact) AS std_impact,
                 AVG(ABS(ir.impact)) AS mean_abs_impact
             FROM player_result pr
                      JOIN impact_result ir USING (match_id, player_name)
             GROUP BY role
             ORDER BY role;
             """)
conn.sql("SELECT * FROM audit_role_neutrality").show(max_rows=50, max_width=200)

# 2) Bounded scale [-100, 100]
conn.execute("""
             CREATE OR REPLACE VIEW audit_bounds AS
WITH base AS (SELECT impact FROM impact_result)
             SELECT
                 MIN(impact) AS min_impact,
                 MAX(impact) AS max_impact,
                 100.0 * AVG(CASE WHEN impact BETWEEN -100 AND 100 THEN 1 ELSE 0 END) AS pct_in_bounds,
                 100.0 * AVG(CASE WHEN impact < -100 OR impact > 100 THEN 1 ELSE 0 END) AS pct_out_bounds
             FROM base;
             """)
conn.sql("SELECT * FROM audit_bounds").show()

# 3) Distribution shape and tails
conn.execute("""
             CREATE OR REPLACE VIEW audit_distribution AS
WITH base AS (SELECT impact FROM impact_result),
stats AS (
  SELECT COUNT(*) AS n, AVG(impact) AS mu, STDDEV_SAMP(impact) AS sigma FROM base
)
             SELECT
                 MAX(s.n) AS n,
                 MAX(s.mu) AS mu,
                 MAX(s.sigma) AS sigma,
                 100.0 * AVG(CASE WHEN ABS((b.impact - s.mu)/NULLIF(s.sigma,0)) > 3 THEN 1 ELSE 0 END) AS pct_gt_3sigma,
                 quantile_cont(b.impact, 0.01) AS q01,
                 quantile_cont(b.impact, 0.05) AS q05,
                 quantile_cont(b.impact, 0.50) AS q50,
                 quantile_cont(b.impact, 0.95) AS q95,
                 quantile_cont(b.impact, 0.99) AS q99
             FROM base b, stats s;
             """)

conn.sql("SELECT * FROM audit_distribution").show()

# optional histogram over [-100,100]
conn.execute("""
             CREATE OR REPLACE VIEW audit_histogram AS
WITH binned AS (
  SELECT
    CAST(FLOOR( (impact - (-100)) / ((200.0) / 20) ) AS INT) AS bin,
    impact
  FROM impact_result
  WHERE impact BETWEEN -100 AND 100
)
             SELECT
                 bin,
                 MIN(impact) AS bin_min,
                 MAX(impact) AS bin_max,
                 COUNT(*) AS n
             FROM binned
             GROUP BY bin
             ORDER BY bin;
             """)
conn.sql("SELECT * FROM audit_histogram").show(max_rows=25, max_width=120)

# per-role tails
conn.execute("""
             CREATE OR REPLACE VIEW audit_role_tails AS
WITH base AS (
  SELECT pr.position AS role, ir.impact
  FROM player_result pr JOIN impact_result ir USING (match_id, player_name)
),
stats AS (
  SELECT role, AVG(impact) AS mu, STDDEV_SAMP(impact) AS sigma
  FROM base GROUP BY role
)
             SELECT
                 b.role,
                 COUNT(*) AS n,
                 100.0 * AVG(CASE WHEN ABS((b.impact - s.mu)/NULLIF(s.sigma,0)) > 3 THEN 1 ELSE 0 END) AS pct_gt_3sigma
             FROM base b
                      JOIN stats s USING (role)
             GROUP BY b.role
             ORDER BY b.role;
             """)
conn.sql("SELECT * FROM audit_role_tails").show(max_rows=50)



# Team × role summary (bias check)
conn.execute("""
             CREATE OR REPLACE VIEW audit_team_role AS
             SELECT
                 pr.team,
                 pr.position AS role,
                 COUNT(*) AS n,
                 AVG(ir.impact) AS mean_impact,
                 STDDEV_SAMP(ir.impact) AS std_impact,
                 MIN(ir.impact) AS min_impact,
                 MAX(ir.impact) AS max_impact
             FROM player_result pr
                      JOIN impact_result ir USING (match_id, player_name)
             GROUP BY pr.team, role
             ORDER BY pr.team, role;
             """)
conn.sql("SELECT * FROM audit_team_role").show(max_rows=50, max_width=200)
conn.execute("""
             CREATE OR REPLACE VIEW top_experts_by_role AS
WITH agg AS (
  SELECT
    pr.position,
    pr.player_name,
    COUNT(*)                          AS match_cnt,
    AVG(vip.impact)                   AS avg_impact
  FROM player_result pr
  JOIN v_impact_player vip USING (match_id, player_name)
  GROUP BY pr.position, pr.player_name
)
             SELECT position, player_name, match_cnt, avg_impact, rn
             FROM (
                      SELECT
                          a.*,
                          ROW_NUMBER() OVER (
      PARTITION BY a.position
      ORDER BY a.avg_impact DESC, a.match_cnt DESC, a.player_name
    ) AS rn
                      FROM agg a
                  )
             WHERE rn <= 20
             ORDER BY position, rn;
             """)

# show
conn.sql("""
         SELECT position, player_name, match_cnt, avg_impact
         FROM top_experts_by_role
         where match_cnt > 10 ORDER BY avg_impact desc 
         """).show(max_rows=20, max_width=200)

print("Audit complete.")
