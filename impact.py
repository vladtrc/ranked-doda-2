"""
Goals:
- Role neutrality: average impact per role = 0 (no built-in bias).  
- Bounded scale: player impact always within [-100, 100].  
- Stable distribution: roughly bell-shaped, with <1% extreme outliers.  
- Historical impacts remain unchanged when new matches are added.

Exports:
- calculate_impacts(matches): Given connection with, creates a table impact_result
  with per-player impact values (match_id:int64, player_name:varchar, impact:double).
- debug_impacts(matches): Uses calculate_impacts internally, then prints
  diagnostics that you probably do not need unless debugging the alg.

Notice:
- The module creates *DuckDB views and tables* with prefix `v_impact_*` inside the provided connection. Others should avoid relying on them.
"""
import duckdb


def init_impact_views(conn: duckdb.DuckDBPyConnection) -> None:
    # 1) Hardcoded weights per position
    conn.execute("""
        CREATE OR REPLACE TABLE impact_weights AS
        SELECT * FROM (VALUES
            (1, 2.0,  -1.5, 0.5,  0.0020),   -- pos1 carry
            (2, 2.0,  -1.5, 0.5,  0.0018),   -- pos2 mid
            (3, 1.5,  -1.5, 0.7,  0.0015),   -- pos3 offlane
            (4, 0.7,  -1.5, 1.5,  0.0010),   -- pos4 support
            (5, 0.5,  -1.5, 2.0,  0.0008)    -- pos5 hard support
        ) AS t(position, w_k, w_d, w_a, w_net);
    """)

    # 2) Player-level raw impact
    conn.execute("""
                 CREATE
                 OR REPLACE VIEW v_impact_player_raw AS
                 SELECT pr.match_id,
                        pr.player_name,
                        pr.team,
                        pr.position,
                        pr.kills,
                        pr.deaths,
                        pr.assists,
                        pr.net_worth,
                        iw.w_k,
                        iw.w_d,
                        iw.w_a,
                        iw.w_net,
                        (iw.w_k * pr.kills) +
                        (iw.w_d * pr.deaths) +
                        (iw.w_a * pr.assists) +
                        (iw.w_net * pr.net_worth) AS impact
                 FROM player_result pr
                          JOIN impact_weights iw USING (position);
                 """)

    # 2b) Fixed calibration per role
    conn.execute("""
        CREATE OR REPLACE TABLE impact_calibration AS
        SELECT * FROM (VALUES
            (1, 0.0, 45.0),
            (2, 0.0, 45.0),
            (3, 0.0, 40.0),
            (4, 0.0, 35.0),
            (5, 0.0, 30.0)
        ) AS t(position, bias, scale);
    """)

    # 2c) Bounded impact
    conn.execute("""
                 CREATE
                 OR REPLACE VIEW v_impact_player AS
                 SELECT p.match_id,
                        p.player_name,
                        p.team,
                        p.position,
                        p.kills,
                        p.deaths,
                        p.assists,
                        p.net_worth,
                        p.impact                                                   AS raw_impact,
                        100.0 * tanh((p.impact - ic.bias) / NULLIF(ic.scale, 0.0)) AS impact
                 FROM v_impact_player_raw p
                          JOIN impact_calibration ic USING (position);
                 """)

    # 3) Team impact per match
    conn.execute("""
                 CREATE
                 OR REPLACE VIEW v_impact_team_raw AS
                 SELECT match_id, team, SUM(impact) AS team_impact
                 FROM v_impact_player_raw
                 GROUP BY match_id, team;
                 """)
    conn.execute("""
                 CREATE
                 OR REPLACE VIEW v_impact_team AS
                 SELECT match_id, team, SUM(impact) AS team_impact
                 FROM v_impact_player
                 GROUP BY match_id, team;
                 """)

    # 4) Numeric outcome
    conn.execute("""
                 CREATE
                 OR REPLACE VIEW v_impact_outcome AS
                 SELECT m.match_id,
                        CASE
                            WHEN m.winning_team = 'radiant' THEN 1
                            WHEN m.winning_team = 'dire' THEN -1
                            ELSE NULL END AS outcome
                 FROM match m;
                 """)

    # 5) Match-level impact diff
    conn.execute("""
                 CREATE
                 OR REPLACE VIEW v_impact_match_raw AS
        WITH ti AS (
            SELECT
                match_id,
                MAX(CASE WHEN team='radiant' THEN team_impact END) AS radiant_impact,
                MAX(CASE WHEN team='dire'    THEN team_impact END) AS dire_impact
            FROM v_impact_team_raw
            GROUP BY match_id
        )
                 SELECT ti.match_id,
                        ti.radiant_impact,
                        ti.dire_impact,
                        (ti.radiant_impact - ti.dire_impact) AS impact_diff,
                        mo.outcome
                 FROM ti
                          JOIN v_impact_outcome mo USING (match_id);
                 """)
    conn.execute("""
                 CREATE
                 OR REPLACE VIEW v_impact_match AS
        WITH ti AS (
            SELECT
                match_id,
                MAX(CASE WHEN team='radiant' THEN team_impact END) AS radiant_impact,
                MAX(CASE WHEN team='dire'    THEN team_impact END) AS dire_impact
            FROM v_impact_team
            GROUP BY match_id
        )
                 SELECT ti.match_id,
                        ti.radiant_impact,
                        ti.dire_impact,
                        (ti.radiant_impact - ti.dire_impact) AS impact_diff,
                        mo.outcome
                 FROM ti
                          JOIN v_impact_outcome mo USING (match_id);
                 """)


def calculate_impacts(conn: duckdb.DuckDBPyConnection) -> None:
    init_impact_views(conn)
    conn.execute("""
        CREATE OR REPLACE TABLE impact_result AS
        SELECT match_id, player_name, impact
        FROM v_impact_player
        ORDER BY match_id, player_name;
    """)


def debug_impacts(conn: duckdb.DuckDBPyConnection) -> None:
    # --- Per-position distribution ---
    pos_rows = conn.execute("""
                            WITH pos_stats AS (SELECT position,
                                                      COUNT(*)            AS n,
                                                      AVG(impact)         AS mean_impact,
                                                      STDDEV_SAMP(impact) AS std_impact,
                                                      MIN(impact)         AS min_impact,
                                                      MAX(impact)         AS max_impact
                                               FROM v_impact_player
                                               GROUP BY position),
                                 outlier_counts AS (SELECT p.position,
                                                           SUM(CASE WHEN ABS(p.impact) > 5 * ps.std_impact THEN 1 ELSE 0 END) AS n_outliers,
                                                           COUNT(*)                                                           AS n_total
                                                    FROM v_impact_player p
                                                             JOIN pos_stats ps USING (position)
                                                    GROUP BY p.position, ps.std_impact)
                            SELECT ps.position,
                                   ps.n,
                                   ps.mean_impact,
                                   ps.std_impact,
                                   ps.min_impact,
                                   ps.max_impact,
                                   CAST(oc.n_outliers AS DOUBLE) / NULLIF(oc.n_total, 0) AS frac_gt_5sigma
                            FROM pos_stats ps
                                     JOIN outlier_counts oc USING (position)
                            ORDER BY ps.position
                            """).fetchall()

    # --- Team spread ---
    team_rows = conn.execute("""
                             SELECT team,
                                    AVG(team_impact)         AS mean_team_impact,
                                    STDDEV_SAMP(team_impact) AS std_team_impact
                             FROM v_impact_team
                             GROUP BY team
                             ORDER BY team
                             """).fetchall()

    # --- Match-level correlation & accuracy ---
    n_matches, corr_diff_outcome, accuracy = conn.execute("""
                                                          SELECT COUNT(*)                                                     AS n_matches,
                                                                 CORR(impact_diff, outcome)                                   AS corr_diff_outcome,
                                                                 AVG(CASE WHEN SIGN(impact_diff) = outcome THEN 1 ELSE 0 END) AS accuracy
                                                          FROM v_impact_match
                                                          WHERE outcome IS NOT NULL
                                                            AND impact_diff IS NOT NULL
                                                          """).fetchone()

    # --- Raw role zero-sum check ---
    role_rows = conn.execute("""
                             SELECT position,
                                    SUM(impact) AS sum_impact_all_matches
                             FROM v_impact_player_raw
                             GROUP BY position
                             ORDER BY position
                             """).fetchall()

    # --- Printing ---
    def _fmt(x):
        return "None" if x is None else f"{x:.3f}"

    print("Per-position distribution (bounded):")
    for r in pos_rows:
        print(
            f"pos={r[0]}  n={r[1]}  mean={_fmt(r[2])}  std={_fmt(r[3])}  "
            f"min={_fmt(r[4])}  max={_fmt(r[5])}  frac_gt_5σ={_fmt(r[6])}"
        )

    print("\nTeam spread (bounded):")
    for r in team_rows:
        print(f"team={r[0]}  mean={_fmt(r[1])}  std={_fmt(r[2])}")

    print("\nMatch-level metrics (bounded):")
    print(f"n_matches={int(n_matches or 0)}")
    print(f"corr(impact_diff, outcome)={_fmt(corr_diff_outcome)}")
    print(f"accuracy(sign(impact_diff) predicts winner)={_fmt(accuracy)}")

    print("\nRaw role sums (neutrality check hint):")
    for r in role_rows:
        print(f"pos={r[0]}  sum_raw_impact={_fmt(r[1])}")
