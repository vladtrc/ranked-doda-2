from pprint import pprint

import duckdb
import optuna
import pandas as pd
from optuna.samplers import NSGAIISampler

from data_to_duckdb import load_matches_into_duckdb
from impact import ImpactCoeffs, RoleCoeff, _register_coeffs_table, init_impact_views
from parse_data import parse_dota_file

ROLES = [1, 2, 3, 4, 5]

def params_to_coeffs(params: dict) -> ImpactCoeffs:
    rows = []
    for pos in ROLES:
        rows.append(RoleCoeff(
            position=pos,
            w_k=float(params[f"wk_{pos}"]),
            w_d=float(params[f"wd_{pos}"]),
            w_a=float(params[f"wa_{pos}"]),
            w_n=float(params[f"wn_{pos}"]),
        ))
    return ImpactCoeffs(rows=rows)


def weights_to_coeffs(trial: optuna.trial.Trial) -> ImpactCoeffs:
    rows = []
    for pos in ROLES:
        rows.append(
            RoleCoeff(
                position=pos,
                w_k=trial.suggest_float(f"wk_{pos}", 1.0, 5.0, step=0.1),
                w_d=trial.suggest_float(f"wd_{pos}", -10.0, -1.0, step=0.1),
                w_a=trial.suggest_float(f"wa_{pos}", 1.0, 10.0, step=0.1),
                w_n=trial.suggest_float(f"wn_{pos}", 1e-4, 5e-3, step=0.00001),
            )
        )
    return ImpactCoeffs(rows=rows)


# ---- metrics ----
METRIC_CNT = 13  # 5 role neutrality + 2 bounds + 2 dist + 1 batch + 1 overflow + 2 team neutrality

def metric_values(conn):
    # batches
    conn.execute("""
                 CREATE OR REPLACE VIEW match_batches AS
    WITH base AS (SELECT DISTINCT match_id FROM match),
    ranked AS (
      SELECT b.batch, m.match_id,
             ROW_NUMBER() OVER (PARTITION BY b.batch ORDER BY random()) AS rn
      FROM range(1,21) AS b(batch)
      CROSS JOIN base m
    )
                 SELECT batch, match_id
                 FROM ranked
                 WHERE rn <= 80
                 ORDER BY batch, match_id;
                 """)

    # per-batch role averages
    conn.execute("""
                 CREATE OR REPLACE VIEW batch_role_impact AS
                 SELECT mb.batch, pr.position AS position, AVG(vip.impact) AS avg_impact
                 FROM match_batches mb
                     JOIN v_impact_player vip USING (match_id)
                     JOIN player_result pr USING (match_id, player_name)
                 GROUP BY mb.batch, pr.position;
                 """)

    # 1–5 role neutrality
    role_row = conn.sql("""
                        SELECT
                            COALESCE(ABS(AVG(CASE WHEN pr.position=1 THEN ir.impact END)), 0.0),
                            COALESCE(ABS(AVG(CASE WHEN pr.position=2 THEN ir.impact END)), 0.0),
                            COALESCE(ABS(AVG(CASE WHEN pr.position=3 THEN ir.impact END)), 0.0),
                            COALESCE(ABS(AVG(CASE WHEN pr.position=4 THEN ir.impact END)), 0.0),
                            COALESCE(ABS(AVG(CASE WHEN pr.position=5 THEN ir.impact END)), 0.0)
                        FROM player_result pr
                                 JOIN v_impact_player ir USING (match_id, player_name);
                        """).fetchone()

    # 6–7 boundedness
    bounds_row = conn.sql("""
                          WITH base AS (SELECT impact FROM v_impact_player)
                          SELECT
                              COALESCE(MAX(ABS(impact)), 0.0),
                              100.0 * AVG(CASE WHEN impact < -100 OR impact > 100 THEN 1 ELSE 0 END)
                          FROM base;
                          """).fetchone()

    # 8–9 distribution
    dist_row = conn.sql("""
                        WITH base AS (SELECT impact FROM v_impact_player),
                             stats AS (SELECT AVG(impact) AS mu, STDDEV_SAMP(impact) AS sigma FROM base)
                        SELECT
                            COALESCE(MAX(s.sigma), 0.0),
                            100.0 * AVG(CASE
                                            WHEN NULLIF(s.sigma,0) IS NULL THEN 0
                                            WHEN ABS((b.impact - s.mu)/s.sigma) > 3 THEN 1 ELSE 0
                                END)
                        FROM base b, stats s;
                        """).fetchone()

    # 10 batch stability
    batch_row = conn.sql("""
                         WITH per_role AS (
                             SELECT position, STDDEV_SAMP(avg_impact) AS s
                             FROM batch_role_impact
                             GROUP BY position
                         )
                         SELECT COALESCE(MAX(s), 0.0) FROM per_role;
                         """).fetchone()

    # 11 overflow hinge
    overflow_row = conn.sql("""
                            WITH base AS (SELECT ABS(impact) AS a FROM v_impact_player)
                            SELECT AVG(GREATEST(a - 100.0, 0.0)) FROM base;
                            """).fetchone()

    # 12–13 team neutrality
    team_row = conn.sql("""
                        SELECT
                            COALESCE(ABS(AVG(CASE WHEN pr.team='radiant' THEN ir.impact END)), 0.0),
                            COALESCE(ABS(AVG(CASE WHEN pr.team='dire'    THEN ir.impact END)), 0.0)
                        FROM player_result pr
                                 JOIN v_impact_player ir USING (match_id, player_name);
                        """).fetchone()

    row = tuple(map(float, role_row + bounds_row + dist_row + batch_row + overflow_row + team_row))
    if len(row) != METRIC_CNT or any(v is None for v in row):
        raise ValueError(f"Expected {METRIC_CNT} metrics, got: {row}")
    return row



def optimize_multi(conn: duckdb.DuckDBPyConnection, n_trials) -> ImpactCoeffs:
    def objective(trial: optuna.trial.Trial):
        coeffs = weights_to_coeffs(trial)
        wds = {rc.position: rc.w_d for rc in coeffs.rows}
        if not all(wds[r] < wds[r+1] for r in range(1, 5)):
            raise optuna.TrialPruned()

        conn.sql("SELECT * FROM player_result LIMIT 5")
        _register_coeffs_table(conn, coeffs)
        conn.sql("SELECT * FROM v_impact_weights LIMIT 5")
        init_impact_views(conn, coeffs)
        r = metric_values(conn)
        return r

    sampler = NSGAIISampler(seed=42)

    study = optuna.create_study(
        directions=["minimize"] * METRIC_CNT,
        sampler=sampler,
        storage=None,  # in-memory
        load_if_exists=False,  # ensure fresh study
    )
    study.optimize(objective, n_trials=n_trials)
    pareto = study.best_trials
    if not pareto:
        raise RuntimeError("No Pareto trials.")

    N = 5  # number of leading metrics to sum (roles)

    rows = []
    K = len(pareto[0].values)
    for t in pareto:
        vals = tuple(float(v) for v in t.values)
        rec = {"trial": t.number}
        rec.update({f"m{i+1}": v for i, v in enumerate(vals)})
        rec["role_sum"] = sum(vals[:min(N, K)])
        rows.append(rec)

    df = pd.DataFrame(rows).sort_values("role_sum").reset_index(drop=True)

    cols = ["trial"] + [f"m{i+1}" for i in range(K)] + ["role_sum"]
    print("\nPareto trials sorted by sum of first N metrics (lower is better):")
    print(df[cols].to_string(index=False, float_format=lambda x: f"{x:.6f}"))

    # print ImpactCoeffs for top-k
    top_k = 5
    print(f"\nTop {top_k} trials as ImpactCoeffs:")
    for _, rec in df.head(top_k).iterrows():
        t = next(tt for tt in pareto if tt.number == rec.trial)
        coeffs = params_to_coeffs(t.params)
        metrics_str = ", ".join(f"{rec[f'm{i+1}']:.6f}" for i in range(K))
        print(f"\nTrial {int(rec.trial)}  role_sum={rec['role_sum']:.6f}  metrics=({metrics_str})")
        pprint(coeffs)

    # return best-by-role_sum
    best_trial_num = int(df.iloc[0]["trial"])
    best_trial = next(tt for tt in pareto if tt.number == best_trial_num)
    best_coeffs = params_to_coeffs(best_trial.params)
    return best_coeffs


# run
if __name__ == "__main__":
    matches = parse_dota_file()
    conn = load_matches_into_duckdb(matches)
    coeffs = optimize_multi(conn, n_trials=25000)
    pprint(coeffs)
    print(coeffs)
