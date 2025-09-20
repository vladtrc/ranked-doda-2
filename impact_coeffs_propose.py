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


def metric_values(conn: duckdb.DuckDBPyConnection):
    conn.sql("SELECT * FROM v_impact_player LIMIT 5")
    conn.execute("""
                 CREATE
                 OR REPLACE VIEW match_batches AS
                WITH base AS (
                  SELECT DISTINCT match_id FROM match
                ),
                ranked AS (
                  SELECT
                    b.batch,
                    m.match_id,
                    ROW_NUMBER() OVER (PARTITION BY b.batch ORDER BY random()) AS rn
                  FROM range(1, 21) AS b(batch)
                  CROSS JOIN base AS m
                )
                 SELECT batch, match_id
                 FROM ranked
                 WHERE rn <= 80
                 ORDER BY batch, match_id;
                 """)
    conn.execute("""
                 CREATE OR REPLACE VIEW batch_role_impact AS
                 SELECT mb.batch,
                        pr.position AS position,
                     AVG(vip.impact)  AS avg_impact,
                     ABS(AVG(vip.impact))  AS avg_impact_abs,
                     MIN(vip.impact)  AS min_impact,
                     MAX(vip.impact)  AS max_impact,
                     COUNT(*)         AS samples,
                     COUNT(DISTINCT pr.player_name) AS unique_players,
                     COUNT(DISTINCT pr.match_id)    AS unique_matches
                 FROM match_batches AS mb
                     JOIN v_impact_player AS vip USING (match_id)
                     JOIN player_result AS pr USING (match_id, player_name)
                 GROUP BY mb.batch, pr.position
                 ORDER BY mb.batch, pr.position;
                 """)
    row = conn.sql("""
                   WITH role_max AS (SELECT position, MAX(avg_impact_abs) AS max_avg_abs
                                     FROM batch_role_impact
                                     GROUP BY position)
                   SELECT MAX(CASE WHEN position = 1 THEN max_avg_abs END) AS role1,
                          MAX(CASE WHEN position = 2 THEN max_avg_abs END) AS role2,
                          MAX(CASE WHEN position = 3 THEN max_avg_abs END) AS role3,
                          MAX(CASE WHEN position = 4 THEN max_avg_abs END) AS role4,
                          MAX(CASE WHEN position = 5 THEN max_avg_abs END) AS role5
                   FROM role_max
                   """).fetchone()
    if row is None or (len(row) != 5) or any(v is None for v in row):
        raise ValueError(f"Expected 5 doubles, got: {row}")
    return tuple(float(v) for v in row)


def optimize_multi(conn: duckdb.DuckDBPyConnection, n_trials=12000) -> ImpactCoeffs:
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
        directions=["minimize"] * 5,
        sampler=sampler,
        storage=None,  # in-memory
        load_if_exists=False,  # ensure fresh study
    )
    study.optimize(objective, n_trials=n_trials)
    pareto = study.best_trials
    if not pareto:
        raise RuntimeError("No Pareto trials.")

    rows = []
    for t in pareto:
        vals = tuple(float(v) for v in t.values)
        rows.append({
            "trial": t.number, "r1": vals[0], "r2": vals[1], "r3": vals[2],
            "r4": vals[3], "r5": vals[4], "sum": sum(vals)
        })

    df = pd.DataFrame(rows).sort_values("sum").reset_index(drop=True)
    print("\nPareto trials sorted by sum (lower is better):")
    print(df[["trial","r1","r2","r3","r4","r5","sum"]].to_string(
        index=False, float_format=lambda x: f"{x:.6f}"))

    # Print ImpactCoeffs for top-k trials
    top_k = 5
    print(f"\nTop {top_k} trials as ImpactCoeffs:")
    for i, rec in df.head(top_k).iterrows():
        t = next(tt for tt in pareto if tt.number == rec.trial)
        coeffs = params_to_coeffs(t.params)
        print(f"\nTrial {rec.trial}  sum={rec['sum']:.6f}  "
              f"objectives=({rec.r1:.6f}, {rec.r2:.6f}, {rec.r3:.6f}, {rec.r4:.6f}, {rec.r5:.6f})")
        # pretty table
        pprint(coeffs)

    # Optionally set the best-by-sum as return value
    best_trial_num = int(df.iloc[0].trial)
    best_trial = next(tt for tt in pareto if tt.number == best_trial_num)
    best_coeffs = params_to_coeffs(best_trial.params)
    return best_coeffs



# run
if __name__ == "__main__":
    matches = parse_dota_file()
    conn = load_matches_into_duckdb(matches)
    coeffs = optimize_multi(conn)
    pprint(coeffs)
    print(coeffs)
