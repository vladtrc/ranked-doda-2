"""
Goals:
- Deterministic updates: no randomness, stable ordering across runs.
- Role adjustment: ratings scaled by position multipliers, no fixed role bias.
- Controlled volatility: rating pool per match bounded by config values.
- Equal starting point: all players begin at the same initial rating.

Inputs:
- Tables: match, player_result, optional impact_result.
- Fields used:
  match: match_id, date_time, duration_sec, radiant_kills, dire_kills, winning_team
  player_result: match_id, player_name, team, position, kills, deaths, assists, net_worth
  impact_result: match_id, player_name, impact (defaults to 0.0 if missing)

Exports:
- calculate_ranked_mmr(conn, cfg=DEFAULT_MMR) -> None
- Creates table rating_result with columns:
  match_id:int64, player_name:varchar, pool:int64,
  th_skew:double, pr_skew:double, team_share:double,
  rating_before:int64, rating_after:int64, rating_diff:int64
- Creates helper views v_mmr_inputs and v_mmr_ordered (ephemeral)

Core formulas:
- Position multiplier: m(pos) = a + b * (pos-1)
- Team strengths:
  R = sum(rating[p] * m(pos_p)) for Radiant
  D = sum(rating[p] * m(pos_p)) for Dire
- Theoretical skew (rating based):
  th_log = clip(log(R/D), -th_clip, th_clip)
  th_skew = exp(th_log)
- Performance skew (kills based, duration adjusted):
  rk_s = radiant_kills + k_alpha
  dk_s = dire_kills + k_alpha
  k_log_raw = log(rk_s / dk_s)
  shrink = (min(duration, ref_seconds) / ref_seconds) ^ dur_power
  k_log = shrink * k_log_raw
  pr_skew = exp(k_log)
- Combined evidence:
  z = w_th * th_log + w_k * k_log
  s = abs(z)
- Pool size per match:
  pool_f = clip(base_pool + pool_gamma * s, pool_min, pool_max)
  pool = round(pool_f)
- Impact mapping to [0,1]:
  x = clip((impact + 100) / 200, 0, 1)
  impact01 = x ^ map_gamma
- Merit shares:
  Winners: normalize(impact01(+impact))
  Losers:  normalize(impact01(-impact))
  Final share = merit_weight * merit + (1 - merit_weight) * uniform
- Integer split:
  raw = share * pool
  base = floor(raw)
  remainder = pool - sum(base)
  distribute +1 to top remainder players by fractional remainder, tie key, then index
- Rating update:
  Winners gain units, losers lose units
  rating_before, rating_after = before + delta, rating_diff = delta

Determinism:
- Matches processed in date_time, match_id, team, position order
- Tie-breaking uses fractional remainder, merit-based tie key, then index
- No randomness anywhere
"""
from dataclasses import dataclass
from typing import Dict
from collections import defaultdict
import duckdb, pandas as pd, numpy as np

@dataclass(frozen=True)
class PosMultiplier:
    a: float = 2.0
    b: float = -0.25

@dataclass(frozen=True)
class MmrConfig:
    initial_mmr: int = 500
    base_pool: float = 50.0
    pool_gamma: float = 25.0
    pool_min: float = 25.0
    pool_max: float = 400.0
    ref_seconds: int = 5400
    th_clip: float = 2.0
    k_alpha: float = 2.0
    dur_power: float = 0.5
    w_th: float = 0.6
    w_k: float = 0.4
    merit_weight: float = 0.8
    map_gamma: float = 1.0
    team_radiant: str = "radiant"
    team_dire: str = "dire"
    pos_multiplier: PosMultiplier = PosMultiplier()

DEFAULT_MMR = MmrConfig()

def _init_mmr_views(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
                 create or replace view v_mmr_inputs as
                 select m.match_id,m.date_time,cast(m.duration_sec as int) as duration_sec,
                        cast(m.radiant_kills as int) as radiant_kills,cast(m.dire_kills as int) as dire_kills,
                        lower(trim(m.winning_team)) as winning_team,pr.player_name,lower(trim(pr.team)) as team,
                        cast(pr.position as int) as position,cast(pr.kills as int) as kills,
           cast(pr.deaths as int) as deaths,cast(pr.assists as int) as assists,
           cast(pr.net_worth as int) as net_worth,cast(coalesce(ir.impact,0.0) as double) as impact
                 from match m join player_result pr using(match_id)
                     left join impact_result ir using(match_id,player_name);
                 """)
    conn.execute("""
                 create or replace view v_mmr_ordered as
                 select * from v_mmr_inputs
                 where team in ('radiant','dire') and winning_team in ('radiant','dire')
                 order by date_time, match_id, team, position;
                 """)

def _normalize(v: np.ndarray) -> np.ndarray:
    s = v.sum()
    return v / s if s > 1e-12 else np.full_like(v, 1.0 / len(v), dtype=float)

def _impact01(x: np.ndarray, gamma: float) -> np.ndarray:
    return np.clip((x + 100.0) / 200.0, 0.0, 1.0) ** gamma

def _shares(impact: np.ndarray, winners: bool, merit_w: float, gamma: float) -> tuple[np.ndarray, np.ndarray]:
    merit_raw = _impact01(impact if winners else -impact, gamma)
    merit = _normalize(merit_raw)
    floor = np.full_like(merit, 1.0 / len(merit))
    return merit_w * merit + (1 - merit_w) * floor, merit_raw  # share, tie-key

def _integer_split(weights: np.ndarray, total: int, tie_key: np.ndarray) -> np.ndarray:
    w = _normalize(weights)
    raw = w * float(total)
    base = np.floor(raw).astype(int)
    rem = int(total - base.sum())
    if rem <= 0: return base
    frac = raw - base
    idx = np.arange(len(frac))
    order = np.lexsort((idx, -tie_key, -frac))  # frac desc, tie desc, index asc
    base[order[:rem]] += 1
    return base

def _pos_mult(pos: np.ndarray, cfg: MmrConfig) -> np.ndarray:
    return cfg.pos_multiplier.a + cfg.pos_multiplier.b * (pos - 1.0)

def calculate_ranked_mmr(conn: duckdb.DuckDBPyConnection, cfg: MmrConfig = DEFAULT_MMR) -> None:
    _init_mmr_views(conn)
    df = conn.execute("select * from v_mmr_ordered").df()
    ratings: Dict[str, int] = defaultdict(lambda: int(cfg.initial_mmr))
    out_frames = []

    group_keys = ["match_id","date_time","duration_sec","radiant_kills","dire_kills","winning_team"]
    for (mid, dt, dur, rk, dk, win), g in df.groupby(group_keys, sort=False):
        rad = g[g.team == cfg.team_radiant]; dire = g[g.team == cfg.team_dire]
        if rad.empty or dire.empty: continue

        r_names = rad.player_name.to_numpy()
        d_names = dire.player_name.to_numpy()
        r_pos = rad.position.to_numpy(dtype=float)
        d_pos = dire.position.to_numpy(dtype=float)
        r_imp = rad.impact.to_numpy(dtype=float)
        d_imp = dire.impact.to_numpy(dtype=float)

        r_before = np.array([ratings[n] for n in r_names], dtype=int)
        d_before = np.array([ratings[n] for n in d_names], dtype=int)

        R = max(float((r_before * _pos_mult(r_pos, cfg)).sum()), 1e-9)
        D = max(float((d_before * _pos_mult(d_pos, cfg)).sum()), 1e-9)

        th_log = float(np.clip(np.log(R / D), -cfg.th_clip, cfg.th_clip))
        th_skew = float(np.exp(th_log))

        rk_s = float(rk) + cfg.k_alpha
        dk_s = float(dk) + cfg.k_alpha
        k_log_raw = float(np.log(rk_s / dk_s))
        shrink = (min(int(dur), cfg.ref_seconds) / cfg.ref_seconds) ** cfg.dur_power
        k_log = float(shrink * k_log_raw)
        pr_skew = float(np.exp(k_log))

        z = cfg.w_th * th_log + cfg.w_k * k_log
        s = abs(z)
        pool = int(round(float(np.clip(cfg.base_pool + cfg.pool_gamma * s, cfg.pool_min, cfg.pool_max))))

        win_radiant = (win == cfg.team_radiant)
        r_share, r_tie = _shares(r_imp, win_radiant, cfg.merit_weight, cfg.map_gamma)
        d_share, d_tie = _shares(d_imp, not win_radiant, cfg.merit_weight, cfg.map_gamma)

        r_units = _integer_split(r_share, pool, r_tie)
        d_units = _integer_split(d_share, pool, d_tie)

        r_delta = r_units if win_radiant else -r_units
        d_delta = d_units if not win_radiant else -d_units

        r_after = r_before + r_delta
        d_after = d_before + d_delta

        for n, v in zip(r_names, r_after): ratings[n] = int(v)
        for n, v in zip(d_names, d_after): ratings[n] = int(v)

        rad_rows = pd.DataFrame({
            "match_id": int(mid), "player_name": r_names, "pool": pool,
            "th_skew": th_skew, "pr_skew": pr_skew, "team_share": r_share.astype(float),
            "rating_before": r_before.astype(int), "rating_after": r_after.astype(int),
            "rating_diff": r_delta.astype(int),
        })
        dire_rows = pd.DataFrame({
            "match_id": int(mid), "player_name": d_names, "pool": pool,
            "th_skew": th_skew, "pr_skew": pr_skew, "team_share": d_share.astype(float),
            "rating_before": d_before.astype(int), "rating_after": d_after.astype(int),
            "rating_diff": d_delta.astype(int),
        })
        out_frames.append(pd.concat([rad_rows, dire_rows], ignore_index=True))
    assert r_units.sum() == pool and d_units.sum() == pool
    assert int(r_delta.sum() + d_delta.sum()) == 0
    assert int(r_before.sum() + d_before.sum()) == int(r_after.sum() + d_after.sum())

    out = pd.concat(out_frames, ignore_index=True) if out_frames else pd.DataFrame(
        columns=["match_id","player_name","pool","th_skew","pr_skew","team_share","rating_before","rating_after","rating_diff"]
    )
    conn.execute("drop table if exists rating_result")
    conn.register("rating_df", out)
    conn.execute("""
                 create table rating_result as
                 select cast(match_id as bigint) match_id,
                        cast(player_name as varchar) player_name,
                        cast(pool as bigint) pool,
                        cast(th_skew as double) th_skew,
                        cast(pr_skew as double) pr_skew,
                        cast(team_share as double) team_share,
                        cast(rating_before as bigint) rating_before,
                        cast(rating_after as bigint) rating_after,
                        cast(rating_diff as bigint) rating_diff
                 from rating_df
                 order by match_id, player_name;
                 """)
    conn.unregister("rating_df")
