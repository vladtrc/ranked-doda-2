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
import duckdb
import pandas as pd
import numpy as np

@dataclass(frozen=True)
class PosMultiplier:
    a: float = 2.0
    b: float = -0.25

@dataclass(frozen=True)
class MmrConfig:
    # ratings and pool
    initial_mmr: int = 500
    base_pool: float = 50.0
    pool_gamma: float = 25.0
    pool_min: float = 25.0
    pool_max: float = 400.0
    # skew math
    ref_seconds: int = 5400
    th_clip: float = 2.0
    k_alpha: float = 2.0
    dur_power: float = 0.5
    w_th: float = 0.6
    w_k: float = 0.4
    # split
    merit_weight: float = 0.8
    map_gamma: float = 1.0
    # misc
    team_radiant: str = "radiant"
    team_dire: str = "dire"
    pos_multiplier: PosMultiplier = PosMultiplier()

DEFAULT_MMR = MmrConfig()

def _init_mmr_views(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
                 create or replace view v_mmr_inputs as
                 select
                     m.match_id,
                     m.date_time,
                     cast(m.duration_sec as int)  as duration_sec,
                     cast(m.radiant_kills as int) as radiant_kills,
                     cast(m.dire_kills as int)    as dire_kills,
                     lower(trim(m.winning_team))  as winning_team,
                     pr.player_name,
                     lower(trim(pr.team))         as team,
                     cast(pr.position as int)     as position,
			cast(pr.kills as int)        as kills,
			cast(pr.deaths as int)       as deaths,
			cast(pr.assists as int)      as assists,
			cast(pr.net_worth as int)    as net_worth,
			cast(coalesce(ir.impact, 0.0) as double) as impact
                 from match m
                     join player_result pr using(match_id)
                     left join impact_result ir using(match_id, player_name);
                 """)
    conn.execute("""
                 create or replace view v_mmr_ordered as
                 select * from v_mmr_inputs
                 where team in ('radiant','dire') and winning_team in ('radiant','dire')
                 order by date_time asc, match_id asc, team asc, position asc;
                 """)

def _pos_mult(pos: int, cfg: MmrConfig) -> float:
    return cfg.pos_multiplier.a + cfg.pos_multiplier.b * (pos - 1)

def _normalize(v: pd.Series) -> pd.Series:
    s = float(v.sum())
    if s <= 1e-12:
        return pd.Series([1.0 / len(v)] * len(v), index=v.index)
    return v / s

def _impact01(s: pd.Series, gamma: float) -> pd.Series:
    x = ((s.astype(float) + 100.0) / 200.0).clip(0.0, 1.0)
    return x.pow(gamma)

def _shares_winners(s_impact: pd.Series, cfg: MmrConfig) -> pd.Series:
    merit = _normalize(_impact01(s_impact, cfg.map_gamma))
    floor = pd.Series([1.0 / len(s_impact)] * len(s_impact), index=s_impact.index)
    return cfg.merit_weight * merit + (1.0 - cfg.merit_weight) * floor

def _shares_losers(s_impact: pd.Series, cfg: MmrConfig) -> pd.Series:
    merit = _normalize(_impact01(-s_impact, cfg.map_gamma))
    floor = pd.Series([1.0 / len(s_impact)] * len(s_impact), index=s_impact.index)
    return cfg.merit_weight * merit + (1.0 - cfg.merit_weight) * floor

def _integer_split(weights: pd.Series, total: int, tie_key: pd.Series) -> pd.Series:
    w = _normalize(weights)
    raw = (w * total).astype(float)
    base = np.floor(raw).astype(int)
    rem = int(total - int(base.sum()))
    alloc = pd.Series(base.values, index=weights.index, dtype=int)
    if rem <= 0:
        return alloc

    frac = raw - base
    order_df = pd.DataFrame({
        'frac': frac,
        'tie': tie_key
    }, index=weights.index)
    # sort by larger fractional remainder, then larger tie_key, then stable index
    order_idx = order_df.sort_values(['frac', 'tie', order_df.index.name or 'tie'],
                                     ascending=[False, False, True]).index.tolist()

    for i in range(rem):
        alloc.loc[order_idx[i]] += 1   # label-based, not positional

    return alloc

def calculate_ranked_mmr(conn: duckdb.DuckDBPyConnection, cfg: MmrConfig = DEFAULT_MMR) -> None:
    _init_mmr_views(conn)
    df = conn.execute("select * from v_mmr_ordered").df()

    ratings: Dict[str, int] = defaultdict(lambda: int(cfg.initial_mmr))
    rows = []

    for (mid, dt, dur, rk, dk, win), g in df.groupby(
            ["match_id", "date_time", "duration_sec", "radiant_kills", "dire_kills", "winning_team"], sort=False
    ):
        rad = g[g.team == cfg.team_radiant].copy()
        dire = g[g.team == cfg.team_dire].copy()
        if rad.empty or dire.empty:
            continue

        R = sum(ratings[p] * _pos_mult(int(pos), cfg) for p, pos in zip(rad.player_name, rad.position))
        D = sum(ratings[p] * _pos_mult(int(pos), cfg) for p, pos in zip(dire.player_name, dire.position))
        R = max(R, 1e-9); D = max(D, 1e-9)

        th_log = float(np.clip(np.log(R / D), -cfg.th_clip, cfg.th_clip))
        th_skew = float(np.exp(th_log))

        rk_s = int(rk) + cfg.k_alpha
        dk_s = int(dk) + cfg.k_alpha
        k_log_raw = float(np.log(rk_s / dk_s))
        shrink = (min(int(dur), cfg.ref_seconds) / cfg.ref_seconds) ** cfg.dur_power
        k_log = float(shrink * k_log_raw)
        pr_skew = float(np.exp(k_log))

        z = cfg.w_th * th_log + cfg.w_k * k_log
        s = abs(z)
        pool_f = float(np.clip(cfg.base_pool + cfg.pool_gamma * s, cfg.pool_min, cfg.pool_max))
        pool = int(round(pool_f))  # integer pool

        win_radiant = (win == cfg.team_radiant)
        rad_share = _shares_winners(rad["impact"], cfg) if win_radiant else _shares_losers(rad["impact"], cfg)
        dire_share = _shares_winners(dire["impact"], cfg) if not win_radiant else _shares_losers(dire["impact"], cfg)

        # Deterministic tie keys: use mapped merit then player_name
        rad_tie = _impact01(rad["impact"] if win_radiant else -rad["impact"], cfg.map_gamma)
        dire_tie = _impact01(dire["impact"] if not win_radiant else -dire["impact"], cfg.map_gamma)

        # Integer allocations per side sum exactly to pool
        rad_units = _integer_split(rad_share, pool, rad_tie)
        dire_units = _integer_split(dire_share, pool, dire_tie)

        # Radiant rows
        for idx, r in rad.iterrows():
            name = r.player_name
            before = ratings[name]
            unit = int(rad_units.loc[idx])
            delta = unit if win_radiant else -unit
            after = before + delta
            ratings[name] = after
            rows.append({
                "match_id": int(mid),
                "player_name": name,
                "pool": int(pool),
                "th_skew": th_skew,
                "pr_skew": pr_skew,
                "team_share": float(rad_share.loc[idx]),
                "rating_before": int(before),
                "rating_after": int(after),
                "rating_diff": int(delta),
            })

        # Dire rows
        for idx, r in dire.iterrows():
            name = r.player_name
            before = ratings[name]
            unit = int(dire_units.loc[idx])
            delta = unit if not win_radiant else -unit
            after = before + delta
            ratings[name] = after
            rows.append({
                "match_id": int(mid),
                "player_name": name,
                "pool": int(pool),
                "th_skew": pr_skew if False else th_skew,  # keep columns uniform; th_skew same per match
                "pr_skew": pr_skew,
                "team_share": float(dire_share.loc[idx]),
                "rating_before": int(before),
                "rating_after": int(after),
                "rating_diff": int(delta),
            })

    out = pd.DataFrame(rows)
    conn.execute("drop table if exists rating_result")
    conn.register("rating_df", out)
    conn.execute("""
                 create table rating_result as
                 select
                     cast(match_id as bigint)       as match_id,
                     cast(player_name as varchar)   as player_name,
                     cast(pool as bigint)           as pool,
                     cast(th_skew as double)        as th_skew,
                     cast(pr_skew as double)        as pr_skew,
                     cast(team_share as double)     as team_share,
                     cast(rating_before as bigint)  as rating_before,
                     cast(rating_after  as bigint)  as rating_after,
                     cast(rating_diff   as bigint)  as rating_diff
                 from rating_df
                 order by match_id, player_name
                 """)
    conn.unregister("rating_df")
