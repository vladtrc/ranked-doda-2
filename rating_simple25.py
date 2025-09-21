"""
Simplified ranked MMR:
- Winners: +25
- Losers:  -25
- Starting rating: 0
- Deterministic order: date_time, match_id, team, position

Exports:
rating_result(match_id, player_name, rating_before, rating_after, rating_diff)
"""

from dataclasses import dataclass
from typing import Dict
from collections import defaultdict
import duckdb
import pandas as pd

@dataclass(frozen=True)
class MmrConfig:
    initial_mmr: int = 0
    team_radiant: str = "radiant"
    team_dire: str = "dire"
    win_delta: int = 25

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

def calculate_ranked_mmr(conn: duckdb.DuckDBPyConnection, cfg: MmrConfig = DEFAULT_MMR) -> None:
    _init_mmr_views(conn)
    df = conn.execute("select * from v_mmr_ordered").df()

    ratings: Dict[str, int] = defaultdict(lambda: int(cfg.initial_mmr))
    rows = []

    for (mid, dt, dur, rk, dk, win), g in df.groupby(
            ["match_id", "date_time", "duration_sec", "radiant_kills", "dire_kills", "winning_team"], sort=False
    ):
        win_team = str(win)
        for _, r in g.iterrows():
            name = r.player_name
            before = ratings[name]
            is_winner = (str(r.team) == win_team)
            delta = cfg.win_delta if is_winner else -cfg.win_delta
            after = before + delta
            ratings[name] = after

            rows.append({
                "match_id": int(mid),
                "player_name": name,
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
                     cast(rating_before as bigint)  as rating_before,
                     cast(rating_after  as bigint)  as rating_after,
                     cast(rating_diff   as bigint)  as rating_diff
                 from rating_df
                 order by match_id, player_name
                 """)
    conn.unregister("rating_df")
