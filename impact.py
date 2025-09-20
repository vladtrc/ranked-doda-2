"""
Goals:
- Role neutrality: average impact per role = 0 (no built-in bias).
- Bounded scale: player impact always within [-100, 100].
- Stable distribution: roughly bell-shaped, with <1% extreme outliers.
- Historical impacts remain unchanged when new matches are added.
- Temporal consistency: adding or removing a single match should not materially shift long-run player impact averages (e.g. across 100 matches).

Exports:
- calculate_impacts(conn): creates table impact_result (match_id:int64, player_name:varchar, impact:double).
  Creates helper DuckDB views and tables with prefix `v_impact_*`. Avoid relying on them.
"""
import duckdb
from dataclasses import dataclass
from typing import List

import pandas as pd


@dataclass(frozen=True)
class RoleCoeff:
    position: int
    w_k: float
    w_d: float
    w_a: float
    w_net: float
    bias: float
    scale: float


@dataclass(frozen=True)
class ImpactCoeffs:
    rows: List[RoleCoeff]

    def to_df(self) -> pd.DataFrame:
        df = pd.DataFrame([{
            "position": rc.position,
            "w_k": rc.w_k,
            "w_d": rc.w_d,
            "w_a": rc.w_a,
            "w_net": rc.w_net,
            "bias": rc.bias,
            "scale": rc.scale,
        } for rc in self.rows])
        return df.set_index("position").sort_index()

    @classmethod
    def from_df(cls, df: pd.DataFrame) -> "ImpactCoeffs":
        if "position" in df.columns:
            it = df.itertuples(index=False)
            get_pos = lambda r: int(r.position)
        else:
            df = df.reset_index().rename(columns={"index": "position"})
            it = df.itertuples(index=False)
            get_pos = lambda r: int(r.position)

        rows = [
            RoleCoeff(
                position=get_pos(r),
                w_k=float(r.w_k),
                w_d=float(r.w_d),
                w_a=float(r.w_a),
                w_net=float(r.w_net),
                bias=float(r.bias),
                scale=float(r.scale),
            )
            for r in it
        ]
        rows.sort(key=lambda rc: rc.position)
        return cls(rows=rows)


DEFAULT_COEFFS = ImpactCoeffs(rows=[
    RoleCoeff(position=1, w_k=1, w_d=-2, w_a=1, w_net=0.0001, bias=36, scale=21),
    RoleCoeff(position=2, w_k=1, w_d=-1.5, w_a=1, w_net=0.0001, bias=27, scale=20),
    RoleCoeff(position=3, w_k=1, w_d=-1, w_a=1, w_net=0.0001, bias=16, scale=17),
    RoleCoeff(position=4, w_k=1, w_d=-0.5, w_a=1, w_net=0.0001, bias=3, scale=12),
    RoleCoeff(position=5, w_k=1, w_d=-0.5, w_a=1, w_net=0.0001, bias=-2, scale=9)
]
)


def _register_coeffs_table(conn: duckdb.DuckDBPyConnection, coeffs: ImpactCoeffs) -> None:
    values_sql = ",\n            ".join(
        f"({rc.position}, {rc.w_k}, {rc.w_d}, {rc.w_a}, {rc.w_net}, {rc.bias}, {rc.scale})"
        for rc in coeffs.rows
    )
    conn.execute(f"""
        create or replace table v_impact_weights as
        select
            cast(position as integer)   as position,
            cast(w_k     as double)     as w_k,
            cast(w_d     as double)     as w_d,
            cast(w_a     as double)     as w_a,
            cast(w_net   as double)     as w_net,
            cast(bias    as double)     as bias,
            cast(scale   as double)     as scale
        from (
            select * from (values
                {values_sql}
            ) as t(position, w_k, w_d, w_a, w_net, bias, scale)
        );
    """)


def init_impact_views(conn: duckdb.DuckDBPyConnection, coeffs: ImpactCoeffs) -> None:
    """
    Impact math (role-calibrated, bounded):

      raw_pos = w_k*K + w_d*D + w_a*A + w_net*NW
      z_pos   = (raw_pos - bias_pos) / scale_pos
      impact  = 100 * tanh(z_pos)         ∈ [-100, 100]

    - Role neutrality: subtract per-role bias from a frozen snapshot.
    - Bounded scale: tanh bounds outputs; scale maps typical highs near +80.
    - Stability: pass DEFAULT_COEFFS to keep historical numbers unchanged.
    """
    _register_coeffs_table(conn, coeffs)

    # Player RAW
    conn.execute("""
                 create
                 or replace view v_impact_player_raw as
        with src_player as (
            select
                pr.match_id,
                pr.player_name,
                pr.team,
                pr.position,
                pr.kills,
                pr.deaths,
                pr.assists,
                pr.net_worth
            from player_result pr
        ),
        jn_weights as (
            select s.*, iw.w_k, iw.w_d, iw.w_a, iw.w_net
            from src_player s
            join v_impact_weights iw using (position)
        )
                 select match_id,
                        player_name,
                        team,
                        position,
                        kills,
                        deaths,
                        assists,
                        net_worth,
                        w_k,
                        w_d,
                        w_a,
                        w_net,
                        (w_k * kills) + (w_d * deaths) + (w_a * assists) + (w_net * net_worth) as impact
                 from jn_weights;
                 """)

    # Player bounded
    conn.execute("""
                 create
                 or replace view v_impact_player as
    with base as (
        select
            r.match_id, r.player_name, r.team, r.position,
            cast(r.kills      as double) as kills,
            cast(r.deaths     as double) as deaths,
            cast(r.assists    as double) as assists,
            cast(r.net_worth  as double) as net_worth,
            cast(r.impact     as double) as raw_impact,
            cast(iw.bias      as double) as bias,
            cast(iw.scale     as double) as scale
        from v_impact_player_raw r
        join v_impact_weights iw using (position)
    )
                 select match_id,
                        player_name,
                        team,
                        position,
                        kills,
                        deaths,
                        assists,
                        net_worth,
                        raw_impact,
                        cast(100.0 * tanh((raw_impact - bias) / nullif(scale, 0.0)) as double) as impact
                 from base;
                 """)

    # Team RAW and bounded
    conn.execute("""
                 create
                 or replace view v_impact_team_raw as
                 select match_id, team, sum(impact) as team_impact
                 from v_impact_player_raw
                 group by match_id, team;
                 """)
    conn.execute("""
                 create
                 or replace view v_impact_team as
                 select match_id, team, sum(impact) as team_impact
                 from v_impact_player
                 group by match_id, team;
                 """)

    # Outcome
    conn.execute("""
                 create
                 or replace view v_impact_outcome as
                 select m.match_id,
                        case
                            when m.winning_team = 'radiant' then 1
                            when m.winning_team = 'dire' then -1
                            else null end as outcome
                 from match m;
                 """)

    # Match RAW and bounded
    conn.execute("""
                 create
                 or replace view v_impact_match_raw as
        with ti as (
            select
                match_id,
                max(case when team='radiant' then team_impact end) as radiant_impact,
                max(case when team='dire'    then team_impact end) as dire_impact
            from v_impact_team_raw
            group by match_id
        )
                 select ti.match_id,
                        ti.radiant_impact,
                        ti.dire_impact,
                        (ti.radiant_impact - ti.dire_impact) as impact_diff,
                        o.outcome
                 from ti
                          join v_impact_outcome o using (match_id);
                 """)
    conn.execute("""
                 create
                 or replace view v_impact_match as
        with ti as (
            select
                match_id,
                max(case when team='radiant' then team_impact end) as radiant_impact,
                max(case when team='dire'    then team_impact end) as dire_impact
            from v_impact_team
            group by match_id
        )
                 select ti.match_id,
                        ti.radiant_impact,
                        ti.dire_impact,
                        (ti.radiant_impact - ti.dire_impact) as impact_diff,
                        o.outcome
                 from ti
                          join v_impact_outcome o using (match_id);
                 """)


def _write_impact_table(conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
    conn.execute(f"""
        create or replace table {table_name} as
        select match_id, player_name, impact
        from v_impact_player
        order by match_id, player_name;
    """)


def calculate_impacts(conn: duckdb.DuckDBPyConnection) -> None:
    init_impact_views(conn, DEFAULT_COEFFS)
    _write_impact_table(conn, "impact_result")
