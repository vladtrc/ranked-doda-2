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

from dataclasses import dataclass
from typing import List

import duckdb


@dataclass(frozen=True)
class RoleCoeff:
    position: int
    w_k: float
    w_d: float
    w_a: float
    w_n: float


@dataclass(frozen=True)
class ImpactCoeffs:
    rows: List[RoleCoeff]


DEFAULT_COEFFS = ImpactCoeffs(
    rows=[RoleCoeff(position=1, w_k=1.3, w_d=-10.0, w_a=1.3, w_n=0.0009),
          RoleCoeff(position=2, w_k=1.4, w_d=-6.9, w_a=1.7, w_n=0.00017),
          RoleCoeff(position=3, w_k=2.2, w_d=-6.4, w_a=1.8, w_n=0.00019),
          RoleCoeff(position=4, w_k=2.1, w_d=-5.0, w_a=1.1, w_n=0.00081),
          RoleCoeff(position=5, w_k=1.2, w_d=-4.1, w_a=1.1, w_n=0.00116)])


def _register_coeffs_table(conn: duckdb.DuckDBPyConnection, coeffs: ImpactCoeffs) -> None:
    values_sql = ",\n            ".join(
        f"({rc.position}, {rc.w_k}, {rc.w_d}, {rc.w_a}, {rc.w_n})"
        for rc in coeffs.rows
    )
    conn.execute(f"""
        create or replace table v_impact_weights as
        select
            cast(position as integer) as position,
            cast(w_k      as double)  as w_k,
            cast(w_d      as double)  as w_d,
            cast(w_a      as double)  as w_a,
            cast(w_net    as double)  as w_net
        from (
            select * from (values
                {values_sql}
            ) as t(position, w_k, w_d, w_a, w_net)
        );
    """)


def init_impact_views(conn: duckdb.DuckDBPyConnection, coeffs: ImpactCoeffs) -> None:
    _register_coeffs_table(conn, coeffs)
    conn.execute("""
                 create
                 or replace view v_impact_player as
                 with src as (
                     select
                         pr.match_id,
                         pr.player_name,
                         pr.team,
                         pr.position,
                         cast(pr.kills      as double) as kills,
                         cast(pr.deaths     as double) as deaths,
                         cast(pr.assists    as double) as assists,
                         cast(pr.net_worth  as double) as net_worth
                     from player_result pr
                 )
                 select s.match_id,
                        s.player_name,
                        s.team,
                        s.position,
                        s.kills,
                        s.deaths,
                        s.assists,
                        s.net_worth,
                        iw.w_k,
                        iw.w_d,
                        iw.w_a,
                        iw.w_net,
                        least(100,
                              greatest(-100,
                                       (iw.w_k * s.kills) +
                                       (iw.w_d * s.deaths) +
                                       (iw.w_a * s.assists) +
                                       (iw.w_net * s.net_worth)
                              )
                        ) as impact
                 from src s
                          join v_impact_weights iw using (position);
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
