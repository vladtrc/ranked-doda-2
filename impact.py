"""
Goals:
- Role neutrality: average impact per role = 0 (no built-in bias).
- Bounded scale: player impact always within [-100, 100].
- Stable distribution: roughly bell-shaped, with <1% extreme outliers.
- Historical impacts remain unchanged when new matches are added.

Exports:
- calculate_impacts(conn): creates table impact_result (match_id:int64, player_name:varchar, impact:double).
  Creates helper DuckDB views and tables with prefix `v_impact_*`. Avoid relying on them.

- debug_impacts(conn): Uses calculate_impacts internally, then prints diagnostics. Floods global state.

"""
import duckdb


def init_impact_views(conn: duckdb.DuckDBPyConnection) -> None:
    # 1) weights per position
    conn.execute("""
        create or replace table v_impact_weights as
        with params_weights as (
            select 1::int as position, 2.0::double as w_k,  -1.5::double as w_d, 0.5::double as w_a,  0.0020::double as w_net
            union all select 2,         2.0,                 -1.5,              0.5,                 0.0018
            union all select 3,         1.5,                 -1.5,              0.7,                 0.0015
            union all select 4,         0.7,                 -1.5,              1.5,                 0.0010
            union all select 5,         0.5,                 -1.5,              2.0,                 0.0008
        )
        select * from params_weights;
    """)

    # 2) player-level raw impact
    conn.execute("""
                 create or replace view v_impact_player_raw as
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
            select
                s.*,
                iw.w_k,
                iw.w_d,
                iw.w_a,
                iw.w_net
            from src_player s
            join v_impact_weights iw using (position)
        ),
        calc_raw as (
            select
                match_id,
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
            from jn_weights
        )
                 select * from calc_raw;
                 """)

    # 2b) fixed calibration per role
    conn.execute("""
        create or replace table v_impact_calibration as
        with params_cal as (
            select 1::int as position, 0.0::double as bias, 45.0::double as scale
            union all select 2,        0.0,                45.0
            union all select 3,        0.0,                40.0
            union all select 4,        0.0,                35.0
            union all select 5,        0.0,                30.0
        )
        select * from params_cal;
    """)

    # 2c) bounded impact
    conn.execute("""
                 create or replace view v_impact_player as
        with src_raw as (
            select * from v_impact_player_raw
        ),
        jn_cal as (
            select
                p.*,
                ic.bias,
                ic.scale
            from src_raw p
            join v_impact_calibration ic using (position)
        ),
        calc_bounded as (
            select
                match_id,
                player_name,
                team,
                position,
                kills,
                deaths,
                assists,
                net_worth,
                impact as raw_impact,
                100.0 * tanh((impact - bias) / nullif(scale, 0.0)) as impact
            from jn_cal
        )
                 select * from calc_bounded;
                 """)

    # 3) team impact per match
    conn.execute("""
                 create or replace view v_impact_team_raw as
        with src as (select * from v_impact_player_raw),
        agg as (
            select
                match_id,
                team,
                sum(impact) as team_impact
            from src
            group by match_id, team
        )
                 select * from agg;
                 """)
    conn.execute("""
                 create or replace view v_impact_team as
        with src as (select * from v_impact_player),
        agg as (
            select
                match_id,
                team,
                sum(impact) as team_impact
            from src
            group by match_id, team
        )
                 select * from agg;
                 """)

    # 4) numeric outcome
    conn.execute("""
                 create or replace view v_impact_outcome as
        with src as (
            select
                m.match_id,
                case
                    when m.winning_team = 'radiant' then 1
                    when m.winning_team = 'dire' then -1
                    else null
                end as outcome
            from match m
        )
                 select * from src;
                 """)

    # 5) match-level impact diff
    conn.execute("""
                 create or replace view v_impact_match_raw as
        with ti as (
            select
                match_id,
                max(case when team = 'radiant' then team_impact end) as radiant_impact,
                max(case when team = 'dire' then team_impact end)     as dire_impact
            from v_impact_team_raw
            group by match_id
        ),
        pres as (
            select
                ti.match_id,
                ti.radiant_impact,
                ti.dire_impact,
                (ti.radiant_impact - ti.dire_impact) as impact_diff
            from ti
        ),
        jn as (
            select
                p.match_id,
                p.radiant_impact,
                p.dire_impact,
                p.impact_diff,
                o.outcome
            from pres p
            join v_impact_outcome o using (match_id)
        )
                 select * from jn;
                 """)
    conn.execute("""
                 create or replace view v_impact_match as
        with ti as (
            select
                match_id,
                max(case when team = 'radiant' then team_impact end) as radiant_impact,
                max(case when team = 'dire' then team_impact end)     as dire_impact
            from v_impact_team
            group by match_id
        ),
        pres as (
            select
                ti.match_id,
                ti.radiant_impact,
                ti.dire_impact,
                (ti.radiant_impact - ti.dire_impact) as impact_diff
            from ti
        ),
        jn as (
            select
                p.match_id,
                p.radiant_impact,
                p.dire_impact,
                p.impact_diff,
                o.outcome
            from pres p
            join v_impact_outcome o using (match_id)
        )
                 select * from jn;
                 """)


def calculate_impacts(conn: duckdb.DuckDBPyConnection) -> None:
    init_impact_views(conn)
    conn.execute("""
        create or replace table impact_result as
        with src as (
            select match_id, player_name, impact
            from v_impact_player
        ),
        pres as (
            select * from src
            order by match_id, player_name
        )
        select * from pres;
    """)


def debug_impacts(conn: duckdb.DuckDBPyConnection) -> None:
    # per-position distribution
    pos_rows = conn.execute("""
                            with pos_stats as (
                                select
                                    position,
                                    count(*)            as n,
                                    avg(impact)         as mean_impact,
                                    stddev_samp(impact) as std_impact,
                                    min(impact)         as min_impact,
                                    max(impact)         as max_impact
                                from v_impact_player
                                group by position
                            ),
                                 outlier_counts as (
                                     select
                                         p.position,
                                         sum(case when abs(p.impact) > 5 * ps.std_impact then 1 else 0 end) as n_outliers,
                                         count(*)                                                           as n_total
                                     from v_impact_player p
                                              join pos_stats ps using (position)
                                     group by p.position, ps.std_impact
                                 )
                            select
                                ps.position,
                                ps.n,
                                ps.mean_impact,
                                ps.std_impact,
                                ps.min_impact,
                                ps.max_impact,
                                cast(oc.n_outliers as double) / nullif(oc.n_total, 0) as frac_gt_5sigma
                            from pos_stats ps
                                     join outlier_counts oc using (position)
                            order by ps.position;
                            """).fetchall()

    # team spread
    team_rows = conn.execute("""
                             with src as (select * from v_impact_team),
                                  agg as (
                                      select
                                          team,
                                          avg(team_impact)         as mean_team_impact,
                                          stddev_samp(team_impact) as std_team_impact
                                      from src
                                      group by team
                                  )
                             select * from agg order by team;
                             """).fetchall()

    # match-level correlation & accuracy
    n_matches, corr_diff_outcome, accuracy = conn.execute("""
                                                          with src as (
                                                              select * from v_impact_match
                                                              where outcome is not null
                                                                and impact_diff is not null
                                                          )
                                                          select
                                                              count(*)                                                     as n_matches,
                                                              corr(impact_diff, outcome)                                   as corr_diff_outcome,
                                                              avg(case when sign(impact_diff) = outcome then 1 else 0 end) as accuracy
                                                          from src;
                                                          """).fetchone()

    # raw role zero-sum check
    role_rows = conn.execute("""
                             with src as (select * from v_impact_player_raw),
                                  agg as (
                                      select
                                          position,
                                          sum(impact) as sum_impact_all_matches
                                      from src
                                      group by position
                                  )
                             select * from agg order by position;
                             """).fetchall()

    # printing
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
