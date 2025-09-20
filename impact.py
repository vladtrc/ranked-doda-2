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
    """
    Impact math (frozen, role-calibrated, bounded):

      raw_pos = w_k*K + w_d*D + w_a*A + w_net*NW
      z_pos   = (raw_pos - bias_pos) / scale_pos
      impact  = 100 * tanh(z_pos)         ∈ [-100, 100]

    - role neutrality: subtract per-role bias (frozen median of RAW).
    - bounded scale: tanh bounds outputs; scale maps typical highs near +80.
      scale_pos ≈ (p95_raw - median_raw) / atanh(0.8).
    - stability: bias_pos and scale_pos are hard-coded from a reference snapshot.
      Do not recompute when new matches are added.
    """

    # 1) weights per position, with frozen calibration (bias, scale)
    conn.execute("""
        create or replace table v_impact_weights as
        select * from (values
            -- position, w_k, w_d,  w_a,  w_net,   bias,    scale
            (1,        2.0, -1.5,  0.5,  0.0020,  66.076,  46.737),
            (2,        2.0, -1.5,  0.5,  0.0018,  51.714,  45.468),
            (3,        1.5, -1.5,  0.7,  0.0015,  35.939,  31.285),
            (4,        0.7, -1.5,  1.5,  0.0010,  28.462,  25.630),
            (5,        0.5, -1.5,  2.0,  0.0008,  32.230,  25.298)
        ) as t(position, w_k, w_d, w_a, w_net, bias, scale);
    """)

    # 2) player-level RAW impact from linear weights only
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
                iw.w_k, iw.w_d, iw.w_a, iw.w_net
            from src_player s
            join v_impact_weights iw using (position)
        )
                 select
                     match_id,
                     player_name,
                     team,
                     position,
                     kills,
                     deaths,
                     assists,
                     net_worth,
                     w_k, w_d, w_a, w_net,
                     (w_k * kills) + (w_d * deaths) + (w_a * assists) + (w_net * net_worth) as impact
                 from jn_weights;
                 """)

    # 2b) calibrated, bounded impact view (keeps interface: raw_impact + impact)
    conn.execute("""
                 create or replace view v_impact_player as
        with base as (
            select
                r.match_id, r.player_name, r.team, r.position,
                r.kills, r.deaths, r.assists, r.net_worth,
                r.impact as raw_impact,
                iw.bias, iw.scale
            from v_impact_player_raw r
            join v_impact_weights iw using (position)
        )
                 select
                     match_id, player_name, team, position, kills, deaths, assists, net_worth,
                     raw_impact,
                     100 * tanh( (raw_impact - bias) / nullif(scale, 0) ) as impact
                 from base;
                 """)

    # 3) team impact per match (raw and calibrated)
    conn.execute("""
                 create or replace view v_impact_team_raw as
        with src as (select * from v_impact_player_raw)
                 select
                     match_id,
                     team,
                     sum(impact) as team_impact
                 from src
                 group by match_id, team;
                 """)
    conn.execute("""
                 create or replace view v_impact_team as
        with src as (select * from v_impact_player)
                 select
                     match_id,
                     team,
                     sum(impact) as team_impact
                 from src
                 group by match_id, team;
                 """)

    # 4) numeric outcome
    conn.execute("""
                 create or replace view v_impact_outcome as
                 select
                     m.match_id,
                     case
                         when m.winning_team = 'radiant' then 1
                         when m.winning_team = 'dire'    then -1
                         else null
                         end as outcome
                 from match m;
                 """)

    # 5) match-level impact diff (raw and calibrated)
    conn.execute("""
                 create or replace view v_impact_match_raw as
        with ti as (
            select
                match_id,
                max(case when team = 'radiant' then team_impact end) as radiant_impact,
                max(case when team = 'dire'    then team_impact end) as dire_impact
            from v_impact_team_raw
            group by match_id
        )
                 select
                     ti.match_id,
                     ti.radiant_impact,
                     ti.dire_impact,
                     (ti.radiant_impact - ti.dire_impact) as impact_diff,
                     o.outcome
                 from ti
                          join v_impact_outcome o using (match_id);
                 """)
    conn.execute("""
                 create or replace view v_impact_match as
        with ti as (
            select
                match_id,
                max(case when team = 'radiant' then team_impact end) as radiant_impact,
                max(case when team = 'dire'    then team_impact end) as dire_impact
            from v_impact_team
            group by match_id
        )
                 select
                     ti.match_id,
                     ti.radiant_impact,
                     ti.dire_impact,
                     (ti.radiant_impact - ti.dire_impact) as impact_diff,
                     o.outcome
                 from ti
                          join v_impact_outcome o using (match_id);
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
    # keep existing metrics EXACTLY
    init_impact_views(conn)

    # --- existing sections ---
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
                                         sum(case when abs(p.impact - ps.mean_impact) > 5 * ps.std_impact then 1 else 0 end) as n_outliers,
                                         count(*)                                                                           as n_total
                                     from v_impact_player p
                                              join pos_stats ps using (position)
                                     group by p.position, ps.std_impact, ps.mean_impact
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

    pos_team_rows = conn.execute("""
                                 with src as (select * from v_impact_player)
                                 select
                                     position, team, count(*) as n,
                                     avg(impact) as mean_impact,
                                     stddev_samp(impact) as std_impact,
                                     min(impact) as min_impact,
                                     max(impact) as max_impact
                                 from src
                                 group by position, team
                                 order by position, team;
                                 """).fetchall()

    team_rows = conn.execute("""
                             with src as (select * from v_impact_team)
                             select
                                 team, count(*) as n_matches,
                                 avg(team_impact) as mean_team_impact,
                                 stddev_samp(team_impact) as std_team_impact,
                                 min(team_impact) as min_team_impact,
                                 max(team_impact) as max_team_impact
                             from src
                             group by team
                             order by team;
                             """).fetchall()

    n_matches, corr_diff_outcome, accuracy = conn.execute("""
                                                          with src as (
                                                              select * from v_impact_match
                                                              where outcome is not null and impact_diff is not null
                                                          )
                                                          select
                                                              count(*)                                                     as n_matches,
                                                              corr(impact_diff, outcome)                                   as corr_diff_outcome,
                                                              avg(case when sign(impact_diff) = outcome then 1 else 0 end) as accuracy
                                                          from src;
                                                          """).fetchone()

    mean_rad, mean_dir, mean_diff = conn.execute("""
                                                 with x as (select radiant_impact, dire_impact, impact_diff from v_impact_match)
                                                 select avg(radiant_impact), avg(dire_impact), avg(impact_diff) from x;
                                                 """).fetchone()

    role_rows = conn.execute("""
                             with src as (select * from v_impact_player_raw)
                             select position, sum(impact) as sum_raw_impact
                             from src
                             group by position
                             order by position;
                             """).fetchall()

    # --- NEW: calibration hints (read-only, suggestions only) ---
    # Per-role RAW stats and suggested coefficients
    cal_rows = conn.execute("""
                            with base as (
                                select position, impact
                                from v_impact_player_raw
                            ),
                                 med as (
                                     select position, median(impact) as med
                                     from base
                                     group by position
                                 ),
                                 dev as (
                                     select b.position, abs(b.impact - m.med) as abs_dev
                                     from base b
                                              join med m using (position)
                                 ),
                                 mad as (
                                     select position, 1.4826 * median(abs_dev) as mad_raw
                                     from dev
                                     group by position
                                 ),
                                 s as (
                                     select
                                         b.position,
                                         count(*)                                as n,
                                         avg(b.impact)                           as mean_raw,
                                         median(b.impact)                        as median_raw,
                                         stddev_samp(b.impact)                   as sd_raw,
                                         m.med                                   as med_raw,
                                         q.quant01                               as p01,
                                         q.quant05                               as p05,
                                         q.quant25                               as p25,
                                         q.quant50                               as p50,
                                         q.quant75                               as p75,
                                         q.quant95                               as p95,
                                         q.quant99                               as p99
                                     from base b
                                              join med m using (position)
                                              join (
                                         select
                                             position,
                                             quantile_cont(impact, 0.01) as quant01,
                                             quantile_cont(impact, 0.05) as quant05,
                                             quantile_cont(impact, 0.25) as quant25,
                                             quantile_cont(impact, 0.50) as quant50,
                                             quantile_cont(impact, 0.75) as quant75,
                                             quantile_cont(impact, 0.95) as quant95,
                                             quantile_cont(impact, 0.99) as quant99
                                         from base
                                         group by position
                                     ) q using (position)
                                     group by b.position, m.med, q.quant01, q.quant05, q.quant25, q.quant50, q.quant75, q.quant95, q.quant99
                                 )
                            select
                                s.position, s.n,
                                s.mean_raw, s.median_raw, s.sd_raw, md.mad_raw,
                                s.p01, s.p05, s.p25, s.p50, s.p75, s.p95, s.p99,
                                -- candidate biases
                                s.mean_raw                                  as bias_mean,
                                s.median_raw                                as bias_median,
                                -- candidate scales
                                nullif(2.5 * md.mad_raw, 0)                 as scale_2p5mad,
                                nullif(s.sd_raw, 0)                         as scale_sd,
                                case when (s.p95 - s.median_raw) <> 0
                                         then (s.p95 - s.median_raw) / atanh(0.8)
                                     else null end                          as scale_map95_to_80,
                                case when (s.p99 - s.median_raw) <> 0
                                         then (s.p99 - s.median_raw) / atanh(0.95)
                                     else null end                          as scale_map99_to_95
                            from s
                                     join mad md using (position)
                            order by s.position;
                            """).fetchall()

    # RAW side bias by role
    raw_pos_team = conn.execute("""
                                with src as (select * from v_impact_player_raw)
                                select position, team,
                                       avg(impact) as mean_raw_impact,
                                       stddev_samp(impact) as sd_raw_impact
                                from src
                                group by position, team
                                order by position, team;
                                """).fetchall()

    # RAW team totals spread (pre-bounding) for context
    raw_team_rows = conn.execute("""
                                 with src as (select * from v_impact_team_raw)
                                 select team,
                                        avg(team_impact) as mean_team_raw,
                                        stddev_samp(team_impact) as sd_team_raw
                                 from src
                                 group by team
                                 order by team;
                                 """).fetchall()

    # Formatting
    def _fmt(x):
        return "None" if x is None else f"{x:.3f}"

    print("Per-position distribution (bounded):")
    for r in pos_rows:
        print(f"pos={r[0]}  n={r[1]}  mean={_fmt(r[2])}  std={_fmt(r[3])}  min={_fmt(r[4])}  max={_fmt(r[5])}  frac_gt_5σ={_fmt(r[6])}")

    print("\nPosition × Team spread (bounded):")
    for r in pos_team_rows:
        print(f"pos={r[0]}  team={r[1]}  n={r[2]}  mean={_fmt(r[3])}  std={_fmt(r[4])}  min={_fmt(r[5])}  max={_fmt(r[6])}")

    print("\nTeam spread (bounded):")
    for r in team_rows:
        print(f"team={r[0]}  n_matches={r[1]}  mean={_fmt(r[2])}  std={_fmt(r[3])}  min={_fmt(r[4])}  max={_fmt(r[5])}")

    print("\nMatch-level metrics (bounded):")
    print(f"n_matches={int(n_matches or 0)}")
    print(f"corr(impact_diff, outcome)={_fmt(corr_diff_outcome)}")
    print(f"accuracy(sign(impact_diff) predicts winner)={_fmt(accuracy)}")

    print("\nSide symmetry (bounded):")
    print(f"mean_radiant_impact={_fmt(mean_rad)}  mean_dire_impact={_fmt(mean_dir)}  mean_diff={_fmt(mean_diff)}")

    print("\nRaw role sums (neutrality check hint):")
    for r in role_rows:
        print(f"pos={r[0]}  sum_raw_impact={_fmt(r[1])}")

    # --- PRINT: Calibration hints (no changes applied) ---
    print("\nCalibration hints (RAW impacts; suggestions only):")
    print("Fields: pos  n  mean  median  sd  MAD  p01  p05  p25  p50  p75  p95  p99  bias_mean  bias_median  scale_2.5*MAD  scale_SD  scale_p95->+80  scale_p99->+95")
    for r in cal_rows:
        # unpack for readability
        (pos,n,mean_raw,median_raw,sd_raw,mad_raw,
         p01,p05,p25,p50,p75,p95,p99,
         bias_mean,bias_median,
         scale_mad,scale_sd,scale_95to80,scale_99to95) = r
        print(
            f"pos={pos}  n={n}  mean={_fmt(mean_raw)}  median={_fmt(median_raw)}  sd={_fmt(sd_raw)}  MAD={_fmt(mad_raw)}  "
            f"p01={_fmt(p01)}  p05={_fmt(p05)}  p25={_fmt(p25)}  p50={_fmt(p50)}  p75={_fmt(p75)}  p95={_fmt(p95)}  p99={_fmt(p99)}  "
            f"bias_mean={_fmt(bias_mean)}  bias_median={_fmt(bias_median)}  "
            f"scale_2.5*MAD={_fmt(scale_mad)}  scale_SD={_fmt(scale_sd)}  "
            f"scale_p95->+80={_fmt(scale_95to80)}  scale_p99->+95={_fmt(scale_99to95)}"
        )

    print("\nRaw side bias by role (mean±sd of RAW impacts):")
    for r in raw_pos_team:
        print(f"pos={r[0]}  team={r[1]}  mean_raw={_fmt(r[2])}  sd_raw={_fmt(r[3])}")

    print("\nRaw team totals (pre-bounding):")
    for r in raw_team_rows:
        print(f"team={r[0]}  mean_team_raw={_fmt(r[1])}  sd_team_raw={_fmt(r[2])}")
