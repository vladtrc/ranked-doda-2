from ..db import get_conn

LEADER_TREND_COLORS = ["#7dd3fc", "#2dd4bf", "#a78bfa", "#facc15", "#f472b6"]
LOSER_TREND_COLORS = ["#fb7185", "#ef4444", "#f97316", "#f59e0b", "#dc2626"]
TREND_PLAYER_LIMIT = 3
LANE_ENTRY_LIMIT = 3


def _recent_matches(match_window: int | None) -> tuple[str, list[int]]:
    if match_window is None:
        return (
            """
            recent_matches AS (
                SELECT match_id, date_time
                FROM "match"
            )
            """,
            [],
        )
    return (
        """
        recent_matches AS (
            SELECT match_id, date_time
            FROM "match"
            ORDER BY date_time DESC
            LIMIT ?
        )
        """,
        [match_window],
    )


def _build_smooth_path(points: list[tuple[float, float]]) -> str:
    if not points:
        return ""
    if len(points) == 1:
        x, y = points[0]
        return f"M {x:.1f},{y:.1f}"
    if len(points) == 2:
        return f"M {points[0][0]:.1f},{points[0][1]:.1f} L {points[1][0]:.1f},{points[1][1]:.1f}"

    tension = 0.12
    commands = [f"M {points[0][0]:.1f},{points[0][1]:.1f}"]
    for idx in range(len(points) - 1):
        p0 = points[idx - 1] if idx > 0 else points[idx]
        p1 = points[idx]
        p2 = points[idx + 1]
        p3 = points[idx + 2] if idx + 2 < len(points) else p2

        cp1x = p1[0] + (p2[0] - p0[0]) * tension
        cp1y = p1[1] + (p2[1] - p0[1]) * tension
        cp2x = p2[0] - (p3[0] - p1[0]) * tension
        cp2y = p2[1] - (p3[1] - p1[1]) * tension
        commands.append(
            f"C {cp1x:.1f},{cp1y:.1f} {cp2x:.1f},{cp2y:.1f} {p2[0]:.1f},{p2[1]:.1f}"
        )
    return " ".join(commands)


def _smooth_zigzag(points: list[dict]) -> list[dict]:
    """Average out alternating local extrema (zigzag noise) with a few passes."""
    if len(points) < 3:
        return points
    values = [p["value"] for p in points]
    for _ in range(4):
        new = list(values)
        for i in range(1, len(values) - 1):
            prev, curr, nxt = values[i - 1], values[i], values[i + 1]
            if (curr > prev and curr > nxt) or (curr < prev and curr < nxt):
                new[i] = (prev + nxt) / 2
        values = new
    return [{**p, "value": v} for p, v in zip(points, values)]


def build_trend_chart(lines: list[dict], palette: list[str], total_matches: int = 0) -> dict:
    width = 760
    height = 320
    pad_left = 52
    pad_right = 22
    pad_top = 24
    pad_bottom = 38
    inner_width = width - pad_left - pad_right
    inner_height = height - pad_top - pad_bottom

    all_points = [point for line in lines for point in line["points"]]
    if not all_points:
        return {"width": width, "height": height, "lines": [], "y_ticks": [], "x_ticks": []}

    all_values = [p["value"] for p in all_points]
    min_value = min(all_values)
    max_value = max(all_values)
    if min_value == max_value:
        min_value -= 1
        max_value += 1

    value_padding = max(4.0, (max_value - min_value) * 0.12)
    chart_min = min_value - value_padding
    chart_max = max_value + value_padding
    value_span = chart_max - chart_min
    max_idx = max(total_matches, 1)

    def scale_x(match_idx: int) -> float:
        return pad_left + (match_idx / max_idx) * inner_width

    def scale_y(value: float) -> float:
        return pad_top + (1 - ((value - chart_min) / value_span)) * inner_height

    rendered_lines: list[dict] = []
    for idx, line in enumerate(lines):
        points = line["points"]
        scaled_points = [(scale_x(p["match_idx"]), scale_y(p["value"])) for p in points]
        polyline = " ".join(f"{x:.1f},{y:.1f}" for x, y in scaled_points)
        path = _build_smooth_path(scaled_points)
        last_point = points[-1]
        rendered_lines.append(
            {
                **line,
                "color": line.get("color", palette[idx % len(palette)]),
                "polyline": polyline,
                "path": path,
                "end_x": round(scale_x(last_point["match_idx"]), 1),
                "end_y": round(scale_y(last_point["value"]), 1),
            }
        )

    y_ticks = []
    tick_count = 5
    tick_start = int(chart_min)
    tick_end = int(chart_max)
    if tick_start == tick_end:
        tick_end = tick_start + tick_count - 1
    for idx in range(tick_count):
        value = round(tick_start + ((tick_end - tick_start) * idx / (tick_count - 1)))
        y_ticks.append({"value": int(value), "y": round(scale_y(value), 1)})

    # X ticks: 5 evenly spaced match indices labelled by date
    idx_to_date = {}
    for p in all_points:
        idx_to_date[p["match_idx"]] = p["date_time"]
    sorted_indices = sorted(idx_to_date)
    x_tick_count = min(5, len(sorted_indices))
    x_ticks = []
    for i in range(x_tick_count):
        si = round(i * (len(sorted_indices) - 1) / max(x_tick_count - 1, 1))
        midx = sorted_indices[si]
        anchor = "start" if i == 0 else ("end" if i == x_tick_count - 1 else "middle")
        x_ticks.append({"label": idx_to_date[midx].strftime("%Y-%m-%d"), "x": round(scale_x(midx), 1), "anchor": anchor})

    return {
        "width": width,
        "height": height,
        "lines": rendered_lines,
        "y_ticks": y_ticks,
        "x_ticks": x_ticks,
    }


def fetch_dashboard_trends(match_window: int, direction: str = "desc") -> dict:
    conn = get_conn()
    order_dir = "desc" if direction == "desc" else "asc"
    palette = LEADER_TREND_COLORS if direction == "desc" else LOSER_TREND_COLORS
    recent_matches_cte, params = _recent_matches(match_window)
    rows = conn.execute(
        f"""
        WITH {recent_matches_cte},
        match_seq AS (
            SELECT match_id, date_time,
                row_number() OVER (ORDER BY date_time ASC, match_id ASC) AS match_idx
            FROM recent_matches
        ),
        player_trend AS (
            SELECT
                pr.player_name,
                ms.match_id,
                ms.match_idx,
                ms.date_time,
                sum(CASE WHEN pr.team = m.winning_team THEN 1 ELSE -1 END) OVER (
                    PARTITION BY pr.player_name
                    ORDER BY ms.match_idx
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cumulative_wl,
                count(*) OVER (PARTITION BY pr.player_name) AS total_games
            FROM player_result pr
            JOIN match_seq ms USING (match_id)
            JOIN "match" m USING (match_id)
        ),
        leaders AS (
            SELECT player_name, cumulative_wl AS latest_cumulative_wl
            FROM player_trend
            WHERE total_games >= 5
            QUALIFY row_number() OVER (PARTITION BY player_name ORDER BY match_idx DESC) = 1
            ORDER BY latest_cumulative_wl {order_dir}, player_name ASC
            LIMIT {TREND_PLAYER_LIMIT}
        ),
        filled AS (
            SELECT
                ms.match_idx,
                ms.date_time,
                l.player_name,
                COALESCE(
                    last_value(pt.cumulative_wl IGNORE NULLS) OVER (
                        PARTITION BY l.player_name ORDER BY ms.match_idx
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ),
                    0
                ) AS cumulative_wl,
                l.latest_cumulative_wl
            FROM match_seq ms
            CROSS JOIN leaders l
            LEFT JOIN player_trend pt
                ON pt.match_id = ms.match_id AND pt.player_name = l.player_name
        )
        SELECT player_name, date_time, match_idx, cumulative_wl, latest_cumulative_wl
        FROM filled
        ORDER BY match_idx ASC, latest_cumulative_wl {order_dir}, player_name ASC
        """,
        params,
    ).fetchall()

    by_player: dict[str, list[dict]] = {}
    latest_scores: dict[str, float] = {}
    total_matches = 0

    for player_name, date_time, match_idx, cumulative_wl, latest_score in rows:
        by_player.setdefault(player_name, []).append({"match_idx": match_idx, "date_time": date_time, "value": cumulative_wl})
        latest_scores[player_name] = latest_score
        if match_idx > total_matches:
            total_matches = match_idx

    reverse_scores = direction == "desc"
    lines = []
    for player_name, points in sorted(
        by_player.items(),
        key=lambda item: (latest_scores[item[0]], item[0]),
        reverse=reverse_scores,
    ):
        # prepend true zero at match_idx=0
        full_points = [{"match_idx": 0, "date_time": points[0]["date_time"], "value": 0}] + points
        lines.append(
            {
                "player_name": player_name,
                "points": _smooth_zigzag(full_points),
                "latest_value": round(latest_scores[player_name], 1),
            }
        )

    return build_trend_chart(lines, palette, total_matches)


def fetch_dashboard_lane_stats(match_window: int | None) -> dict[str, dict[str, list[dict]]]:
    conn = get_conn()
    recent_matches_cte, params = _recent_matches(match_window)
    rows = conn.execute(
        f"""
        WITH {recent_matches_cte},
        lane_entries AS (
            SELECT
                'safe' AS lane,
                p1.player_name || ' + ' || p5.player_name AS lineup,
                p1.team,
                m.winning_team
            FROM recent_matches rm
            JOIN "match" m USING (match_id)
            JOIN player_result p1 ON p1.match_id = rm.match_id AND p1.position = 1
            JOIN player_result p5 ON p5.match_id = rm.match_id AND p5.team = p1.team AND p5.position = 5

            UNION ALL

            SELECT
                'mid' AS lane,
                p2.player_name AS lineup,
                p2.team,
                m.winning_team
            FROM recent_matches rm
            JOIN "match" m USING (match_id)
            JOIN player_result p2 ON p2.match_id = rm.match_id AND p2.position = 2

            UNION ALL

            SELECT
                'hard' AS lane,
                p3.player_name || ' + ' || p4.player_name AS lineup,
                p3.team,
                m.winning_team
            FROM recent_matches rm
            JOIN "match" m USING (match_id)
            JOIN player_result p3 ON p3.match_id = rm.match_id AND p3.position = 3
            JOIN player_result p4 ON p4.match_id = rm.match_id AND p4.team = p3.team AND p4.position = 4
        ),
        lane_summary AS (
            SELECT
                lane,
                lineup,
                count(*) AS games,
                sum(CASE WHEN team = winning_team THEN 1 ELSE 0 END) AS wins,
                round(100.0 * sum(CASE WHEN team = winning_team THEN 1 ELSE 0 END) / count(*), 1) AS win_pct
            FROM lane_entries
            GROUP BY lane, lineup
        ),
        ranked AS (
            SELECT
                lane,
                lineup,
                games,
                wins,
                games - wins AS losses,
                win_pct,
                row_number() OVER (
                    PARTITION BY lane
                    ORDER BY (wins + 8.0) / (games + 16.0) ASC, games DESC, lineup ASC
                ) AS worst_rn,
                row_number() OVER (
                    PARTITION BY lane
                    ORDER BY (wins + 8.0) / (games + 16.0) DESC, games DESC, lineup ASC
                ) AS best_rn
            FROM lane_summary
        )
        SELECT
            lane,
            'best' AS bucket,
            lineup,
            games,
            wins,
            losses,
            win_pct,
            best_rn,
            worst_rn
        FROM ranked
        WHERE best_rn <= {LANE_ENTRY_LIMIT}

        UNION ALL

        SELECT
            lane,
            'worst' AS bucket,
            lineup,
            games,
            wins,
            losses,
            win_pct,
            best_rn,
            worst_rn
        FROM ranked
        WHERE worst_rn <= {LANE_ENTRY_LIMIT}
        ORDER BY lane ASC, bucket ASC, best_rn ASC, worst_rn ASC
        """,
        params,
    ).fetchall()

    result = {
        "safe": {"best": [], "worst": []},
        "mid": {"best": [], "worst": []},
        "hard": {"best": [], "worst": []},
    }
    for lane, bucket, lineup, games, wins, losses, win_pct, _best_rn, _worst_rn in rows:
        if bucket is None:
            continue
        result[lane][bucket].append(
            {
                "lineup": lineup,
                "games": games,
                "wins": wins,
                "losses": losses,
                "win_pct": win_pct,
            }
        )
    return result
