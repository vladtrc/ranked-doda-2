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
    return [{"date_time": p["date_time"], "value": v} for p, v in zip(points, values)]


def _build_trend_chart(lines: list[dict], palette: list[str]) -> dict:
    width = 760
    height = 320
    pad_left = 52
    pad_right = 22
    pad_top = 24
    pad_bottom = 38
    inner_width = width - pad_left - pad_right
    inner_height = height - pad_top - pad_bottom

    all_values = [point["value"] for line in lines for point in line["points"]]
    max_points = max((len(line["points"]) for line in lines), default=0)
    if max_points == 0 or not all_values:
        return {"width": width, "height": height, "lines": [], "y_ticks": [], "x_ticks": []}

    min_value = min(all_values)
    max_value = max(all_values)
    if min_value == max_value:
        min_value -= 1
        max_value += 1

    value_padding = max(4.0, (max_value - min_value) * 0.12)
    chart_min = min_value - value_padding
    chart_max = max_value + value_padding
    value_span = chart_max - chart_min
    max_index = max(max_points - 1, 1)

    def scale_x(index: int) -> float:
        return pad_left + ((index / max_index) * inner_width)

    def scale_y(value: float) -> float:
        return pad_top + (1 - ((value - chart_min) / value_span)) * inner_height

    rendered_lines: list[dict] = []
    for idx, line in enumerate(lines):
        points = line["points"]
        scaled_points = [
            (scale_x(point_idx), scale_y(point["value"]))
            for point_idx, point in enumerate(points)
        ]
        polyline = " ".join(f"{x:.1f},{y:.1f}" for x, y in scaled_points)
        path = _build_smooth_path(scaled_points)
        last_index = len(points) - 1
        last_point = points[last_index]
        rendered_lines.append(
            {
                **line,
                "color": palette[idx % len(palette)],
                "polyline": polyline,
                "path": path,
                "end_x": round(scale_x(last_index), 1),
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

    longest_line = max(lines, key=lambda line: len(line["points"]))
    x_tick_count = min(5, max_points)
    x_ticks = []
    for idx in range(x_tick_count):
        point_index = round(idx * max_index / (x_tick_count - 1)) if x_tick_count > 1 else 0
        tick_date = longest_line["points"][point_index]["date_time"]
        anchor = "middle"
        if idx == 0:
            anchor = "start"
        elif idx == x_tick_count - 1:
            anchor = "end"
        x_ticks.append(
            {
                "label": tick_date.strftime("%Y-%m-%d"),
                "x": round(scale_x(point_index), 1),
                "anchor": anchor,
            }
        )

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
        player_trend AS (
            SELECT
                pr.player_name,
                rm.date_time,
                sum(case when pr.team = m.winning_team then 1 else -1 end) OVER (
                    PARTITION BY pr.player_name
                    ORDER BY rm.date_time, rm.match_id
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cumulative_wl,
                count(*) OVER (PARTITION BY pr.player_name) AS total_games
            FROM player_result pr
            JOIN recent_matches rm USING (match_id)
            JOIN "match" m USING (match_id)
        ),
        latest AS (
            SELECT
                player_name,
                cumulative_wl,
                total_games,
                row_number() OVER (
                    PARTITION BY player_name
                    ORDER BY date_time DESC
                ) AS rn
            FROM player_trend
            WHERE total_games >= 5
        ),
        leaders AS (
            SELECT player_name, cumulative_wl
            FROM latest
            WHERE rn = 1
            ORDER BY cumulative_wl {order_dir}, player_name ASC
            LIMIT {TREND_PLAYER_LIMIT}
        )
        SELECT
            pt.player_name,
            pt.date_time,
            pt.cumulative_wl,
            l.cumulative_wl AS latest_cumulative_wl
        FROM player_trend pt
        JOIN leaders l USING (player_name)
        ORDER BY pt.date_time ASC, l.cumulative_wl {order_dir}, pt.player_name ASC
        """,
        params,
    ).fetchall()

    by_player: dict[str, list[dict]] = {}
    latest_scores: dict[str, float] = {}
    for player_name, date_time, cumulative_wl, latest_score in rows:
        by_player.setdefault(player_name, []).append({"date_time": date_time, "value": cumulative_wl})
        latest_scores[player_name] = latest_score

    reverse_scores = direction == "desc"
    lines = []
    for player_name, points in sorted(
        by_player.items(),
        key=lambda item: (latest_scores[item[0]], item[0]),
        reverse=reverse_scores,
    ):
        if points:
            points = [{"date_time": points[0]["date_time"], "value": 0}] + points
        lines.append(
            {
                "player_name": player_name,
                "points": _smooth_zigzag(points),
                "latest_value": round(latest_scores[player_name], 1),
            }
        )

    return _build_trend_chart(lines, palette)


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
