from ..db import get_conn

LEADER_TREND_COLORS = ["#7dd3fc", "#2dd4bf", "#a78bfa", "#facc15", "#f472b6"]
LOSER_TREND_COLORS = ["#fb7185", "#ef4444", "#f97316", "#f59e0b", "#dc2626"]
TREND_PLAYER_LIMIT = 4


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


def _build_trend_chart(lines: list[dict], palette: list[str]) -> dict:
    width = 980
    height = 360
    pad_left = 56
    pad_right = 28
    pad_top = 24
    pad_bottom = 42
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
    rows = conn.execute(
        f"""
        WITH recent_matches AS (
            SELECT match_id, date_time
            FROM "match"
            ORDER BY date_time DESC
            LIMIT ?
        ),
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
        [match_window],
    ).fetchall()

    by_player: dict[str, list[dict]] = {}
    latest_scores: dict[str, float] = {}
    for player_name, date_time, cumulative_wl, latest_score in rows:
        by_player.setdefault(player_name, []).append({"date_time": date_time, "value": cumulative_wl})
        latest_scores[player_name] = latest_score

    lines = []
    for player_name, points in sorted(by_player.items(), key=lambda item: (-latest_scores[item[0]], item[0])):
        if points:
            points = [{"date_time": points[0]["date_time"], "value": 0}] + points
        lines.append(
            {
                "player_name": player_name,
                "points": points,
                "latest_value": round(latest_scores[player_name], 1),
            }
        )

    return _build_trend_chart(lines, palette)
