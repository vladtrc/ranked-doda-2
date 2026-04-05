from .dashboard_trends import build_trend_chart
from ..db import get_conn

PLAYER_TREND_COLORS = ["#f8fafc", "#ef4444", "#60a5fa", "#facc15", "#2dd4bf", "#84cc16"]

VALID_SORT_COLS = {
    "name",
    "games",
    "wins",
    "win_pct",
    "wl",
    "avg_k",
    "avg_d",
    "avg_a",
    "avg_gold",
    "top_pos",
    "first_game",
    "last_game",
}
DEFAULT_SORT = ("wl", "desc")

PLAYERS_SQL = """
SELECT
    pr.player_name                                         AS name,
    count(*)                                               AS games,
    sum(CASE WHEN pr.team = m.winning_team THEN 1 ELSE 0 END) AS wins,
    cast(round(100.0 * sum(CASE WHEN pr.team = m.winning_team THEN 1 ELSE 0 END)
         / nullif(count(*), 0)) AS int)                    AS win_pct,
    sum(CASE WHEN pr.team = m.winning_team THEN 1 ELSE -1 END) AS wl,
    round(avg(pr.kills),   1)                              AS avg_k,
    round(avg(pr.deaths),  1)                              AS avg_d,
    round(avg(pr.assists), 1)                              AS avg_a,
    cast(round(avg(pr.net_worth)) AS bigint)               AS avg_gold,
    arg_max(pr.position, cnt_pos.cnt)                      AS top_pos,
    min(m.date_time)                                       AS first_game,
    max(m.date_time)                                       AS last_game
FROM player_result pr
JOIN "match" m USING (match_id)
JOIN (
    SELECT player_name, position, count(*) AS cnt
    FROM player_result
    GROUP BY player_name, position
) cnt_pos ON cnt_pos.player_name = pr.player_name AND cnt_pos.position = pr.position
{where}
GROUP BY pr.player_name
ORDER BY {sort_col} {sort_dir}, name ASC
"""


def fetch_players(q: str = "", sort_by: str = "games", sort_dir: str = "desc") -> list[dict]:
    if sort_by not in VALID_SORT_COLS:
        sort_by = DEFAULT_SORT[0]
    sort_dir = "asc" if sort_dir == "asc" else "desc"
    conn = get_conn()
    where = "WHERE lower(pr.player_name) LIKE lower(?)" if q else ""
    sql = PLAYERS_SQL.format(where=where, sort_col=sort_by, sort_dir=sort_dir)
    params = [f"%{q}%"] if q else []
    rows = conn.execute(sql, params).fetchall()
    cols = ["name", "games", "wins", "win_pct", "wl", "avg_k", "avg_d", "avg_a", "avg_gold", "top_pos", "first_game", "last_game"]
    return [dict(zip(cols, row)) for row in rows]


def fetch_player(name: str) -> dict | None:
    players = fetch_players(q=name)
    exact = [player for player in players if player["name"].lower() == name.lower()]
    return exact[0] if exact else (players[0] if len(players) == 1 else None)


def fetch_player_stats(name: str, positions: list[int] | None = None) -> dict | None:
    """Returns player stats optionally filtered by positions."""
    conn = get_conn()
    if positions:
        placeholders = ", ".join(["?" for _ in positions])
        pos_filter = f"AND pr.position IN ({placeholders})"
        params = [name, *positions]
    else:
        pos_filter = ""
        params = [name]
    rows = conn.execute(
        f"""
        SELECT
            count(*) AS games,
            sum(CASE WHEN pr.team = m.winning_team THEN 1 ELSE 0 END) AS wins,
            sum(CASE WHEN pr.team = m.winning_team THEN 1 ELSE -1 END) AS wl,
            cast(round(100.0 * sum(CASE WHEN pr.team = m.winning_team THEN 1 ELSE 0 END)
                 / nullif(count(*), 0)) AS int) AS win_pct,
            round(avg(pr.kills),   1) AS avg_k,
            round(avg(pr.deaths),  1) AS avg_d,
            round(avg(pr.assists), 1) AS avg_a,
            cast(round(avg(pr.net_worth)) AS bigint) AS avg_gold,
            min(m.date_time) AS first_game,
            max(m.date_time) AS last_game
        FROM player_result pr
        JOIN "match" m USING (match_id)
        WHERE lower(pr.player_name) = lower(?)
        {pos_filter}
        """,
        params,
    ).fetchone()
    if not rows or rows[0] == 0:
        return None
    cols = ["games", "wins", "wl", "win_pct", "avg_k", "avg_d", "avg_a", "avg_gold", "first_game", "last_game"]
    return dict(zip(cols, rows))


def fetch_player_positions(name: str) -> dict[int, int]:
    """Returns {position: game_count} for all 5 positions."""
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT pr.position, count(*) AS games
        FROM player_result pr
        WHERE lower(pr.player_name) = lower(?)
        GROUP BY pr.position
        """,
        [name],
    ).fetchall()
    counts = {pos: 0 for pos in range(1, 6)}
    for pos, cnt in rows:
        if pos in counts:
            counts[pos] = cnt
    return counts


def fetch_recent_games(name: str, limit: int, offset: int, positions: list[int] | None = None) -> list[dict]:
    conn = get_conn()
    if positions:
        placeholders = ", ".join(["?" for _ in positions])
        pos_filter = f"AND pr.position IN ({placeholders})"
        params = [name, *positions, limit, offset]
    else:
        pos_filter = ""
        params = [name, limit, offset]
    sql = f"""
    SELECT
        m.match_id,
        m.date_time,
        m.duration,
        pr.team,
        m.winning_team,
        pr.position,
        pr.kills,
        pr.deaths,
        pr.assists,
        pr.net_worth
    FROM player_result pr
    JOIN "match" m USING (match_id)
    WHERE lower(pr.player_name) = lower(?)
    {pos_filter}
    ORDER BY m.date_time DESC
    LIMIT ?
    OFFSET ?
    """
    rows = conn.execute(sql, params).fetchall()
    cols = ["match_id", "date_time", "duration", "team", "winning_team", "position", "kills", "deaths", "assists", "net_worth"]
    result = []
    for row in rows:
        game = dict(zip(cols, row))
        game["won"] = game["team"] == game["winning_team"]
        result.append(game)
    return result


def fetch_player_trend(name: str, positions: list[int] | None = None) -> dict:
    conn = get_conn()
    if positions:
        placeholders = ", ".join(["?" for _ in positions])
        pos_filter = f"AND pr.position IN ({placeholders})"
        params = [name, *positions]
    else:
        pos_filter = ""
        params = [name]

    rows = conn.execute(
        f"""
        WITH player_matches AS (
            SELECT
                m.date_time,
                pr.position,
                row_number() OVER (ORDER BY m.date_time ASC, m.match_id ASC) AS match_idx,
                CASE WHEN pr.team = m.winning_team THEN 1 ELSE -1 END AS wl_delta
            FROM player_result pr
            JOIN "match" m USING (match_id)
            WHERE lower(pr.player_name) = lower(?)
            {pos_filter}
        ),
        selected_positions AS (
            SELECT DISTINCT position
            FROM player_matches
        ),
        filled AS (
            SELECT
                pm.match_idx,
                pm.date_time,
                sp.position,
                sum(CASE WHEN pm.position = sp.position THEN pm.wl_delta ELSE 0 END) OVER (
                    PARTITION BY sp.position
                    ORDER BY pm.match_idx
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cumulative_wl
            FROM player_matches pm
            CROSS JOIN selected_positions sp
        )
        SELECT position, date_time, match_idx, cumulative_wl
        FROM filled
        ORDER BY match_idx ASC, position ASC
        """,
        params,
    ).fetchall()

    if not rows:
        return build_trend_chart([], PLAYER_TREND_COLORS, 0)

    by_position: dict[int, list[dict]] = {}
    latest_values: dict[int, int] = {}
    total_matches = 0
    first_date = rows[0][1]
    overall_by_match: dict[int, dict] = {}
    for position, date_time, match_idx, cumulative_wl in rows:
        by_position.setdefault(position, []).append(
            {"match_idx": match_idx, "date_time": date_time, "value": cumulative_wl}
        )
        latest_values[position] = cumulative_wl
        total_matches = max(total_matches, match_idx)
        overall_by_match[match_idx] = {"match_idx": match_idx, "date_time": date_time}

    overall_points = []
    overall_latest = 0
    for match_idx in sorted(overall_by_match):
        total_value = sum(points[match_idx - 1]["value"] for points in by_position.values())
        overall_points.append({**overall_by_match[match_idx], "value": total_value})
        overall_latest = total_value

    return build_trend_chart(
        [
            {
                "player_name": "Overall",
                "points": [{"match_idx": 0, "date_time": first_date, "value": 0}, *overall_points],
                "latest_value": overall_latest,
            },
            *sorted(
                [
                    {
                        "player_name": f"POS {position}",
                        "points": [{"match_idx": 0, "date_time": first_date, "value": 0}, *points],
                        "latest_value": latest_values[position],
                    }
                    for position, points in by_position.items()
                ],
                key=lambda line: line["player_name"],
            ),
        ],
        PLAYER_TREND_COLORS,
        total_matches,
    )
