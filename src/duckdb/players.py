from ..db import get_conn

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
    cols = ["name", "games", "wins", "win_pct", "wl", "avg_k", "avg_d", "avg_a", "avg_gold", "top_pos", "last_game"]
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
            cast(round(100.0 * sum(CASE WHEN pr.team = m.winning_team THEN 1 ELSE 0 END)
                 / nullif(count(*), 0)) AS int) AS win_pct,
            round(avg(pr.kills),   1) AS avg_k,
            round(avg(pr.deaths),  1) AS avg_d,
            round(avg(pr.assists), 1) AS avg_a,
            cast(round(avg(pr.net_worth)) AS bigint) AS avg_gold
        FROM player_result pr
        JOIN "match" m USING (match_id)
        WHERE lower(pr.player_name) = lower(?)
        {pos_filter}
        """,
        params,
    ).fetchone()
    if not rows or rows[0] == 0:
        return None
    cols = ["games", "wins", "win_pct", "avg_k", "avg_d", "avg_a", "avg_gold"]
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
