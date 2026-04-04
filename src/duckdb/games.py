from collections import defaultdict

from ..db import get_conn


def fetch_game(match_id: str) -> dict | None:
    conn = get_conn()
    row = conn.execute(
        'SELECT match_id, date_time, duration, radiant_kills, dire_kills, winning_team FROM "match" WHERE match_id = ?',
        [match_id],
    ).fetchone()
    if not row:
        return None
    cols = ["match_id", "date_time", "duration", "radiant_kills", "dire_kills", "winning_team"]
    game = dict(zip(cols, row))
    player_rows = conn.execute(
        "SELECT match_id, player_name, team, position, kills, deaths, assists, net_worth FROM player_result WHERE match_id = ? ORDER BY team, position",
        [match_id],
    ).fetchall()
    player_cols = ["match_id", "player_name", "team", "position", "kills", "deaths", "assists", "net_worth"]
    sides: dict[str, list] = defaultdict(list)
    for r in player_rows:
        player = dict(zip(player_cols, r))
        sides[player["team"]].append(player)
    game["radiant_players"] = sides["radiant"]
    game["dire_players"] = sides["dire"]
    return game


def fetch_games(limit: int, offset: int) -> list[dict]:
    conn = get_conn()
    match_sql = """
    SELECT match_id, date_time, duration, radiant_kills, dire_kills, winning_team
    FROM "match"
    ORDER BY date_time DESC
    LIMIT ? OFFSET ?
    """
    match_rows = conn.execute(match_sql, [limit, offset]).fetchall()
    match_cols = ["match_id", "date_time", "duration", "radiant_kills", "dire_kills", "winning_team"]
    games = [dict(zip(match_cols, row)) for row in match_rows]

    if not games:
        return games

    ids = [game["match_id"] for game in games]
    placeholders = ", ".join("?" * len(ids))
    player_sql = f"""
    SELECT match_id, player_name, team, position, kills, deaths, assists, net_worth
    FROM player_result
    WHERE match_id IN ({placeholders})
    ORDER BY team, position
    """
    player_rows = conn.execute(player_sql, ids).fetchall()
    player_cols = ["match_id", "player_name", "team", "position", "kills", "deaths", "assists", "net_worth"]

    players_by_match: dict[str, dict[str, list]] = defaultdict(lambda: {"radiant": [], "dire": []})
    for row in player_rows:
        player = dict(zip(player_cols, row))
        players_by_match[player["match_id"]][player["team"]].append(player)

    for game in games:
        game["radiant_players"] = players_by_match[game["match_id"]]["radiant"]
        game["dire_players"] = players_by_match[game["match_id"]]["dire"]

    return games
