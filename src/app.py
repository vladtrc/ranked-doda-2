from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.gzip import GZipMiddleware

from .db import init_db, get_conn

TEMPLATES_DIR = Path(__file__).parent / "templates"
STATIC_DIR = Path(__file__).parent / "static"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
templates.env.filters["nw"] = lambda v: f"{v:,}".replace(",", "\u202f") if v else "—"

_VALID_SORT_COLS = {"name", "games", "wins", "win_pct", "wl", "avg_k", "avg_d", "avg_a", "avg_gold", "top_pos", "last_game"}
_DEFAULT_SORT = ("wl", "desc")

_PLAYERS_SQL = """
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


def _fetch_players(q: str = "", sort_by: str = "games", sort_dir: str = "desc") -> list[dict]:
    if sort_by not in _VALID_SORT_COLS:
        sort_by = _DEFAULT_SORT[0]
    sort_dir = "asc" if sort_dir == "asc" else "desc"
    conn = get_conn()
    where = "WHERE lower(pr.player_name) LIKE lower(?)" if q else ""
    sql = _PLAYERS_SQL.format(where=where, sort_col=sort_by, sort_dir=sort_dir)
    params = [f"%{q}%"] if q else []
    rows = conn.execute(sql, params).fetchall()
    cols = ["name", "games", "wins", "win_pct", "wl", "avg_k", "avg_d", "avg_a", "avg_gold", "top_pos", "last_game"]
    return [dict(zip(cols, r)) for r in rows]


def _fetch_player(name: str) -> dict | None:
    players = _fetch_players(q=name)
    exact = [p for p in players if p["name"].lower() == name.lower()]
    return exact[0] if exact else (players[0] if len(players) == 1 else None)


_PLAYER_GAMES_PAGE_SIZE = 20


def _fetch_recent_games(name: str, limit: int = _PLAYER_GAMES_PAGE_SIZE, offset: int = 0) -> list[dict]:
    conn = get_conn()
    sql = """
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
    ORDER BY m.date_time DESC
    LIMIT ?
    OFFSET ?
    """
    rows = conn.execute(sql, [name, limit, offset]).fetchall()
    cols = ["match_id", "date_time", "duration", "team", "winning_team", "position", "kills", "deaths", "assists", "net_worth"]
    result = []
    for r in rows:
        d = dict(zip(cols, r))
        d["won"] = d["team"] == d["winning_team"]
        result.append(d)
    return result


def _fetch_game(match_id: str) -> dict | None:
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
    from collections import defaultdict
    sides: dict[str, list] = defaultdict(list)
    for r in player_rows:
        p = dict(zip(player_cols, r))
        sides[p["team"]].append(p)
    game["radiant_players"] = sides["radiant"]
    game["dire_players"] = sides["dire"]
    return game


_PAGE_SIZE = 20


def _fetch_games(limit: int = _PAGE_SIZE, offset: int = 0) -> list[dict]:
    conn = get_conn()
    match_sql = """
    SELECT match_id, date_time, duration, radiant_kills, dire_kills, winning_team
    FROM "match"
    ORDER BY date_time DESC
    LIMIT ? OFFSET ?
    """
    match_rows = conn.execute(match_sql, [limit, offset]).fetchall()
    match_cols = ["match_id", "date_time", "duration", "radiant_kills", "dire_kills", "winning_team"]
    games = [dict(zip(match_cols, r)) for r in match_rows]

    if not games:
        return games

    ids = [g["match_id"] for g in games]
    placeholders = ", ".join("?" * len(ids))
    player_sql = f"""
    SELECT match_id, player_name, team, position, kills, deaths, assists, net_worth
    FROM player_result
    WHERE match_id IN ({placeholders})
    ORDER BY team, position
    """
    player_rows = conn.execute(player_sql, ids).fetchall()
    player_cols = ["match_id", "player_name", "team", "position", "kills", "deaths", "assists", "net_worth"]

    from collections import defaultdict
    players_by_match: dict[str, dict[str, list]] = defaultdict(lambda: {"radiant": [], "dire": []})
    for r in player_rows:
        p = dict(zip(player_cols, r))
        players_by_match[p["match_id"]][p["team"]].append(p)

    for g in games:
        g["radiant_players"] = players_by_match[g["match_id"]]["radiant"]
        g["dire_players"] = players_by_match[g["match_id"]]["dire"]

    return games


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db("data.txt")
    yield


app = FastAPI(lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=500, compresslevel=6)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/", response_class=RedirectResponse)
def index():
    return RedirectResponse(url="/games")


@app.get("/games", response_class=HTMLResponse)
def games_page(request: Request):
    games = _fetch_games(offset=0)
    return templates.TemplateResponse(
        request, "games.html", {"games": games, "offset": _PAGE_SIZE, "active_page": "games"}
    )


@app.get("/api/games", response_class=HTMLResponse)
def games_partial(request: Request, offset: int = Query(default=0)):
    games = _fetch_games(offset=offset)
    next_offset = offset + _PAGE_SIZE
    return templates.TemplateResponse(
        request, "partials/game_cards.html", {"games": games, "offset": next_offset}
    )


@app.get("/players", response_class=HTMLResponse)
def players_page(request: Request):
    sort_by, sort_dir = _DEFAULT_SORT
    players = _fetch_players(sort_by=sort_by, sort_dir=sort_dir)
    return templates.TemplateResponse(
        request,
        "players.html",
        {"players": players, "sort_by": sort_by, "sort_dir": sort_dir, "active_page": "players"},
    )


@app.get("/api/suggest")
def player_suggest(q: str = Query(default="")):
    if not q:
        return JSONResponse([])
    players = _fetch_players(q, sort_by="games", sort_dir="desc")
    return JSONResponse([p["name"] for p in players[:8]])


@app.get("/api/players", response_class=HTMLResponse)
def players_partial(
    request: Request,
    q: str = Query(default=""),
    sort_by: str = Query(default="games"),
    sort_dir: str = Query(default="desc"),
):
    players = _fetch_players(q, sort_by, sort_dir)
    return templates.TemplateResponse(
        request, "partials/players_rows.html", {"players": players}
    )


@app.get("/api/game/{match_id}", response_class=HTMLResponse)
def game_card(request: Request, match_id: str):
    game = _fetch_game(match_id)
    if not game:
        return HTMLResponse("<p>Game not found.</p>", status_code=404)
    return templates.TemplateResponse(request, "partials/game_cards.html", {"games": [game], "offset": None})


@app.get("/api/player/{name}/games", response_class=HTMLResponse)
def player_games_partial(request: Request, name: str, offset: int = Query(default=0)):
    recent_games = _fetch_recent_games(name, offset=offset)
    next_offset = offset + _PLAYER_GAMES_PAGE_SIZE
    return templates.TemplateResponse(
        request,
        "partials/player_recent_games_rows.html",
        {"recent_games": recent_games, "player_name": name, "offset": next_offset},
    )


@app.get("/player", response_class=RedirectResponse)
def player_search(name: str = Query(default="")):
    return RedirectResponse(url=f"/player/{name}")


@app.get("/player/{name}", response_class=HTMLResponse)
def player_profile(request: Request, name: str):
    player = _fetch_player(name)
    recent_games = _fetch_recent_games(name) if player else []
    return templates.TemplateResponse(
        request,
        "player.html",
        {
            "player": player,
            "recent_games": recent_games,
            "player_name": name,
            "offset": _PLAYER_GAMES_PAGE_SIZE,
            "active_page": None,
        },
    )
