from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.gzip import GZipMiddleware

from .db import init_db
from .duckdb import (
    fetch_dashboard_lane_stats,
    fetch_dashboard_trends,
    fetch_game,
    fetch_games,
    fetch_player,
    fetch_players,
    fetch_recent_games,
)
from .duckdb.players import DEFAULT_SORT, fetch_player_positions, fetch_player_stats

TEMPLATES_DIR = Path(__file__).parent / "templates"
STATIC_DIR = Path(__file__).parent / "static"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
templates.env.filters["nw"] = lambda v: f"{v:,}".replace(",", "\u202f") if v else "—"


_PLAYER_GAMES_PAGE_SIZE = 20


_PAGE_SIZE = 20
_DEFAULT_DASHBOARD_WINDOW = "50"
_DASHBOARD_WINDOWS: list[dict[str, str | int | None]] = [
    {"key": "15", "matches": 15, "label": "15 matches"},
    {"key": "50", "matches": 50, "label": "50 matches"},
    {"key": "100", "matches": 100, "label": "100 matches"},
    {"key": "all", "matches": None, "label": "All time"},
]
_DASHBOARD_WINDOW_MAP = {item["key"]: item["matches"] for item in _DASHBOARD_WINDOWS}


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
    games = fetch_games(limit=_PAGE_SIZE, offset=0)
    return templates.TemplateResponse(
        request, "games.html", {"games": games, "offset": _PAGE_SIZE, "active_page": "games"}
    )


@app.get("/api/games", response_class=HTMLResponse)
def games_partial(request: Request, offset: int = Query(default=0)):
    games = fetch_games(limit=_PAGE_SIZE, offset=offset)
    next_offset = offset + _PAGE_SIZE
    return templates.TemplateResponse(
        request, "partials/game_cards.html", {"games": games, "offset": next_offset}
    )


@app.get("/players", response_class=HTMLResponse)
def players_page(request: Request):
    sort_by, sort_dir = DEFAULT_SORT
    players = fetch_players(sort_by=sort_by, sort_dir=sort_dir)
    players_json = [
        {
            **player,
            "last_game": player["last_game"].strftime("%Y-%m-%d") if player["last_game"] else None,
        }
        for player in players
    ]
    return templates.TemplateResponse(
        request,
        "players.html",
        {
            "players": players,
            "players_json": players_json,
            "sort_by": sort_by,
            "sort_dir": sort_dir,
            "active_page": "players",
        },
    )


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard_page(request: Request, window: str = Query(default=_DEFAULT_DASHBOARD_WINDOW)):
    if window not in _DASHBOARD_WINDOW_MAP:
        window = _DEFAULT_DASHBOARD_WINDOW
    match_window = _DASHBOARD_WINDOW_MAP[window]
    leader_chart = fetch_dashboard_trends(match_window=match_window, direction="desc")
    loser_chart = fetch_dashboard_trends(match_window=match_window, direction="asc")
    lane_tables = fetch_dashboard_lane_stats(match_window=match_window)
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "active_page": "dashboard",
            "window": window,
            "dashboard_windows": _DASHBOARD_WINDOWS,
            "window_label": "All time" if match_window is None else f"Last {match_window} matches",
            "leader_chart": leader_chart,
            "loser_chart": loser_chart,
            "lane_tables": lane_tables,
        },
    )


@app.get("/api/suggest")
def player_suggest(q: str = Query(default="")):
    if not q:
        return JSONResponse([])
    players = fetch_players(q, sort_by="games", sort_dir="desc")
    return JSONResponse([p["name"] for p in players[:8]])


@app.get("/api/game/{match_id}", response_class=HTMLResponse)
def game_card(request: Request, match_id: str):
    game = fetch_game(match_id)
    if not game:
        return HTMLResponse("<p>Game not found.</p>", status_code=404)
    return templates.TemplateResponse(request, "partials/game_cards.html", {"games": [game], "offset": None})


def _parse_positions(positions: str) -> list[int]:
    return [int(p) for p in positions.split(",") if p.strip().isdigit() and 1 <= int(p) <= 5]


@app.get("/api/player/{name}/games", response_class=HTMLResponse)
def player_games_partial(request: Request, name: str, offset: int = Query(default=0), positions: str = Query(default="")):
    selected = _parse_positions(positions)
    recent_games = fetch_recent_games(name, limit=_PLAYER_GAMES_PAGE_SIZE, offset=offset, positions=selected or None)
    next_offset = offset + _PLAYER_GAMES_PAGE_SIZE
    return templates.TemplateResponse(
        request,
        "partials/player_recent_games_rows.html",
        {"recent_games": recent_games, "player_name": name, "offset": next_offset, "positions": positions},
    )


@app.get("/player", response_class=RedirectResponse)
def player_search(name: str = Query(default="")):
    return RedirectResponse(url=f"/player/{name}")


@app.get("/player/{name}", response_class=HTMLResponse)
def player_profile(request: Request, name: str, positions: str = Query(default="")):
    selected = _parse_positions(positions)
    player = fetch_player(name)
    position_counts = fetch_player_positions(name) if player else {pos: 0 for pos in range(1, 6)}
    stats = fetch_player_stats(name, selected or None) if player else None
    recent_games = fetch_recent_games(name, limit=_PLAYER_GAMES_PAGE_SIZE, offset=0, positions=selected or None) if player else []
    return templates.TemplateResponse(
        request,
        "player.html",
        {
            "player": player,
            "stats": stats,
            "recent_games": recent_games,
            "player_name": name,
            "offset": _PLAYER_GAMES_PAGE_SIZE,
            "active_page": None,
            "positions": positions,
            "selected_positions": selected,
            "position_counts": position_counts,
        },
    )
