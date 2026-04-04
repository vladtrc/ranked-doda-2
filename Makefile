.PHONY: csv leaderboard main info personal players optimize sync serve lint type

# rebuild CSV exports from data.txt
csv:
	uv run python -m src.main_save

# generate HTML leaderboard report
leaderboard:
	uv run python -m src.leaderboard

# print match-by-match analysis
main:
	uv run python -m src.main

# print stats/info tables
info:
	uv run python -m src.main_info

# personal player breakdown
personal:
	uv run python -m src.leaderboard_personal

# export players summary to TSV
players:
	uv run python -m src.export_players

# run multi-objective impact coefficient optimization
optimize:
	uv run python -m src.impact_coeffs_propose

serve:
	uv run uvicorn src.app:app --reload

lint:
	uv run ruff check src/ && uv run ruff format --check src/

type:
	uv run basedpyright src/

sync:
	uv sync --all-groups
