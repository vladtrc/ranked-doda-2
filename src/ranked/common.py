from __future__ import annotations

from collections import Counter
from pathlib import Path

from src.data_parse import Match, parse_dota_file


DEFAULT_OUT_DIR = Path("artifacts/ranked")


def load_matches(filename: str = "data.txt") -> list[Match]:
    matches = parse_dota_file(filename)
    return sorted(matches, key=lambda m: m.date_time)


def build_game_counts(matches: list[Match]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for match in matches:
        for player in match.players:
            counts[player.player_name] += 1
    return counts


def ensure_out_dir(path: str | Path = DEFAULT_OUT_DIR) -> Path:
    out_dir = Path(path)
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir
