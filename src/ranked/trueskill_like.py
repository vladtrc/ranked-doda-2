from __future__ import annotations

import argparse
import csv
import math
from dataclasses import dataclass
from pathlib import Path

from src.ranked.common import ensure_out_dir, load_matches


INITIAL_MU = 25.0
INITIAL_SIGMA = INITIAL_MU / 3.0
BETA = INITIAL_MU / 6.0
TAU = INITIAL_MU / 300.0
MIN_SIGMA = 1e-3


@dataclass
class PlayerRating:
    mu: float = INITIAL_MU
    sigma: float = INITIAL_SIGMA
    games: int = 0
    wins: int = 0
    losses: int = 0

    @property
    def conservative_2sigma(self) -> float:
        return self.mu - 2.0 * self.sigma

    @property
    def conservative_3sigma(self) -> float:
        return self.mu - 3.0 * self.sigma

    @property
    def win_pct(self) -> float:
        if self.games == 0:
            return 0.0
        return 100.0 * self.wins / self.games


def _normal_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def _normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _v_func(delta: float) -> float:
    cdf = max(_normal_cdf(delta), 1e-12)
    return _normal_pdf(delta) / cdf


def _w_func(delta: float) -> float:
    v = _v_func(delta)
    return v * (v + delta)


def _write_rows(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()), delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)


def run(out_dir: str | Path = "artifacts/ranked") -> tuple[Path, Path]:
    matches = load_matches()
    out = ensure_out_dir(out_dir)
    ratings: dict[str, PlayerRating] = {}
    match_rows: list[dict[str, object]] = []

    for match in matches:
        for player in match.players:
            rating = ratings.setdefault(player.player_name, PlayerRating())
            rating.sigma = math.sqrt(rating.sigma * rating.sigma + TAU * TAU)

        radiant = [p.player_name for p in match.players if p.team == "radiant"]
        dire = [p.player_name for p in match.players if p.team == "dire"]
        radiant_win = match.winning_team == "radiant"
        winner_names = radiant if radiant_win else dire
        loser_names = dire if radiant_win else radiant

        winner_mean = sum(ratings[name].mu for name in winner_names)
        loser_mean = sum(ratings[name].mu for name in loser_names)
        winner_var = sum(ratings[name].sigma ** 2 for name in winner_names)
        loser_var = sum(ratings[name].sigma ** 2 for name in loser_names)

        c = math.sqrt(winner_var + loser_var + 2.0 * BETA * BETA)
        delta = (winner_mean - loser_mean) / c
        v = _v_func(delta)
        w = _w_func(delta)

        for name in winner_names:
            rating = ratings[name]
            sigma_sq = rating.sigma ** 2
            rating.mu += (sigma_sq / c) * v
            new_sigma_sq = sigma_sq * max(1e-9, 1.0 - (sigma_sq / (c * c)) * w)
            rating.sigma = max(math.sqrt(new_sigma_sq), MIN_SIGMA)
            rating.games += 1
            rating.wins += 1

        for name in loser_names:
            rating = ratings[name]
            sigma_sq = rating.sigma ** 2
            rating.mu -= (sigma_sq / c) * v
            new_sigma_sq = sigma_sq * max(1e-9, 1.0 - (sigma_sq / (c * c)) * w)
            rating.sigma = max(math.sqrt(new_sigma_sq), MIN_SIGMA)
            rating.games += 1
            rating.losses += 1

        radiant_strength = sum(ratings[name].mu for name in radiant)
        dire_strength = sum(ratings[name].mu for name in dire)
        prob_radiant = 1.0 / (1.0 + math.exp(-(radiant_strength - dire_strength) / (5.0 * BETA)))
        match_rows.append(
            {
                "date_time": match.date_time.isoformat(sep=" "),
                "winner": match.winning_team,
                "radiant_mu_sum": round(radiant_strength, 4),
                "dire_mu_sum": round(dire_strength, 4),
                "predicted_radiant_win_prob_after_update": round(prob_radiant, 6),
            }
        )

    leaderboard_rows = []
    for name, rating in sorted(
        ratings.items(),
        key=lambda item: (-item[1].conservative_3sigma, -item[1].mu, item[0]),
    ):
        leaderboard_rows.append(
            {
                "player_name": name,
                "games": rating.games,
                "wins": rating.wins,
                "losses": rating.losses,
                "win_pct": round(rating.win_pct, 2),
                "mu": round(rating.mu, 4),
                "sigma": round(rating.sigma, 4),
                "rank_score": round(rating.conservative_3sigma, 4),
                "rank_score_2sigma": round(rating.conservative_2sigma, 4),
            }
        )

    leaderboard_path = out / "trueskill_leaderboard.tsv"
    matches_path = out / "trueskill_matches.tsv"
    _write_rows(leaderboard_path, leaderboard_rows)
    _write_rows(matches_path, match_rows)
    return leaderboard_path, matches_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute a TrueSkill-like leaderboard.")
    parser.add_argument("--out-dir", default="artifacts/ranked", help="Directory for TSV outputs.")
    args = parser.parse_args()
    leaderboard_path, matches_path = run(args.out_dir)
    print(f"wrote {leaderboard_path}")
    print(f"wrote {matches_path}")
    print("rank_score = mu - 3*sigma")


if __name__ == "__main__":
    main()
