from __future__ import annotations

import argparse
import csv
import math
from dataclasses import dataclass
from pathlib import Path

from src.ranked.common import ensure_out_dir, load_matches


REG_STRENGTH = 1.0
LEARNING_RATE = 0.05
EPOCHS = 6000


@dataclass
class PlayerSummary:
    games: int = 0
    wins: int = 0
    losses: int = 0

    @property
    def win_pct(self) -> float:
        if self.games == 0:
            return 0.0
        return 100.0 * self.wins / self.games


def _sigmoid(x: float) -> float:
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def _write_rows(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()), delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)


def run(out_dir: str | Path = "artifacts/ranked") -> tuple[Path, Path, Path]:
    matches = load_matches()
    out = ensure_out_dir(out_dir)

    names = sorted({player.player_name for match in matches for player in match.players})
    index_by_name = {name: idx for idx, name in enumerate(names)}
    theta = [0.0 for _ in names]
    radiant_bias = 0.0
    summary = {name: PlayerSummary() for name in names}

    encoded_matches: list[tuple[list[int], list[int], float]] = []
    for match in matches:
        radiant_idx = [index_by_name[p.player_name] for p in match.players if p.team == "radiant"]
        dire_idx = [index_by_name[p.player_name] for p in match.players if p.team == "dire"]
        y = 1.0 if match.winning_team == "radiant" else 0.0
        encoded_matches.append((radiant_idx, dire_idx, y))

        for idx in radiant_idx:
            s = summary[names[idx]]
            s.games += 1
            if y == 1.0:
                s.wins += 1
            else:
                s.losses += 1
        for idx in dire_idx:
            s = summary[names[idx]]
            s.games += 1
            if y == 0.0:
                s.wins += 1
            else:
                s.losses += 1

    for _ in range(EPOCHS):
        grad = [-REG_STRENGTH * value for value in theta]
        bias_grad = 0.0

        for radiant_idx, dire_idx, y in encoded_matches:
            logit = radiant_bias
            logit += sum(theta[idx] for idx in radiant_idx)
            logit -= sum(theta[idx] for idx in dire_idx)
            p = _sigmoid(logit)
            err = y - p
            bias_grad += err
            for idx in radiant_idx:
                grad[idx] += err
            for idx in dire_idx:
                grad[idx] -= err

        scale = LEARNING_RATE / len(encoded_matches)
        for idx in range(len(theta)):
            theta[idx] += scale * grad[idx]

        mean_theta = sum(theta) / len(theta)
        for idx in range(len(theta)):
            theta[idx] -= mean_theta

        radiant_bias += scale * bias_grad

    diag_h = [REG_STRENGTH for _ in names]
    match_rows: list[dict[str, object]] = []
    correct = 0
    log_loss = 0.0
    for match, (radiant_idx, dire_idx, y) in zip(matches, encoded_matches, strict=True):
        logit = radiant_bias
        logit += sum(theta[idx] for idx in radiant_idx)
        logit -= sum(theta[idx] for idx in dire_idx)
        p = _sigmoid(logit)
        weight = p * (1.0 - p)
        for idx in radiant_idx:
            diag_h[idx] += weight
        for idx in dire_idx:
            diag_h[idx] += weight
        correct += int((p >= 0.5) == (y == 1.0))
        log_loss += -(y * math.log(max(p, 1e-12)) + (1.0 - y) * math.log(max(1.0 - p, 1e-12)))
        match_rows.append(
            {
                "date_time": match.date_time.isoformat(sep=" "),
                "winner": match.winning_team,
                "pred_radiant_win_prob": round(p, 6),
                "lineup_logit": round(logit, 6),
            }
        )

    leaderboard_rows = []
    for idx, name in sorted(
        enumerate(names),
        key=lambda item: (-(theta[item[0]] - 2.0 / math.sqrt(diag_h[item[0]])), -theta[item[0]], item[1]),
    ):
        std_err = 1.0 / math.sqrt(diag_h[idx])
        effect = theta[idx]
        conservative = effect - 2.0 * std_err
        leaderboard_rows.append(
            {
                "player_name": name,
                "games": summary[name].games,
                "wins": summary[name].wins,
                "losses": summary[name].losses,
                "win_pct": round(summary[name].win_pct, 2),
                "bt_score": round(effect, 6),
                "std_err": round(std_err, 6),
                "bt_conservative": round(conservative, 6),
                "odds_multiplier_vs_avg": round(math.exp(effect), 6),
                "win_prob_vs_avg": round(_sigmoid(effect), 6),
            }
        )

    leaderboard_path = out / "bradley_terry_leaderboard.tsv"
    matches_path = out / "bradley_terry_matches.tsv"
    _write_rows(leaderboard_path, leaderboard_rows)
    _write_rows(matches_path, match_rows)

    avg_log_loss = log_loss / len(encoded_matches)
    accuracy = correct / len(encoded_matches)
    summary_path = out / "bradley_terry_summary.txt"
    summary_path.write_text(
        "\n".join(
            [
                "Bradley-Terry lineup model",
                f"matches={len(encoded_matches)}",
                f"players={len(names)}",
                f"radiant_bias={radiant_bias:.6f}",
                f"train_accuracy={accuracy:.6f}",
                f"train_log_loss={avg_log_loss:.6f}",
                "bt_score is the player log-odds contribution relative to an average player.",
                "bt_conservative = bt_score - 2*std_err",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return leaderboard_path, matches_path, summary_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute a Bradley-Terry lineup model.")
    parser.add_argument("--out-dir", default="artifacts/ranked", help="Directory for outputs.")
    args = parser.parse_args()
    leaderboard_path, matches_path, summary_path = run(args.out_dir)
    print(f"wrote {leaderboard_path}")
    print(f"wrote {matches_path}")
    print(f"wrote {summary_path}")


if __name__ == "__main__":
    main()
