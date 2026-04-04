# Ranked Analysis

This folder contains two offline ranking models for the current `data.txt` schema.

## `trueskill_like.py`

Product-style leaderboard.

- Input: only lineups and match winners.
- State per player: `mu`, `sigma`.
- Public leaderboard field: `rank_score = mu - 3*sigma`.
- Why this is the main leaderboard candidate:
  it shrinks players with small sample sizes instead of overrating short hot streaks.

Outputs:

- `artifacts/ranked/trueskill_leaderboard.tsv`
- `artifacts/ranked/trueskill_matches.tsv`

## `bradley_terry.py`

Research-style lineup model.

- Input: lineups and match winners.
- Model: `P(radiant win) = sigmoid(radiant_bias + sum(theta_radiant) - sum(theta_dire))`
- `bt_score` is a player's contribution in log-odds units relative to an average player.
- `bt_conservative = bt_score - 2*std_err`.

Notes:

- No global `role_effects` are included. In this dataset both teams always contain positions `1..5`, so a shared role intercept is not identifiable.
- This model is interpretable, but it still inherits teammate-strength confounding because there are no substitutions or play-by-play segments.

Outputs:

- `artifacts/ranked/bradley_terry_leaderboard.tsv`
- `artifacts/ranked/bradley_terry_matches.tsv`
- `artifacts/ranked/bradley_terry_summary.txt`
