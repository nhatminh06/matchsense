"""Generates synthetic match-state snapshots for win-probability training.

Each row is an independently simulated match sampled at one random minute
-- there is no shared match context between rows -- so a random row-level
train/test split (used in train_win_prob.py) is equivalent to a match-level
split and does not leak information between rows.
"""

import numpy as np
import pandas as pd

from paths import WIN_PROB_DATA_PATH, ensure_dirs

DEFAULT_N_MATCHES = 5000
DEFAULT_SEED = 42


def generate_dataset(n_matches: int = DEFAULT_N_MATCHES, seed: int = DEFAULT_SEED) -> pd.DataFrame:
    """Generates n_matches independent synthetic match-state snapshots.
    Deterministic: the same (n_matches, seed) always produces an identical
    DataFrame."""
    rng = np.random.default_rng(seed)
    data = []

    for _ in range(n_matches):
        minute = int(rng.integers(1, 91))

        # Score so far (Poisson, ~1.5 home / ~1.3 away goals per full match)
        time_factor = minute / 90.0
        home_goals = int(rng.poisson(1.5 * time_factor))
        away_goals = int(rng.poisson(1.3 * time_factor))
        goal_diff = home_goals - away_goals

        home_shots = int(rng.poisson(7 * time_factor))
        away_shots = int(rng.poisson(6 * time_factor))
        home_shots_ot = int(min(home_shots, rng.poisson(3 * time_factor)))
        away_shots_ot = int(min(away_shots, rng.poisson(2.5 * time_factor)))
        home_corners = int(rng.poisson(3 * time_factor))
        away_corners = int(rng.poisson(2.5 * time_factor))
        home_fouls = int(rng.poisson(6 * time_factor))
        away_fouls = int(rng.poisson(6 * time_factor))
        home_yellow = int(rng.poisson(0.8 * time_factor))
        away_yellow = int(rng.poisson(0.8 * time_factor))
        home_red = 1 if rng.random() < 0.02 * time_factor else 0
        away_red = 1 if rng.random() < 0.02 * time_factor else 0

        # Project the remaining time to a final result
        remaining_factor = (90 - minute) / 90.0
        extra_home = int(rng.poisson(1.5 * remaining_factor))
        extra_away = int(rng.poisson(1.3 * remaining_factor))

        if home_red > 0:
            extra_home = int(extra_home * 0.6)
            extra_away = int(extra_away * 1.3)
        if away_red > 0:
            extra_away = int(extra_away * 0.6)
            extra_home = int(extra_home * 1.3)

        final_home = home_goals + extra_home
        final_away = away_goals + extra_away

        if final_home > final_away:
            result = "home_win"
        elif final_away > final_home:
            result = "away_win"
        else:
            result = "draw"

        data.append({
            "minute": minute,
            "home_goals": home_goals,
            "away_goals": away_goals,
            "goal_diff": goal_diff,
            "home_shots": home_shots,
            "away_shots": away_shots,
            "home_shots_on_target": home_shots_ot,
            "away_shots_on_target": away_shots_ot,
            "shot_diff": home_shots - away_shots,
            "home_corners": home_corners,
            "away_corners": away_corners,
            "home_fouls": home_fouls,
            "away_fouls": away_fouls,
            "home_yellow": home_yellow,
            "away_yellow": away_yellow,
            "home_red": home_red,
            "away_red": away_red,
            "result": result,
        })

    return pd.DataFrame(data)


def main() -> None:
    ensure_dirs()
    df = generate_dataset()
    df.to_csv(WIN_PROB_DATA_PATH, index=False)

    print(f"Generated {len(df)} match states -> {WIN_PROB_DATA_PATH}")
    print("\nResult distribution:")
    print(df["result"].value_counts(normalize=True))


if __name__ == "__main__":
    main()
