"""Generates synthetic shot data for xG model training.

Every shot is independently simulated -- there is no "match" grouping in
this dataset -- so a random row-level train/test split (used in
train_xg.py) does not leak information between rows the way it could if
multiple rows shared match context.
"""

import math

import numpy as np
import pandas as pd

from paths import XG_DATA_PATH, ensure_dirs

DEFAULT_N_SAMPLES = 10000
DEFAULT_SEED = 42


def goal_probability(rng: np.random.Generator, x: float, y: float, shot_type: str) -> float:
    """Realistic xG based on distance and angle to goal.
    Goal is at x=100, y=50 (center of goal line)."""
    dist = math.sqrt((100 - x) ** 2 + (50 - y) ** 2)

    goal_width = 7.32  # meters
    scaled_width = goal_width * (100 / 105)
    angle = math.atan2(scaled_width, dist * (105 / 100))

    base_prob = 0.5 * math.exp(-dist / 25) * (angle / (math.pi / 2))

    if shot_type == "header":
        base_prob *= 0.7
    elif shot_type == "freekick":
        base_prob *= 0.4
    elif shot_type == "penalty":
        base_prob = 0.76

    noise = rng.normal(0, 0.03)
    return float(np.clip(base_prob + noise, 0.01, 0.95))


def generate_dataset(n_samples: int = DEFAULT_N_SAMPLES, seed: int = DEFAULT_SEED) -> pd.DataFrame:
    """Generates n_samples independent synthetic shots. Deterministic: the
    same (n_samples, seed) always produces an identical DataFrame, since
    all randomness is drawn from a Generator seeded here rather than the
    global numpy random state."""
    rng = np.random.default_rng(seed)
    data = []

    for _ in range(n_samples):
        # Shot location: mostly in attacking third
        x = rng.beta(5, 2) * 40 + 60
        y = float(np.clip(rng.normal(50, 20), 5, 95))

        distance = math.sqrt((100 - x) ** 2 + (50 - y) ** 2)
        angle = math.degrees(math.atan2(7.0, distance * (105 / 100)))

        shot_type = rng.choice(
            ["foot", "header", "freekick", "penalty"],
            p=[0.70, 0.15, 0.10, 0.05],
        )

        defenders = max(0, int(rng.normal(3 - distance / 15, 1.5)))

        if shot_type == "foot":
            is_strong_foot = 1 if rng.random() < 0.75 else 0
        else:
            is_strong_foot = 0

        true_prob = goal_probability(rng, x, y, shot_type)
        true_prob *= max(0.2, 1 - defenders * 0.08)  # defender effect

        is_goal = 1 if rng.random() < true_prob else 0

        data.append({
            "x": round(x, 2),
            "y": round(y, 2),
            "distance": round(distance, 2),
            "angle": round(angle, 2),
            "shot_type_foot": 1 if shot_type == "foot" else 0,
            "shot_type_header": 1 if shot_type == "header" else 0,
            "shot_type_freekick": 1 if shot_type == "freekick" else 0,
            "shot_type_penalty": 1 if shot_type == "penalty" else 0,
            "defenders": defenders,
            "is_strong_foot": is_strong_foot,
            "is_goal": is_goal,
        })

    return pd.DataFrame(data)


def main() -> None:
    ensure_dirs()
    df = generate_dataset()
    df.to_csv(XG_DATA_PATH, index=False)

    print(f"Generated {len(df)} shots -> {XG_DATA_PATH}")
    print(f"Goal rate: {df['is_goal'].mean():.3f}")
    print(f"\nSample:\n{df.head(10)}")
    print("\nGoal rate by shot type:")
    for col in ["shot_type_foot", "shot_type_header", "shot_type_freekick", "shot_type_penalty"]:
        subset = df[df[col] == 1]
        print(f"  {col}: {subset['is_goal'].mean():.3f} ({len(subset)} shots)")


if __name__ == "__main__":
    main()
