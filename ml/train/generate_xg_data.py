from pathlib import Path
import pandas as pd
import numpy as np
import math

np.random.seed(42)

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
DATA_PATH = DATA_DIR / "xg_training_data.csv"

DATA_DIR.mkdir(parents=True, exist_ok=True)


def goal_probability(x, y, shot_type):
    """
    Realistic xG based on distance and angle to goal.
    Goal is at x=100, y=50 (center of goal line).
    """
    # Distance to center of goal
    dist = math.sqrt((100 - x) ** 2 + (50 - y) ** 2)

    # Angle to goal
    goal_width = 7.32  # meters
    scaled_width = goal_width * (100 / 105)
    angle = math.atan2(scaled_width, dist * (105 / 100))

    # Base probability from distance and angle
    base_prob = 0.5 * math.exp(-dist / 25) * (angle / (math.pi / 2))

    # Modifiers
    if shot_type == "header":
        base_prob *= 0.7
    elif shot_type == "freekick":
        base_prob *= 0.4
    elif shot_type == "penalty":
        base_prob = 0.76

    # Add noise
    noise = np.random.normal(0, 0.03)
    return np.clip(base_prob + noise, 0.01, 0.95)


# Generate 10000 shots
n_samples = 10000
data = []

for _ in range(n_samples):
    # Shot location: mostly in attacking third
    x = np.random.beta(5, 2) * 40 + 60
    y = np.random.normal(50, 20)
    y = np.clip(y, 5, 95)

    # Distance to goal center
    distance = math.sqrt((100 - x) ** 2 + (50 - y) ** 2)

    # Angle
    angle = math.degrees(math.atan2(7.0, distance * (105 / 100)))

    # Shot type
    shot_type = np.random.choice(
        ["foot", "header", "freekick", "penalty"],
        p=[0.70, 0.15, 0.10, 0.05]
    )

    # Number of defenders
    defenders = max(0, int(np.random.normal(3 - distance / 15, 1.5)))

    # Body part
    if shot_type == "foot":
        is_strong_foot = 1 if np.random.random() < 0.75 else 0
    else:
        is_strong_foot = 0

    # Calculate true probability and simulate outcome
    true_prob = goal_probability(x, y, shot_type)

    # Defender effect
    true_prob *= max(0.2, 1 - defenders * 0.08)

    is_goal = 1 if np.random.random() < true_prob else 0

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

df = pd.DataFrame(data)
df.to_csv("data/xg_training_data.csv", index=False)

print(f"Generated {len(df)} shots")
print(f"Goal rate: {df['is_goal'].mean():.3f}")
print(f"\nSample:\n{df.head(10)}")
print(f"\nGoal rate by shot type:")
for col in ["shot_type_foot", "shot_type_header", "shot_type_freekick", "shot_type_penalty"]:
    subset = df[df[col] == 1]
    print(f"  {col}: {subset['is_goal'].mean():.3f} ({len(subset)} shots)")

for col in [
    "shot_type_foot",
    "shot_type_header",
    "shot_type_freekick",
    "shot_type_penalty"
]:
    subset = df[df[col] == 1]
    print(f"  {col}: {subset['is_goal'].mean():.3f} ({len(subset)} shots)")