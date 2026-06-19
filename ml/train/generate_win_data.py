import pandas as pd
import numpy as np

np.random.seed(42)

n_matches = 5000
data = []

for _ in range(n_matches):
    # Simulate a match state at a random minute
    minute = np.random.randint(1, 91)

    # Score (Poisson distributed, ~1.5 goals per team per match)
    time_factor = minute / 90.0
    home_goals = np.random.poisson(1.5 * time_factor)
    away_goals = np.random.poisson(1.3 * time_factor)  # slight away disadvantage

    goal_diff = home_goals - away_goals

    # Match stats at this minute
    home_shots = int(np.random.poisson(7 * time_factor))
    away_shots = int(np.random.poisson(6 * time_factor))
    home_shots_ot = int(min(home_shots, np.random.poisson(3 * time_factor)))
    away_shots_ot = int(min(away_shots, np.random.poisson(2.5 * time_factor)))
    home_corners = int(np.random.poisson(3 * time_factor))
    away_corners = int(np.random.poisson(2.5 * time_factor))
    home_fouls = int(np.random.poisson(6 * time_factor))
    away_fouls = int(np.random.poisson(6 * time_factor))
    home_yellow = int(np.random.poisson(0.8 * time_factor))
    away_yellow = int(np.random.poisson(0.8 * time_factor))
    home_red = 1 if np.random.random() < 0.02 * time_factor else 0
    away_red = 1 if np.random.random() < 0.02 * time_factor else 0

    # Simulate final result based on current state
    remaining = 90 - minute
    remaining_factor = remaining / 90.0

    # More goals can happen in remaining time
    extra_home = np.random.poisson(1.5 * remaining_factor)
    extra_away = np.random.poisson(1.3 * remaining_factor)

    # Red card effect
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

df = pd.DataFrame(data)
df.to_csv("data/win_prob_training_data.csv", index=False)

print(f"Generated {len(df)} match states")
print(f"\nResult distribution:")
print(df["result"].value_counts(normalize=True))