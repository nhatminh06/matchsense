import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import log_loss, classification_report
import joblib

df = pd.read_csv("data/win_prob_training_data.csv")

features = [
    "minute", "home_goals", "away_goals", "goal_diff",
    "home_shots", "away_shots",
    "home_shots_on_target", "away_shots_on_target",
    "shot_diff", "home_corners", "away_corners",
    "home_fouls", "away_fouls",
    "home_yellow", "away_yellow",
    "home_red", "away_red",
]

X = df[features]
y = df["result"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

model = GradientBoostingClassifier(
    n_estimators=200,
    max_depth=4,
    learning_rate=0.1,
    min_samples_leaf=20,
    random_state=42
)
model.fit(X_train, y_train)

y_pred = model.predict(X_test)
y_pred_proba = model.predict_proba(X_test)

print("=== Win Probability Model Evaluation ===")
print(f"Log Loss: {log_loss(y_test, y_pred_proba):.4f}")
print(f"\n{classification_report(y_test, y_pred)}")

print("\nFeature Importance:")
for name, importance in sorted(
    zip(features, model.feature_importances_),
    key=lambda x: x[1],
    reverse=True
):
    print(f"  {name}: {importance:.4f}")

print(f"\nClasses: {model.classes_}")

joblib.dump(model, "models/win_prob_model.pkl")
joblib.dump(features, "models/win_prob_features.pkl")
print("\nModel saved to models/win_prob_model.pkl")