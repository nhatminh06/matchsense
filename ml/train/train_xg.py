import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    log_loss, brier_score_loss, roc_auc_score,
    classification_report
)
from sklearn.calibration import calibration_curve
import joblib

# Load data
df = pd.read_csv("data/xg_training_data.csv")

features = [
    "x", "y", "distance", "angle",
    "shot_type_foot", "shot_type_header",
    "shot_type_freekick", "shot_type_penalty",
    "defenders", "is_strong_foot"
]

X = df[features]
y = df["is_goal"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# Train Gradient Boosting (better calibrated than Random Forest for probabilities)
model = GradientBoostingClassifier(
    n_estimators=200,
    max_depth=4,
    learning_rate=0.1,
    min_samples_leaf=20,
    random_state=42
)
model.fit(X_train, y_train)

# Evaluate
y_pred_proba = model.predict_proba(X_test)[:, 1]
y_pred = model.predict(X_test)

print("=== xG Model Evaluation ===")
print(f"ROC AUC:     {roc_auc_score(y_test, y_pred_proba):.4f}")
print(f"Log Loss:    {log_loss(y_test, y_pred_proba):.4f}")
print(f"Brier Score: {brier_score_loss(y_test, y_pred_proba):.4f}")
print(f"\n{classification_report(y_test, y_pred)}")

# Feature importance
print("\nFeature Importance:")
for name, importance in sorted(
    zip(features, model.feature_importances_),
    key=lambda x: x[1],
    reverse=True
):
    print(f"  {name}: {importance:.4f}")

# Save model
joblib.dump(model, "models/xg_model.pkl")
joblib.dump(features, "models/xg_features.pkl")
print("\nModel saved to models/xg_model.pkl")