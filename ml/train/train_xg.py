"""Trains the xG (expected goals) model.

Reports metrics against a naive baseline (predict the training set's
global goal rate for every shot) so the trained model's numbers have
something to be better than, not just an absolute log loss/Brier score
that means little in isolation.
"""

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
from sklearn.model_selection import train_test_split

import joblib

from metadata import write_metadata
from paths import MODELS_DIR, XG_DATA_PATH, ensure_dirs

FEATURES = [
    "x", "y", "distance", "angle",
    "shot_type_foot", "shot_type_header",
    "shot_type_freekick", "shot_type_penalty",
    "defenders", "is_strong_foot",
]
DEFAULT_SEED = 42


def train(df: pd.DataFrame, seed: int = DEFAULT_SEED):
    """Trains the model and returns (model, metrics). metrics includes a
    naive-baseline comparison, not just the trained model's own numbers."""
    X = df[FEATURES]
    y = df["is_goal"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=seed
    )

    model = GradientBoostingClassifier(
        n_estimators=200,
        max_depth=4,
        learning_rate=0.1,
        min_samples_leaf=20,
        random_state=seed,
    )
    model.fit(X_train, y_train)

    y_pred_proba = model.predict_proba(X_test)[:, 1]

    # Naive baseline: predict the training set's overall goal rate for
    # every shot, regardless of its features.
    baseline_rate = float(y_train.mean())
    baseline_pred = np.full_like(y_pred_proba, baseline_rate)

    metrics = {
        "roc_auc": float(roc_auc_score(y_test, y_pred_proba)),
        "log_loss": float(log_loss(y_test, y_pred_proba)),
        "brier_score": float(brier_score_loss(y_test, y_pred_proba)),
        "baseline_goal_rate": baseline_rate,
        "baseline_log_loss": float(log_loss(y_test, baseline_pred)),
        "baseline_brier_score": float(brier_score_loss(y_test, baseline_pred)),
        "feature_importance": {
            name: float(importance)
            for name, importance in zip(FEATURES, model.feature_importances_)
        },
        "train_rows": len(X_train),
        "test_rows": len(X_test),
    }
    return model, metrics


def main() -> None:
    ensure_dirs()
    df = pd.read_csv(XG_DATA_PATH)
    model, metrics = train(df)

    print("=== xG Model Evaluation ===")
    print(f"ROC AUC:     {metrics['roc_auc']:.4f}")
    print(f"Log Loss:    {metrics['log_loss']:.4f}  (baseline: {metrics['baseline_log_loss']:.4f})")
    print(f"Brier Score: {metrics['brier_score']:.4f}  (baseline: {metrics['baseline_brier_score']:.4f})")
    print("\nFeature Importance:")
    for name, importance in sorted(
        metrics["feature_importance"].items(), key=lambda x: x[1], reverse=True
    ):
        print(f"  {name}: {importance:.4f}")

    model_path = MODELS_DIR / "xg_model.pkl"
    joblib.dump(model, model_path)
    joblib.dump(FEATURES, MODELS_DIR / "xg_features.pkl")

    meta_path = write_metadata(
        model_path,
        model_name="xg_model",
        model_type="GradientBoostingClassifier",
        data_source="synthetic simulator-generated shot data (ml/train/generate_xg_data.py)",
        training_rows=metrics["train_rows"],
        test_rows=metrics["test_rows"],
        random_seed=DEFAULT_SEED,
        features=FEATURES,
        metrics={k: v for k, v in metrics.items() if k not in ("train_rows", "test_rows")},
    )
    print(f"\nModel saved to {model_path}")
    print(f"Metadata saved to {meta_path}")


if __name__ == "__main__":
    main()
