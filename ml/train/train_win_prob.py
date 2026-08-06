"""Trains the win-probability model.

Reports metrics against a naive baseline (predict the training set's
class distribution -- e.g. "45% home_win, 28% draw, 27% away_win" -- for
every match, ignoring its actual state) so the trained model's numbers
have something to be better than.
"""

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import log_loss
from sklearn.model_selection import train_test_split

import joblib

from metadata import write_metadata
from paths import MODELS_DIR, WIN_PROB_DATA_PATH, ensure_dirs

FEATURES = [
    "minute", "home_goals", "away_goals", "goal_diff",
    "home_shots", "away_shots",
    "home_shots_on_target", "away_shots_on_target",
    "shot_diff", "home_corners", "away_corners",
    "home_fouls", "away_fouls",
    "home_yellow", "away_yellow",
    "home_red", "away_red",
]
DEFAULT_SEED = 42


def multiclass_brier_score(y_true, y_proba, classes) -> float:
    """Mean squared error between one-hot true labels and predicted
    probabilities, averaged over classes -- the standard multi-class
    generalisation of the binary Brier score (sklearn's brier_score_loss
    only supports binary targets)."""
    one_hot = pd.get_dummies(y_true).reindex(columns=classes, fill_value=0).to_numpy()
    return float(np.mean(np.sum((one_hot - y_proba) ** 2, axis=1)))


def train(df: pd.DataFrame, seed: int = DEFAULT_SEED):
    X = df[FEATURES]
    y = df["result"]

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

    y_pred_proba = model.predict_proba(X_test)
    classes = list(model.classes_)

    # Naive baseline: predict the training set's class distribution for
    # every match, regardless of its actual state.
    class_rates = y_train.value_counts(normalize=True).reindex(classes, fill_value=0.0)
    baseline_proba = np.tile(class_rates.to_numpy(), (len(y_test), 1))

    metrics = {
        "log_loss": float(log_loss(y_test, y_pred_proba, labels=classes)),
        "brier_score": multiclass_brier_score(y_test, y_pred_proba, classes),
        "baseline_class_rates": class_rates.to_dict(),
        "baseline_log_loss": float(log_loss(y_test, baseline_proba, labels=classes)),
        "baseline_brier_score": multiclass_brier_score(y_test, baseline_proba, classes),
        "classes": classes,
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
    df = pd.read_csv(WIN_PROB_DATA_PATH)
    model, metrics = train(df)

    print("=== Win Probability Model Evaluation ===")
    print(f"Log Loss:    {metrics['log_loss']:.4f}  (baseline: {metrics['baseline_log_loss']:.4f})")
    print(f"Brier Score: {metrics['brier_score']:.4f}  (baseline: {metrics['baseline_brier_score']:.4f})")
    print(f"Classes: {metrics['classes']}")
    print("\nFeature Importance:")
    for name, importance in sorted(
        metrics["feature_importance"].items(), key=lambda x: x[1], reverse=True
    ):
        print(f"  {name}: {importance:.4f}")

    model_path = MODELS_DIR / "win_prob_model.pkl"
    joblib.dump(model, model_path)
    joblib.dump(FEATURES, MODELS_DIR / "win_prob_features.pkl")

    meta_path = write_metadata(
        model_path,
        model_name="win_prob_model",
        model_type="GradientBoostingClassifier",
        data_source="synthetic simulator-generated match-state data (ml/train/generate_win_data.py)",
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
