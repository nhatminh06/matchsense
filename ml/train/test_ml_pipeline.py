"""Tests for the training pipeline: data generation and training functions.

Uses small fixtures (tens of rows, not the real 10000/5000-row datasets)
so these run fast in ordinary CI rather than performing a full training
run. Full-scale generation and training is exercised manually, not here.
"""

import json

import numpy as np
import pandas as pd
import pytest

import generate_win_data
import generate_xg_data
import train_win_prob
import train_xg
from metadata import write_metadata

# --- xG data generation ---


def test_generate_xg_dataset_same_seed_is_identical():
    a = generate_xg_data.generate_dataset(n_samples=100, seed=7)
    b = generate_xg_data.generate_dataset(n_samples=100, seed=7)
    pd.testing.assert_frame_equal(a, b)


def test_generate_xg_dataset_different_seed_differs():
    a = generate_xg_data.generate_dataset(n_samples=200, seed=1)
    b = generate_xg_data.generate_dataset(n_samples=200, seed=2)
    assert not a.equals(b)


def test_generate_xg_dataset_schema_and_bounds():
    df = generate_xg_data.generate_dataset(n_samples=300, seed=3)

    expected_columns = {
        "x", "y", "distance", "angle",
        "shot_type_foot", "shot_type_header",
        "shot_type_freekick", "shot_type_penalty",
        "defenders", "is_strong_foot", "is_goal",
    }
    assert set(df.columns) == expected_columns
    assert len(df) == 300

    assert df["is_goal"].isin([0, 1]).all()
    assert df["defenders"].ge(0).all()
    # Every shot has exactly one shot_type flag set.
    shot_type_cols = ["shot_type_foot", "shot_type_header", "shot_type_freekick", "shot_type_penalty"]
    assert (df[shot_type_cols].sum(axis=1) == 1).all()


def test_generate_xg_dataset_does_not_mutate_global_numpy_state():
    # A regression guard: generate_dataset must use its own Generator, not
    # np.random.seed(), or two calls in the same process would silently
    # affect each other's output.
    before = np.random.get_state()
    generate_xg_data.generate_dataset(n_samples=50, seed=99)
    after = np.random.get_state()
    assert before[1].tolist() == after[1].tolist()


# --- win-probability data generation ---


def test_generate_win_dataset_same_seed_is_identical():
    a = generate_win_data.generate_dataset(n_matches=100, seed=7)
    b = generate_win_data.generate_dataset(n_matches=100, seed=7)
    pd.testing.assert_frame_equal(a, b)


def test_generate_win_dataset_schema_and_bounds():
    df = generate_win_data.generate_dataset(n_matches=300, seed=3)

    assert len(df) == 300
    assert df["result"].isin(["home_win", "draw", "away_win"]).all()
    assert df["minute"].between(1, 90).all()
    assert (df["goal_diff"] == df["home_goals"] - df["away_goals"]).all()
    assert (df["shot_diff"] == df["home_shots"] - df["away_shots"]).all()


def test_generate_win_dataset_no_duplicate_rows_across_matches():
    # Each row is an independently simulated match; with enough numeric
    # fields, an exact duplicate would suggest the RNG isn't actually
    # varying between iterations.
    df = generate_win_data.generate_dataset(n_matches=500, seed=11)
    assert df.duplicated().sum() < len(df) * 0.05


# --- training ---


def test_train_xg_produces_a_model_that_beats_the_baseline():
    df = generate_xg_data.generate_dataset(n_samples=800, seed=42)
    model, metrics = train_xg.train(df, seed=42)

    assert hasattr(model, "predict_proba")
    assert metrics["train_rows"] + metrics["test_rows"] == len(df)
    assert set(metrics["feature_importance"].keys()) == set(train_xg.FEATURES)

    # A model that's worse than "always predict the base rate" would be a
    # real regression, not just a weak model.
    assert metrics["log_loss"] <= metrics["baseline_log_loss"]


def test_train_xg_predictions_are_valid_probabilities():
    df = generate_xg_data.generate_dataset(n_samples=500, seed=5)
    model, _ = train_xg.train(df, seed=5)

    proba = model.predict_proba(df[train_xg.FEATURES].head(20))[:, 1]
    assert ((proba >= 0.0) & (proba <= 1.0)).all()


def test_train_win_prob_produces_a_model_that_beats_the_baseline():
    df = generate_win_data.generate_dataset(n_matches=800, seed=42)
    model, metrics = train_win_prob.train(df, seed=42)

    assert set(model.classes_) == {"home_win", "draw", "away_win"}
    assert metrics["log_loss"] <= metrics["baseline_log_loss"]


def test_train_win_prob_predictions_sum_to_one():
    df = generate_win_data.generate_dataset(n_matches=500, seed=5)
    model, _ = train_win_prob.train(df, seed=5)

    proba = model.predict_proba(df[train_win_prob.FEATURES].head(20))
    row_sums = proba.sum(axis=1)
    assert np.allclose(row_sums, 1.0, atol=1e-6)


def test_multiclass_brier_score_is_zero_for_perfect_predictions():
    y_true = pd.Series(["home_win", "draw", "away_win"])
    classes = ["away_win", "draw", "home_win"]
    perfect = np.array([
        [0, 0, 1],  # home_win
        [0, 1, 0],  # draw
        [1, 0, 0],  # away_win
    ])
    score = train_win_prob.multiclass_brier_score(y_true, perfect, classes)
    assert score == pytest.approx(0.0)


# --- metadata ---


def test_write_metadata_round_trips(tmp_path):
    model_path = tmp_path / "some_model.pkl"
    model_path.write_bytes(b"not a real model, just a placeholder")

    out_path = write_metadata(
        model_path,
        model_name="some_model",
        model_type="GradientBoostingClassifier",
        data_source="synthetic test fixture",
        training_rows=80,
        test_rows=20,
        random_seed=42,
        features=["a", "b"],
        metrics={"log_loss": 0.5},
    )

    assert out_path == tmp_path / "some_model.metadata.json"
    loaded = json.loads(out_path.read_text())

    assert loaded["model_name"] == "some_model"
    assert loaded["training_rows"] == 80
    assert loaded["test_rows"] == 20
    assert loaded["random_seed"] == 42
    assert loaded["features"] == ["a", "b"]
    assert loaded["metrics"] == {"log_loss": 0.5}
    assert "trained_at" in loaded
