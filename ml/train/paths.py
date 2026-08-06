"""Shared, absolute paths for the training pipeline.

Every script in this directory previously wrote/read via bare relative
paths like "data/xg_training_data.csv", which only resolve correctly if
the process happens to be run with ml/ (not ml/train/, which is what the
README documents: `cd ml/train && python generate_xg_data.py`) as the
working directory. Resolving from this file's location instead makes the
scripts work regardless of the caller's cwd.
"""

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
MODELS_DIR = BASE_DIR / "models"

XG_DATA_PATH = DATA_DIR / "xg_training_data.csv"
WIN_PROB_DATA_PATH = DATA_DIR / "win_prob_training_data.csv"


def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
