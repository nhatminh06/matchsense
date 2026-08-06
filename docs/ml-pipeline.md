# ML pipeline

## Models

Two scikit-learn `GradientBoostingClassifier` models, trained offline and
loaded at startup by `ml-predictor`:

- **xG (expected goals)** — predicts the probability a given shot results
  in a goal, from shot location (`x`, `y`), computed distance/angle to
  goal, and shot type (foot/header/freekick/penalty).
- **Win probability** — predicts home-win / draw / away-win probabilities
  from the current match state (score, shots, shots on target, corners,
  fouls, cards).

**Both models are trained entirely on synthetic, simulator-generated
data — never on real match data.** Their metrics below describe how well
they fit that synthetic distribution's own generating rules, not real
football forecasting accuracy. Treat them as demonstrating the
ingestion → inference → serving integration, not as competitive sports
analytics.

## Training data & scripts

Training data lives in `ml/data/`; training scripts live in `ml/train/`.
Requires **Python 3.11+** — `requirements.txt` pins the same
`scikit-learn` version as `services/ml-predictor`, and that version
requires it.

```bash
cd ml/train
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python generate_xg_data.py        # regenerates ml/data/xg_training_data.csv
python generate_win_data.py       # regenerates ml/data/win_prob_training_data.csv
python train_xg.py                # writes ml/models/xg_model.{pkl,metadata.json} + xg_features.pkl
python train_win_prob.py          # writes ml/models/win_prob_model.{pkl,metadata.json} + win_prob_features.pkl
```

### Reproducibility

Both generation scripts expose a `generate_dataset(n, seed=42)` function
that draws all randomness from its own `numpy.random.Generator` rather
than the global `numpy.random` state — the same `(n, seed)` always
produces a byte-identical DataFrame, and generating one dataset can never
perturb another generated later in the same process. This is enforced by
tests (`test_ml_pipeline.py`), not just documented.

Both training scripts expose a `train(df, seed=42)` function and write a
`<model>.metadata.json` file next to the model artifact:

```json
{
  "model_name": "xg_model",
  "model_type": "GradientBoostingClassifier",
  "data_source": "synthetic simulator-generated shot data (...)",
  "training_rows": 8000,
  "test_rows": 2000,
  "random_seed": 42,
  "features": ["x", "y", "distance", ...],
  "metrics": { "...": "..." },
  "trained_at": "2026-08-06T02:29:15+00:00"
}
```

### Split methodology

Both datasets are generated one independent row per simulated shot (xG) or
per simulated match snapshot (win probability) — there is no shared
context between rows, so an ordinary random row-level `train_test_split`
does not leak information the way it could if multiple rows belonged to
the same match.

### Evaluation

Both training scripts report the trained model's metrics **against a
naive baseline** (predict the training set's overall goal rate, or its
class distribution, for every row) — an absolute log loss or Brier score
means little without something to compare it to. Run `train_xg.py` /
`train_win_prob.py` yourself to get the numbers and a
`<model>.metadata.json` for your environment; this doc intentionally
doesn't hardcode a specific run's numbers, since they'd go stale the
moment the models are retrained.

A model beating its baseline means it's extracting real signal from the
synthetic features — it says nothing about accuracy on real matches, which
has never been measured, here or anywhere in this repository.

**scikit-learn version note:** `ml/train/requirements.txt` pins
`scikit-learn` to the same version as
`services/ml-predictor/requirements.txt`, and that version currently
requires Python 3.11+. A model trained with a different scikit-learn
version than the one `ml-predictor` installs is not guaranteed to be
loadable — training in a mismatched environment produced models that
`ml-predictor` failed to load (returning `None` from every prediction)
during this repository's own CI, which is exactly how this requirement was
discovered. If regenerating locally, verify the resulting models actually
load under `ml-predictor`'s pinned scikit-learn version — e.g. by running
its test suite — before committing them.

## Deploying a retrained model

`ml-predictor` loads models from `services/ml-predictor/models/` at
container startup (`MODEL_DIR` env var, defaults to `/app/models` inside the
container). After retraining:

```bash
cp ml/models/*.pkl services/ml-predictor/models/
docker compose up --build ml-predictor
```

There is no automated retraining or model-versioning pipeline — this is a
manual step today.

## Prediction flow

`ml-predictor` consumes the `match-stats` Kafka topic. For each message:

1. If the last event was a `shot` or `goal`, run the xG model on that
   event's features and accumulate a running home/away xG total in Redis
2. Always run the win-probability model on the full current stats snapshot
3. Write the combined prediction to `match:<id>:predictions` in Redis

If a model failed to load at startup (missing `.pkl` file), that
prediction type is skipped (`predict_xg`/`predict_win_probability` return
`None`) rather than the service crashing — `ml-predictor` degrades
gracefully to serving whichever model is available.

## Known gaps

- No schema (e.g. Pydantic model) validates the shape of the feature
  dict against what the trained model actually expects — a training/serving
  feature mismatch would currently fail at `np.array([[features[f] for f in
  xg_features]])` rather than being caught earlier.
- No automated retraining, model registry, or A/B evaluation.
- No evaluation against real match data exists or is claimed.
