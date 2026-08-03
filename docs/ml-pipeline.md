# ML pipeline

## Models

Two scikit-learn models, trained offline and loaded at startup by
`ml-predictor`:

- **xG (expected goals)** — predicts the probability a given shot results
  in a goal, from shot location (`x`, `y`), computed distance/angle to
  goal, and shot type (foot/header/freekick/penalty).
- **Win probability** — predicts home-win / draw / away-win probabilities
  from the current match state (score, shots, shots on target, corners,
  fouls, cards).

## Training data & scripts

Training data lives in `ml/data/` (`xg_training_data.csv`,
`win_prob_training_data.csv`); training scripts live in `ml/train/`.

```bash
cd ml/train
python generate_xg_data.py        # regenerates ml/data/xg_training_data.csv
python generate_win_data.py       # regenerates ml/data/win_prob_training_data.csv
python train_xg.py                # writes ml/models/xg_model.pkl + xg_features.pkl
python train_win_prob.py          # writes ml/models/win_prob_model.pkl + win_prob_features.pkl
```

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
