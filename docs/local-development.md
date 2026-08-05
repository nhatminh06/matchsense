# Local development & testing

## Cloning and setup

```bash
git clone https://github.com/nhatminh06/matchsense.git
cd matchsense
```

Prerequisites: Docker + Docker Compose, Go 1.25+ (for the 4 Go services),
Python 3.11+ (for ml-predictor).

## Docker Compose workflow

Bring up everything:

```bash
docker compose up --build
```

Bring up a subset (e.g. just the Go services, skipping observability):

```bash
docker compose up --build event-api event-processor query-api match-simulator kafka redis
```

Tear down, including volumes (Kafka/Redis/Prometheus/Grafana state):

```bash
docker compose down -v
```

## Running a single service

**Go services** (`event-api`, `event-processor`, `query-api`,
`match-simulator`):

```bash
cd services/event-api   # or any other Go service
go build ./...
go vet ./...
gofmt -l .              # should print nothing — CI fails if it does
go test -race ./...
```

**ml-predictor**:

```bash
cd services/ml-predictor
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -m compileall .
pip install ruff && ruff check --select F .
pip install pytest httpx && pytest -v
```

## Local observability

- Grafana: http://localhost:3000 (`admin` / `matchsense` locally)
- Prometheus: http://localhost:9090
- Jaeger: http://localhost:16686

The "MatchSense - Live Match" Grafana dashboard is auto-provisioned; give
`match-simulator` a few seconds after startup before expecting data.

## Resetting local state

```bash
docker compose down -v
docker compose up --build
```

## Common failures

- **Kafka/Redis "connection refused" on first boot** — dependent services
  retry internally; this typically clears within a few seconds as Kafka
  finishes its own startup.
- **Grafana dashboard empty** — `match-simulator` needs a few seconds to
  start emitting events.
- **`docker compose build` failing on a Go service after pulling changes
  that touch `services/common`** — the 4 Go services' Dockerfiles build
  from the repository root (not their own service directory) specifically
  so they can `COPY services/common`; make sure you're running
  `docker compose build`, not building an individual Dockerfile with the
  wrong context.

## Test coverage status

All 5 services now have a unit test suite (`go test` for the 4 Go
services, `pytest` for ml-predictor). None of them require Docker, a real
Kafka broker, or a real Redis instance — Kafka/Redis interactions are
exercised through small interfaces (`statsStore`/`statsPublisher` in
event-processor, `eventPublisher` in event-api, `matchStore` in query-api)
with fakes substituted in tests, and ml-predictor's tests run against the
real committed model artifacts under `services/ml-predictor/models/`.

What's covered:

- **event-processor** (`main_test.go`): goal/shot/on-target/corner/foul/
  card aggregation, home/away team detection (including the empty-team
  edge case), unknown event types, and `processEvent`'s Redis-write-failure
  and Kafka-publish-failure paths. Deduplication is covered separately:
  duplicate delivery, distinct IDs, IDs reused across matches, events with
  no ID, dedup-store failure (fail-open), `MarkProcessed` failure,
  crash-before-completion recovery, and concurrent duplicates. Out-of-order
  delivery is still **not** reordered or rejected — that remains a
  documented behaviour, not a guarantee (see
  [Event delivery semantics](architecture.md#event-delivery-semantics)).
- **event-api** (`main_test.go`): valid event acceptance, invalid JSON,
  missing required fields, wrong HTTP method, Kafka publish failure
  (returns 500), that a client-supplied timestamp is preserved rather
  than overwritten, and `event_id` handling — generated when absent,
  unique across requests, preserved when supplied, rejected when malformed.
- **query-api** (`main_test.go`): listing active matches, skipping a match
  whose stats key is missing, Redis-unavailable handling, existing vs.
  missing match detail, the "stats present but predictions absent"
  placeholder response, and an empty match ID.
- **match-simulator** (`main_test.go`): seeded determinism (the same seed
  replays a byte-identical match, different seeds diverge, and draining the
  global `math/rand` source cannot shift the stream), event schema and
  0-100 coordinate bounds, the goal-always-follows-an-on-target-shot
  invariant, bounded send retries (transient retried, 4xx not retried,
  attempt budget capped, cancellation honoured), HTTP status
  classification, and that the run loop stops promptly on cancellation and
  survives an undeliverable event.
- **ml-predictor** (`test_app.py`): `predict_xg` bounds and default-value
  handling, `predict_win_probability` returning all three outcomes summing
  to ~1.0, both prediction functions' behavior when a model failed to
  load (`None`), and the `/health`, `/predict/xg`, and `/metrics` HTTP
  endpoints via FastAPI's `TestClient`.

What's still not covered (open gaps, not silently ignored):

- ml-predictor's Kafka consumer loop (`kafka_consumer_loop`) itself isn't
  unit-tested — only the prediction functions it calls are
- No integration/end-to-end test exercises the full
  event-api → event-processor → ml-predictor → query-api pipeline
  together; each service is tested in isolation
- Model-training scripts under `ml/train/` have no tests
