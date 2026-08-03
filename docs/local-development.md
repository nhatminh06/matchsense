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
go test -race ./...     # currently "[no test files]" for most services
```

**ml-predictor**:

```bash
cd services/ml-predictor
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -m compileall .
pip install ruff && ruff check --select F .
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

Most services have no automated tests yet. This is a real, open gap — see
the [Roadmap in the README](../README.md#roadmap). CI enforces `gofmt`,
`go vet`, `go build`, and `go test` (which currently passes only because
there's nothing to test) for the Go services, and a `ruff check --select F`
plus a compile check for `ml-predictor`. If you add a service's first test
file, CI will start actually running it — no workflow changes needed.

Priority areas for future test coverage, in order of risk:

1. `event-processor`'s stats-aggregation logic (`processEvent`) — the most
   stateful, bug-prone code in the system
2. `ml-predictor`'s feature construction (`predict_xg`,
   `predict_win_probability`) — silent feature-shape mismatches would fail
   quietly
3. HTTP handler validation in `event-api` / `query-api`
