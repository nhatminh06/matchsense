# Contributing to MatchSense

Thanks for your interest in contributing. This is a small, solo-maintained
project, so the process below is intentionally lightweight.

## Prerequisites

- Docker and Docker Compose
- Go 1.25+ (for event-api, event-processor, query-api, match-simulator)
- Python 3.11+ (for ml-predictor)
- `git`

## Repository setup

```bash
git clone https://github.com/nhatminh06/matchsense.git
cd matchsense
```

## Local development

Bring up the full stack (Kafka, Redis, all 5 services, observability):

```bash
docker compose up --build
```

Then:

- `curl http://localhost:8083/matches` — list active matches
- `curl http://localhost:8083/matches/ars-mci-2026` — match stats
- Grafana: http://localhost:3000 (`admin` / `matchsense` locally, override with `GRAFANA_ADMIN_PASSWORD`)
- Prometheus: http://localhost:9090
- Jaeger UI: http://localhost:16686

To work on a single Go service:

```bash
cd services/event-api   # or event-processor, query-api, match-simulator
go build ./...
go vet ./...
gofmt -l .              # should print nothing
go test ./...           # currently no tests exist for most services — see below
```

To work on ml-predictor:

```bash
cd services/ml-predictor
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -m compileall .
```

### Resetting local state

```bash
docker compose down -v   # -v also drops the Kafka/Redis/Prometheus/Grafana volumes
```

### Common failures

- **Kafka/Redis "connection refused" on first boot** — the app services can start
  before Kafka finishes its own startup; they retry internally, so this
  usually resolves within a few seconds.
- **Grafana dashboard empty** — the `match-simulator` service needs a few
  seconds to start emitting events before there's anything to plot.

## Test coverage status

Most services do not have automated tests yet. CI runs `go vet`, `gofmt`,
and build checks for every Go service, and a lint/compile check for
ml-predictor — these catch real regressions but are not a substitute for
tests. If you're adding tests for a service that has none, that's a welcome
contribution on its own.

## Branch naming

```text
feat/<short-description>
fix/<short-description>
docs/<short-description>
test/<short-description>
refactor/<short-description>
ci/<short-description>
chore/<short-description>
```

## Commit format

This repo uses [Conventional Commits](https://www.conventionalcommits.org/):

```text
<type>(<scope>): <imperative summary>
```

Supported types: `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `build`, `ci`, `chore`, `revert`.

Scopes should reflect the actual component: `event-api`, `event-processor`,
`query-api`, `match-simulator`, `ml-predictor`, `gitops`, `observability`,
`security`, `docs`, `repo`.

Examples:

```text
feat(event-api): validate incoming event timestamps
fix(event-processor): restore match state after restart
test(query-api): cover missing match responses
ci(gitops): open digest update pull requests
docs: explain local observability setup
```

## Issue workflow

1. Open an issue describing the problem or proposal (bug report or feature
   request templates are provided).
2. Wait for a decision on scope/approach before starting large changes.

## Pull request workflow

1. Branch from the latest `main`.
2. Keep PRs focused — one logical change per PR.
3. Fill out the PR template, including testing performed and any
   deployment/observability impact.
4. The **PR title must follow Conventional Commits** — it becomes the
   squash-merge commit title on `main`.
5. Ensure CI checks pass before requesting review/merge.

## Code review expectations

This is currently a single-maintainer project, so PRs may be self-merged
once CI passes. External contributions are still reviewed against the same
checklist as the PR template.

## CI expectations

Every PR runs, at minimum: Go build/vet/fmt checks for changed Go services,
a Python compile/lint check for ml-predictor changes, secret scanning
(Gitleaks), and a PR-title format check. Changed services also get a local
Docker build + Trivy scan (no image is pushed for PR builds).

## Security reporting

See [SECURITY.md](SECURITY.md) — do not open a public issue for a
suspected vulnerability.

## Files contributors must not commit

- `.env` files or anything containing credentials/tokens
- Compiled binaries (`services/*/event-api`, etc. — already gitignored)
- Trained model artifacts outside `ml/models/` and `services/ml-predictor/models/`
- Local `__pycache__/`, `.venv/`, `venv/`
