# Architecture

## High-level overview

MatchSense is five services connected by Kafka and Redis, deployed either
via Docker Compose (local) or Kubernetes + ArgoCD (GitOps), with a shared
observability stack (Prometheus, Grafana, Loki, Jaeger).

```text
match-simulator ──HTTP──▶ event-api ──Kafka(match-events)──▶ event-processor
                                                                     │
                                                    Redis(match:*:stats)
                                                                     │
                                                       Kafka(match-stats)
                                                                     ▼
                                                              ml-predictor
                                                                     │
                                                  Redis(match:*:predictions)
                                                                     ▼
                                                              query-api ──▶ clients / Grafana
```

## Service responsibilities

- **event-api** (Go) — validates and accepts match events over HTTP, publishes
  them to the `match-events` Kafka topic. Stateless.
- **event-processor** (Go) — the only stateful service. Consumes
  `match-events`, maintains an in-memory `map[string]*MatchStats` per match
  (guarded by a mutex), persists each update to Redis, republishes the
  updated stats to the `match-stats` Kafka topic, and exposes `/health` +
  `/metrics`. On startup, it rehydrates in-memory state for all matches
  listed in Redis's `matches:active` set, so a restart mid-match doesn't
  reset the score to zero.
- **ml-predictor** (Python/FastAPI) — consumes `match-stats`, runs the xG
  model on shot/goal events and the win-probability model on the full stats
  snapshot, and writes predictions to Redis. Also serves a synchronous
  `GET /predict/xg` endpoint for ad-hoc queries.
- **query-api** (Go) — stateless read API over Redis for match lists,
  per-match stats, and predictions.
- **match-simulator** (Go) — generates a synthetic 90-minute match (goals,
  shots, fouls, corners, cards) and POSTs events to `event-api`. Used for
  local development and demos; not a production event source.

## Kafka topics

| Topic | Producer | Consumer | Payload |
|---|---|---|---|
| `match-events` | event-api | event-processor | Raw `MatchEvent` (goal/shot/foul/corner/card) |
| `match-stats` | event-processor | ml-predictor | Full `MatchStats` snapshot after each event |

## Redis state

| Key pattern | Written by | Read by | Contents |
|---|---|---|---|
| `match:<id>:stats` | event-processor | event-processor (on restart), query-api | Latest full stats snapshot |
| `match:<id>:latest` | event-processor | — | Latest snapshot (duplicate write for potential future fan-out) |
| `match:<id>:predictions` | ml-predictor | query-api | Latest xG/win-probability prediction |
| `matches:active` (set) | event-processor | event-processor (on restart), query-api | IDs of matches currently being tracked |

## Trust boundaries

- `event-api` is the only externally-facing HTTP surface for event
  ingestion; it does basic field validation (`match_id`, `event_type`
  required) but does not authenticate callers — acceptable for a demo/local
  setup, not for an untrusted network.
- Internal service-to-service traffic (Kafka, Redis, inter-service HTTP) is
  unauthenticated and unencrypted by default, consistent with a
  single-namespace Kubernetes deployment where the trust boundary is the
  cluster network, not each connection.
- Kyverno policies are the enforcement point at the cluster boundary:
  images must be Cosign-signed, cannot run as privileged, and must declare
  resource limits before being admitted.

## Failure boundaries

- If Redis is down, event-processor logs and counts the error
  (`event_processor_redis_write_errors_total`) but keeps processing —
  in-memory state stays correct, only persistence/read-API visibility is
  affected until Redis recovers.
- If Kafka is down, event-api rejects new events with a 500 (publish
  failure is not swallowed); event-processor's read loop backs off
  exponentially rather than spinning.
- ml-predictor retries its Kafka connection up to 30 times at startup
  before giving up; per-message processing errors are caught, logged, and
  counted (`ml_predictor_processing_errors_total`) rather than crashing the
  consumer loop.

## Observability flow

Every event carries an OpenTelemetry trace context across process and
transport boundaries: HTTP → Kafka headers → consumer → Redis → Kafka
headers → consumer. Metrics are deliberately **not** labeled by `match_id`
(see the cardinality comments in `event-api`/`ml-predictor` source) since
that label would grow without bound over the service's lifetime; per-match
detail lives in Redis/traces instead.

## Deployment flow

See [deployment.md](deployment.md) for the full build → scan → sign →
GitOps-PR → ArgoCD flow.
