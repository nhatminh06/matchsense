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
| `match-events-dlq` | event-processor | *(none — inspect manually)* | `common.DLQRecord`: original topic/partition/offset/payload, failure reason, attempt count, timestamp |

All three topics rely on Kafka's `auto.create.topics.enable=true` (set in
both Compose and the Kubernetes Kafka manifest); none are provisioned
explicitly.

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
  exponentially rather than spinning. A message that cannot be parsed is
  routed to the `match-events-dlq` topic instead of being silently
  dropped — see [Retries and dead-lettering](#retries-and-dead-lettering).
- ml-predictor retries its Kafka connection up to 30 times at startup
  before giving up; per-message processing errors are caught, logged, and
  counted (`ml_predictor_processing_errors_total`) rather than crashing the
  consumer loop.

## Event delivery semantics

**MatchSense provides at-least-once delivery with idempotent handling. It
does not provide exactly-once processing.**

Kafka redelivers a message when a consumer restarts after handling it but
before committing its offset. Without protection this double-counts
statistics, so every event carries a stable `event_id` that
`event-processor` uses to recognise and skip redeliveries.

### Event identity

`event_id` is optional on the wire at `POST /events`:

- A client that supplies one makes its *own* retries idempotent — resending
  the same event ID is deduplicated downstream.
- A client that omits one gets an ID generated by `event-api`
  (`evt-` + 128 random bits).

Either way every event on the `match-events` topic carries an ID. Supplied
IDs are validated (max 128 characters; no whitespace, `:`, `{` or `}`)
because the ID becomes part of a Redis key.

Events consumed *without* an ID — e.g. from a producer predating this
field — are still processed, just not deduplicated, and are counted by
`event_processor_events_without_id_total`.

### Deduplication design

| Property | Value |
|---|---|
| Key | `event:<event_id>:dedupe` |
| States | `processing` → `processed` |
| TTL | `DEDUPE_TTL`, default `6h` |
| Atomicity | Redis `SET NX`, so exactly one consumer wins a concurrent claim |

A reservation is claimed with `SET NX` before the event is aggregated, then
moved to `processed` immediately after the in-memory statistics are updated.
"Processed" therefore means *counted in the aggregate*, not *persisted* —
Redis/Kafka write failures are tracked by their own counters and must never
cause a redelivery to be counted a second time.

### Why two states rather than one marker

A single "seen" marker written before processing loses events: if the
consumer dies between marking and aggregating, the event is never counted
and the marker suppresses the redelivery permanently. The `processing`
state avoids this — an abandoned reservation expires with its TTL and the
redelivery is then aggregated normally. This is covered by
`TestProcessEvent_EventIsNotLostWhenProcessingNeverCompletes`.

### Failure behaviour

- **Redis unreachable during reservation** — fail *open*: the event is
  processed anyway and `event_processor_dedupe_errors_total{operation="reserve"}`
  is incremented. During an outage duplicate suppression is unavailable, but
  no event is lost. (The same outage already prevents stats from being
  persisted at all, so failing closed would lose data for no benefit.)
- **`MarkProcessed` fails** — the event *is* aggregated; the reservation
  stays in `processing` and expires with its TTL. A redelivery after that
  point would be counted again.
- **Consumer crashes mid-processing** — the reservation expires; the
  redelivery is reprocessed.

### Trade-offs and limits

- **The dedup window is the TTL, not forever.** A redelivery arriving after
  `DEDUPE_TTL` is counted again. The default (6h) comfortably exceeds a
  consumer restart plus rebalance, the realistic source of redeliveries.
- **Memory cost.** Each in-flight and recently-processed event holds one
  small Redis key for the TTL. Raising the TTL widens the window and the
  memory footprint together.
- **Event IDs are global, not per-match.** Reusing an ID across matches is
  a producer bug; the second event is suppressed rather than corrupting a
  second match.

### Retries and dead-lettering

`event-processor` consumes `match-events` with explicit offset control
(`FetchMessage` + `CommitMessages`, not the auto-committing `ReadMessage`),
so an offset is only committed after the message has been handled one way
or another.

**Kafka read errors** (broker unreachable, etc.) retry with exponential
backoff, capped at 30s between attempts — unbounded in attempt count
because reconnecting is the correct behaviour for a transport-level outage,
but never busy-spinning.

**Messages that fail to parse as a `MatchEvent`** are a different kind of
failure: retrying the parse can never succeed. These are routed to the
`match-events-dlq` topic instead, carrying the original topic/partition/
offset, the raw payload, the failure reason, and an attempt count. The DLQ
publish itself is retried up to 3 times with a short backoff, since a
transient DLQ-topic failure *is* worth retrying.

The offset is committed after this handling either way. Refusing to commit
would block every message behind a bad one on that partition forever, for
a message that cannot become parseable by being redelivered. If the DLQ
publish also exhausts its retries — a true double failure — the message is
logged at `CRITICAL` and dropped; this is a real, documented gap rather
than a hidden one; see `event_processor_dlq_publish_errors_total`.

Downstream processing failures (Redis/Kafka publish errors inside
`processEvent`) are handled differently, and deliberately not routed to
the DLQ: they already fail open (the event is aggregated and the failure
is only counted), and the event has already been marked `processed` for
deduplication purposes by that point — sending it to a DLQ meant for retry
would imply it should be reprocessed, which would violate that guarantee.

### Ordering

Every event is published with the **match ID as the Kafka message key**, so
all events for one match land on the same partition and are consumed in
publication order. Ordering *across* matches is not guaranteed and is not
relied upon.

Within a match, the aggregate counters (goals, shots, cards, …) are
commutative — order does not change the totals. The non-commutative state is
`Minute`, `LastEvent`, and the `Events` slice, which reflect whatever event
was processed most recently rather than the latest match minute. There is no
event-time reordering, and none is claimed. See
`TestApplyEvent_OutOfOrderEventsOverwriteMinuteForward`.

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
