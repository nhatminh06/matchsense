# Observability

## What's wired up

- **Prometheus** scrapes `event-api`, `event-processor`, `query-api`, and
  `ml-predictor` (see `observability/prometheus/prometheus.yml`).
  `match-simulator` doesn't expose metrics (it's a short-lived generator
  process, not a long-running API).
- **Grafana** auto-provisions a datasource and a "MatchSense - Live Match"
  dashboard (13 panels) on startup
  (`observability/grafana/provisioning`, `observability/grafana/dashboards`).
- **Loki + Grafana Alloy** collect application logs. Compose sends RFC5424
  syslog over TCP without exposing the Docker API; Kubernetes Alloy reads pod
  logs through the Kubernetes API without node filesystem mounts.
- **Jaeger** collects distributed traces. Every event's trace context is
  propagated across HTTP calls and Kafka message headers, so a single event
  can be followed end to end: `event-api` → Kafka → `event-processor` →
  Kafka → `ml-predictor`.

## Metric design notes

Metrics are deliberately **not** labeled by `match_id`. A per-match label
would create a new, permanent Prometheus time series for every match ever
played — unbounded cardinality growth over the service's lifetime.
Per-match detail is available via Redis (`query-api`) and traces (Jaeger)
instead; Prometheus metrics track system-wide rates and error counts.

Notable metrics:

| Metric | Service | Meaning |
|---|---|---|
| `event_api_publish_errors_total` | event-api | Kafka publish failures |
| `event_processor_kafka_read_errors_total` | event-processor | Kafka consumer read errors (with backoff) |
| `event_processor_redis_write_errors_total` | event-processor | Redis write failures |
| `event_processor_kafka_write_errors_total` | event-processor | Failures publishing to `match-stats` |
| `ml_predictor_processing_errors_total{stage}` | ml-predictor | Per-stage prediction/Redis errors |
| `ml_predictor_kafka_messages_total` | ml-predictor | Messages consumed from `match-stats` |

## What's not wired up yet

**There is no alerting.** Prometheus and Grafana give you dashboards and
raw metrics, not proactive notification — if `event_processor_redis_write_errors_total`
starts climbing, nothing pages anyone. Adding Prometheus alerting rules for
the error-rate metrics above (and Kafka consumer lag) is on the
[roadmap](../README.md#roadmap).

## Local access

- Grafana: http://localhost:3000
- Prometheus: http://localhost:9090
- Jaeger: http://localhost:16686

## Log collection

The Compose logging pipeline covers `event-api`, `event-processor`,
`query-api`, `ml-predictor`, and `match-simulator`. Each stream keeps the
`container` label used by existing Loki queries and also has `service_name`,
`platform="docker"`, and `job="docker"`. Example LogQL:

```logql
{service_name="event-api"}
```

On Kubernetes, Alloy runs once as a non-root Deployment and uses the
namespace-scoped `pods`/`pods/log` API. It collects the same five applications
with `app`, `service_name`, `namespace`, `pod`, and `container` labels. This is
portable across containerd, CRI-O, and Docker-based nodes because it does not
read runtime-specific host paths.

Compose defaults to `tcp://127.0.0.1:1514`, which is tested on Linux Docker
Engine. The Docker logging driver runs in the daemon, not in the application
container. On Docker Desktop, set
`ALLOY_SYSLOG_ADDRESS=tcp://host.docker.internal:1514` only after confirming
that the Desktop backend can reach the published Alloy port. If it cannot,
application containers fail visibly instead of silently dropping collection.
Do not work around that failure by mounting `docker.sock`. The safe fallback is
to run Alloy on the host with a loopback-only syslog listener, or keep logs on
stdout and use Docker Desktop's own supported log export until application
OTLP log export is implemented.

The syslog receiver is intentionally bound to host loopback. Alloy's HTTP UI is
not published, its root filesystem is read-only, and it runs as UID/GID 473
with all Linux capabilities dropped.

Docker's syslog protocol does not preserve Promtail's `stream=stdout|stderr`
label. The stable service identity remains available as both `container` and
`service_name`; no repository dashboard filters on `stream`. Compose uses the
driver's default blocking delivery so collector failure is visible and logs are
not silently discarded, but an unavailable Alloy listener can pause or prevent
application startup. Kubernetes Alloy is a single replica in this development
stack, so collection pauses during its restart and its API-based tailing adds
Kubelet/API traffic; scale and cluster it before production use.
