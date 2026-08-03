# Observability

## What's wired up

- **Prometheus** scrapes `event-api`, `event-processor`, `query-api`, and
  `ml-predictor` (see `observability/prometheus/prometheus.yml`).
  `match-simulator` doesn't expose metrics (it's a short-lived generator
  process, not a long-running API).
- **Grafana** auto-provisions a datasource and a "MatchSense - Live Match"
  dashboard (13 panels) on startup
  (`observability/grafana/provisioning`, `observability/grafana/dashboards`).
- **Loki + Promtail** collect container logs from the Docker Compose stack.
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
