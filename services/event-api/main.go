package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/nhatminh06/matchsense/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

type MatchEvent = common.MatchEvent

// eventPublisher is the subset of the Kafka writer eventsHandler needs,
// kept as an interface so unit tests can substitute a fake and exercise
// the publish-failure path without a real Kafka broker.
type eventPublisher interface {
	Publish(ctx context.Context, msg kafka.Message) error
}

type kafkaEventPublisher struct{ writer *kafka.Writer }

func (p kafkaEventPublisher) Publish(ctx context.Context, msg kafka.Message) error {
	return p.writer.WriteMessages(ctx, msg)
}

var (
	writer *kafka.Writer
	pub    eventPublisher
	tracer = otel.Tracer("event-api")

	// NOTE: intentionally labeled only by event_type, not match_id.
	// match_id is unbounded over the lifetime of the service (a new
	// label value per match forever), which would blow up Prometheus
	// cardinality. Per-match detail belongs in logs/traces, not metrics.
	eventsReceived = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "event_api_events_received_total", Help: "Total events received by type"},
		[]string{"event_type"},
	)
	eventsPublished = prometheus.NewCounter(
		prometheus.CounterOpts{Name: "event_api_events_published_total", Help: "Total events published to Kafka"},
	)
	publishErrors = prometheus.NewCounter(
		prometheus.CounterOpts{Name: "event_api_publish_errors_total", Help: "Total Kafka publish errors"},
	)
	eventIDsGenerated = prometheus.NewCounter(
		prometheus.CounterOpts{Name: "event_api_event_ids_generated_total", Help: "Events that arrived without a client-supplied event_id"},
	)
	publishLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{Name: "event_api_publish_duration_seconds", Help: "Kafka publish latency", Buckets: prometheus.DefBuckets},
	)
	httpRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "event_api_http_requests_total", Help: "Total HTTP requests"},
		[]string{"method", "path", "status"},
	)
)

func init() {
	prometheus.MustRegister(eventsReceived, eventsPublished, publishErrors, publishLatency, httpRequests, eventIDsGenerated)
}

func initTracer() func() {
	otlpEndpoint := common.GetEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "jaeger:4318")

	exporter, err := otlptracehttp.New(
		context.Background(),
		otlptracehttp.WithEndpoint(otlpEndpoint),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		log.Printf("WARNING: failed to create OTLP exporter: %v", err)
		return func() {}
	}

	res, _ := resource.New(context.Background(),
		resource.WithAttributes(semconv.ServiceName("event-api")),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	return func() {
		tp.Shutdown(context.Background())
	}
}

func main() {
	shutdown := initTracer()
	defer shutdown()

	kafkaBroker := common.GetEnv("KAFKA_BROKER", "localhost:9092")
	port := common.GetEnv("PORT", "8080")

	writer = &kafka.Writer{
		Addr:         kafka.TCP(kafkaBroker),
		Topic:        "match-events",
		Balancer:     &kafka.RoundRobin{},
		BatchTimeout: 10 * time.Millisecond,
	}
	pub = kafkaEventPublisher{writer: writer}
	defer writer.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/events", eventsHandler)
	mux.Handle("/metrics", promhttp.Handler())

	handler := otelhttp.NewHandler(mux, "event-api")

	log.Printf("event-api starting on :%s (kafka: %s)", port, kafkaBroker)
	log.Fatal(http.ListenAndServe(":"+port, handler))
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	httpRequests.WithLabelValues("GET", "/health", "200").Inc()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "service": "event-api"})
}

func eventsHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "process-event")
	defer span.End()

	if r.Method != http.MethodPost {
		httpRequests.WithLabelValues(r.Method, "/events", "405").Inc()
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event MatchEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		httpRequests.WithLabelValues("POST", "/events", "400").Inc()
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}

	if event.MatchID == "" || event.EventType == "" {
		httpRequests.WithLabelValues("POST", "/events", "400").Inc()
		http.Error(w, "match_id and event_type required", http.StatusBadRequest)
		return
	}

	// event_id is optional on the wire: a client that supplies one makes
	// its own retries idempotent (the same ID is deduplicated downstream),
	// while a client that omits one gets a generated ID here. Either way
	// every event reaching Kafka carries an ID, so event-processor can
	// always deduplicate. A supplied ID is validated because it becomes
	// part of a Redis key downstream.
	if event.EventID == "" {
		event.EventID = common.NewEventID()
		eventIDsGenerated.Inc()
	} else if err := common.ValidateEventID(event.EventID); err != nil {
		httpRequests.WithLabelValues("POST", "/events", "400").Inc()
		http.Error(w, "invalid event_id: "+err.Error(), http.StatusBadRequest)
		return
	}

	if event.Timestamp == "" {
		event.Timestamp = time.Now().UTC().Format(time.RFC3339)
	}

	span.SetAttributes(
		attribute.String("event.id", event.EventID),
		attribute.String("match.id", event.MatchID),
		attribute.String("event.type", event.EventType),
		attribute.String("event.team", event.Team),
		attribute.Int("event.minute", event.Minute),
	)

	eventsReceived.WithLabelValues(event.EventType).Inc()

	data, err := json.Marshal(event)
	if err != nil {
		httpRequests.WithLabelValues("POST", "/events", "500").Inc()
		span.RecordError(err)
		log.Printf("ERROR: failed to marshal event: %v", err)
		http.Error(w, "failed to process event", http.StatusInternalServerError)
		return
	}

	publishCtx, publishSpan := tracer.Start(ctx, "kafka-publish")

	var headers []kafka.Header
	otel.GetTextMapPropagator().Inject(publishCtx, common.KafkaHeaderCarrier{Headers: &headers})

	start := time.Now()
	err = pub.Publish(context.Background(), kafka.Message{
		Key:     []byte(event.MatchID),
		Value:   data,
		Headers: headers,
	})
	publishLatency.Observe(time.Since(start).Seconds())
	publishSpan.End()

	if err != nil {
		publishErrors.Inc()
		httpRequests.WithLabelValues("POST", "/events", "500").Inc()
		span.RecordError(err)
		log.Printf("ERROR: failed to publish: %v", err)
		http.Error(w, "failed to publish event", http.StatusInternalServerError)
		return
	}

	eventsPublished.Inc()
	httpRequests.WithLabelValues("POST", "/events", "202").Inc()

	log.Printf("[%s] event=%s min:%d %s %s - %s", event.MatchID, event.EventID, event.Minute, event.EventType, event.Team, event.Player)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"status": "accepted"})
}
