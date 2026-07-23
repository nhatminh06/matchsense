package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

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

type MatchEvent struct {
	MatchID   string  `json:"match_id"`
	Minute    int     `json:"minute"`
	EventType string  `json:"event_type"`
	Team      string  `json:"team"`
	Player    string  `json:"player"`
	X         float64 `json:"x"`
	Y         float64 `json:"y"`
	Detail    string  `json:"detail,omitempty"`
	Timestamp string  `json:"timestamp"`
}

var (
	writer *kafka.Writer
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
	publishLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{Name: "event_api_publish_duration_seconds", Help: "Kafka publish latency", Buckets: prometheus.DefBuckets},
	)
	httpRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "event_api_http_requests_total", Help: "Total HTTP requests"},
		[]string{"method", "path", "status"},
	)
)

func init() {
	prometheus.MustRegister(eventsReceived, eventsPublished, publishErrors, publishLatency, httpRequests)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func initTracer() func() {
	otlpEndpoint := getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "jaeger:4318")

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

// kafkaHeaderCarrier adapts kafka.Header slice to satisfy TextMapCarrier
type kafkaHeaderCarrier struct {
	headers *[]kafka.Header
}

func (c kafkaHeaderCarrier) Get(key string) string {
	for _, h := range *c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c kafkaHeaderCarrier) Set(key string, value string) {
	*c.headers = append(*c.headers, kafka.Header{Key: key, Value: []byte(value)})
}

func (c kafkaHeaderCarrier) Keys() []string {
	keys := make([]string, len(*c.headers))
	for i, h := range *c.headers {
		keys[i] = h.Key
	}
	return keys
}

func main() {
	shutdown := initTracer()
	defer shutdown()

	kafkaBroker := getEnv("KAFKA_BROKER", "localhost:9092")
	port := getEnv("PORT", "8080")

	writer = &kafka.Writer{
		Addr:         kafka.TCP(kafkaBroker),
		Topic:        "match-events",
		Balancer:     &kafka.RoundRobin{},
		BatchTimeout: 10 * time.Millisecond,
	}
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

	if event.Timestamp == "" {
		event.Timestamp = time.Now().UTC().Format(time.RFC3339)
	}

	span.SetAttributes(
		attribute.String("match.id", event.MatchID),
		attribute.String("event.type", event.EventType),
		attribute.String("event.team", event.Team),
		attribute.Int("event.minute", event.Minute),
	)

	eventsReceived.WithLabelValues(event.EventType).Inc()

	data, _ := json.Marshal(event)

	publishCtx, publishSpan := tracer.Start(ctx, "kafka-publish")

	var headers []kafka.Header
	otel.GetTextMapPropagator().Inject(publishCtx, kafkaHeaderCarrier{headers: &headers})

	start := time.Now()
	err := writer.WriteMessages(context.Background(), kafka.Message{
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

	log.Printf("[%s] min:%d %s %s - %s", event.MatchID, event.Minute, event.EventType, event.Team, event.Player)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"status": "accepted"})
}
