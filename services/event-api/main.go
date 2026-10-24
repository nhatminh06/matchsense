package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"mime"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
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

const (
	maxRequestBody = 16 << 10
	maxStringField = 256
	minAPIKeyBytes = 32
)

type tokenBucket struct {
	mu     sync.Mutex
	tokens float64
	burst  float64
	rate   float64
	last   time.Time
}

func newTokenBucket(rate float64, burst int) *tokenBucket {
	return &tokenBucket{tokens: float64(burst), burst: float64(burst), rate: rate, last: time.Now()}
}

func (b *tokenBucket) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	b.tokens += now.Sub(b.last).Seconds() * b.rate
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

var (
	writer         *kafka.Writer
	pub            eventPublisher
	tracer         = otel.Tracer("event-api")
	apiKey         string
	limiter        = newTokenBucket(20, 40)
	publishTimeout = 3 * time.Second

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
	var err error
	apiKey, err = requiredAPIKey(os.Getenv("EVENT_API_KEY"))
	if err != nil {
		log.Fatal(err)
	}
	rate, err := positiveFloatEnv("EVENT_API_RATE_LIMIT", 20)
	if err != nil {
		log.Fatal(err)
	}
	burst, err := positiveIntEnv("EVENT_API_RATE_BURST", 40)
	if err != nil {
		log.Fatal(err)
	}
	publishTimeout, err = positiveDurationEnv("EVENT_API_PUBLISH_TIMEOUT", 3*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	limiter = newTokenBucket(rate, burst)

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

	handler := apiKeyMiddleware(otelhttp.NewHandler(mux, "event-api"))

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	log.Printf("event-api starting on :%s (kafka: %s)", port, kafkaBroker)
	log.Fatal(server.ListenAndServe())
}

func requiredAPIKey(key string) (string, error) {
	if len(key) < minAPIKeyBytes {
		return "", fmt.Errorf("EVENT_API_KEY must be set to at least %d characters", minAPIKeyBytes)
	}
	return key, nil
}

func positiveFloatEnv(name string, fallback float64) (float64, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil || v <= 0 || math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, fmt.Errorf("%s must be a positive number", name)
	}
	return v, nil
}

func positiveIntEnv(name string, fallback int) (int, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return v, nil
}

func positiveDurationEnv(name string, fallback time.Duration) (time.Duration, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	v, err := time.ParseDuration(raw)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", name)
	}
	return v, nil
}

func authorized(provided, expected string) bool {
	providedHash := sha256.Sum256([]byte(provided))
	expectedHash := sha256.Sum256([]byte(expected))
	return subtle.ConstantTimeCompare(providedHash[:], expectedHash[:]) == 1
}

func apiKeyMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		provided := r.Header.Get("X-API-Key")
		clean := r.Clone(r.Context())
		clean.Header = r.Header.Clone()
		clean.Header.Del("X-API-Key")
		if r.URL.Path == "/events" && r.Method == http.MethodPost && !authorized(provided, apiKey) {
			httpRequests.WithLabelValues("POST", "/events", "401").Inc()
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, clean)
	})
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
		httpRequests.WithLabelValues("other", "/events", "405").Inc()
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !limiter.allow() {
		httpRequests.WithLabelValues("POST", "/events", "429").Inc()
		w.Header().Set("Retry-After", "1")
		http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
		return
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		httpRequests.WithLabelValues("POST", "/events", "415").Inc()
		http.Error(w, "content type must be application/json", http.StatusUnsupportedMediaType)
		return
	}

	var event MatchEvent
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&event); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			httpRequests.WithLabelValues("POST", "/events", "413").Inc()
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		httpRequests.WithLabelValues("POST", "/events", "400").Inc()
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			httpRequests.WithLabelValues("POST", "/events", "413").Inc()
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		httpRequests.WithLabelValues("POST", "/events", "400").Inc()
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if err := validateEvent(event); err != nil {
		httpRequests.WithLabelValues("POST", "/events", "400").Inc()
		http.Error(w, "invalid event", http.StatusBadRequest)
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
		http.Error(w, "invalid event", http.StatusBadRequest)
		return
	}

	if event.Timestamp == "" {
		event.Timestamp = time.Now().UTC().Format(time.RFC3339)
	} else if _, err := time.Parse(time.RFC3339, event.Timestamp); err != nil {
		httpRequests.WithLabelValues("POST", "/events", "400").Inc()
		http.Error(w, "invalid event", http.StatusBadRequest)
		return
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
	boundedCtx, cancel := context.WithTimeout(publishCtx, publishTimeout)
	defer cancel()
	err = pub.Publish(boundedCtx, kafka.Message{
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

func validateEvent(event MatchEvent) error {
	if event.MatchID == "" || len(event.MatchID) > 128 || strings.ContainsAny(event.MatchID, "\r\n") {
		return errors.New("invalid match_id")
	}
	switch event.EventType {
	case "goal", "shot", "foul", "card", "corner":
	default:
		return errors.New("invalid event_type")
	}
	for _, field := range []string{event.Team, event.Player, event.Detail, event.Timestamp} {
		if len(field) > maxStringField || strings.ContainsAny(field, "\r\n") {
			return errors.New("invalid string field")
		}
	}
	if event.Minute < 0 || event.Minute > 120 || event.X < 0 || event.X > 100 || event.Y < 0 || event.Y > 100 {
		return errors.New("invalid numeric field")
	}
	return nil
}
