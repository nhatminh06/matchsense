package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/nhatminh06/matchsense/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

// matchStore is the subset of Redis operations the handlers need, kept as
// an interface so unit tests can substitute a fake and exercise "not
// found" / "Redis unavailable" / corrupt-value paths without a real Redis.
type matchStore interface {
	ActiveMatchIDs(ctx context.Context) ([]string, error)
	Get(ctx context.Context, key string) (string, error)
}

type redisMatchStore struct{ client *redis.Client }

func (s redisMatchStore) ActiveMatchIDs(ctx context.Context) ([]string, error) {
	return s.client.SMembers(ctx, "matches:active").Result()
}

func (s redisMatchStore) Get(ctx context.Context, key string) (string, error) {
	return s.client.Get(ctx, key).Result()
}

var (
	ctx    = context.Background()
	rdb    *redis.Client
	store  matchStore
	tracer = otel.Tracer("query-api")

	httpRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "query_api_http_requests_total", Help: "Total HTTP requests"},
		[]string{"method", "path", "status"},
	)
	queryLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Name: "query_api_query_duration_seconds", Help: "Query latency", Buckets: prometheus.DefBuckets},
		[]string{"endpoint"},
	)
	activeMatches = prometheus.NewGauge(
		prometheus.GaugeOpts{Name: "query_api_active_matches", Help: "Number of active matches"},
	)
)

func init() {
	prometheus.MustRegister(httpRequests, queryLatency, activeMatches)
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
		resource.WithAttributes(semconv.ServiceName("query-api")),
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

	redisAddr := common.GetEnv("REDIS_URL", "localhost:6379")
	port := common.GetEnv("PORT", "8083")

	rdb = redis.NewClient(&redis.Options{Addr: redisAddr})
	store = redisMatchStore{client: rdb}
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Printf("WARNING: Redis not available: %v", err)
	} else {
		log.Println("Connected to Redis")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/matches", matchesHandler)
	mux.HandleFunc("/matches/", matchDetailHandler)
	mux.Handle("/metrics", promhttp.Handler())

	handler := otelhttp.NewHandler(mux, "query-api")

	log.Printf("query-api starting on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, handler))
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "service": "query-api"})
}

func matchesHandler(w http.ResponseWriter, r *http.Request) {
	_, span := tracer.Start(r.Context(), "list-matches")
	defer span.End()

	start := time.Now()
	w.Header().Set("Content-Type", "application/json")

	matchIDs, err := store.ActiveMatchIDs(ctx)
	if err != nil {
		httpRequests.WithLabelValues("GET", "/matches", "500").Inc()
		span.RecordError(err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	activeMatches.Set(float64(len(matchIDs)))
	span.SetAttributes(attribute.Int("matches.count", len(matchIDs)))

	matches := []json.RawMessage{}
	for _, id := range matchIDs {
		data, err := store.Get(ctx, "match:"+id+":stats")
		if err != nil {
			continue
		}
		matches = append(matches, json.RawMessage(data))
	}

	httpRequests.WithLabelValues("GET", "/matches", "200").Inc()
	queryLatency.WithLabelValues("/matches").Observe(time.Since(start).Seconds())
	json.NewEncoder(w).Encode(map[string]interface{}{"count": len(matches), "matches": matches})
}

func matchDetailHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "get-match-detail")
	defer span.End()

	start := time.Now()
	w.Header().Set("Content-Type", "application/json")

	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/matches/"), "/")
	matchID := parts[0]
	span.SetAttributes(attribute.String("match.id", matchID))

	if matchID == "" {
		http.Error(w, "match_id required", http.StatusBadRequest)
		return
	}

	if len(parts) > 1 && parts[1] == "predictions" {
		_, redisSpan := tracer.Start(ctx, "redis-get-predictions")
		data, err := store.Get(ctx, "match:"+matchID+":predictions")
		redisSpan.End()

		if err == redis.Nil {
			json.NewEncoder(w).Encode(map[string]string{"status": "no predictions yet"})
			return
		} else if err != nil {
			span.RecordError(err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		httpRequests.WithLabelValues("GET", "/matches/predictions", "200").Inc()
		queryLatency.WithLabelValues("/matches/predictions").Observe(time.Since(start).Seconds())
		w.Write([]byte(data))
		return
	}

	_, redisSpan := tracer.Start(ctx, "redis-get-stats")
	data, err := store.Get(ctx, "match:"+matchID+":stats")
	redisSpan.End()

	if err == redis.Nil {
		httpRequests.WithLabelValues("GET", "/matches/detail", "404").Inc()
		http.Error(w, "match not found", http.StatusNotFound)
		return
	} else if err != nil {
		span.RecordError(err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	httpRequests.WithLabelValues("GET", "/matches/detail", "200").Inc()
	queryLatency.WithLabelValues("/matches/detail").Observe(time.Since(start).Seconds())
	w.Write([]byte(data))
}
