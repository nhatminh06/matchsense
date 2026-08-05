package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/nhatminh06/matchsense/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

type MatchEvent = common.MatchEvent

type MatchStats struct {
	MatchID     string       `json:"match_id"`
	Minute      int          `json:"minute"`
	HomeTeam    string       `json:"home_team"`
	AwayTeam    string       `json:"away_team"`
	HomeGoals   int          `json:"home_goals"`
	AwayGoals   int          `json:"away_goals"`
	HomeShots   int          `json:"home_shots"`
	AwayShots   int          `json:"away_shots"`
	HomeShotsOT int          `json:"home_shots_on_target"`
	AwayShotsOT int          `json:"away_shots_on_target"`
	HomeFouls   int          `json:"home_fouls"`
	AwayFouls   int          `json:"away_fouls"`
	HomeCorners int          `json:"home_corners"`
	AwayCorners int          `json:"away_corners"`
	HomeYellow  int          `json:"home_yellow_cards"`
	AwayYellow  int          `json:"away_yellow_cards"`
	HomeRed     int          `json:"home_red_cards"`
	AwayRed     int          `json:"away_red_cards"`
	Events      []MatchEvent `json:"events"`
	LastEvent   MatchEvent   `json:"last_event"`
	UpdatedAt   string       `json:"updated_at"`
}

// statsStore is the subset of Redis operations processEvent needs. Kept as
// an interface (rather than a concrete *redis.Client) so unit tests can
// substitute a fake and exercise write-failure paths without a real Redis.
type statsStore interface {
	SetStats(ctx context.Context, key string, data []byte) error
	AddActiveMatch(ctx context.Context, matchID string) error
}

// statsPublisher is the subset of the Kafka writer processEvent needs, for
// the same reason as statsStore above.
type statsPublisher interface {
	Publish(ctx context.Context, msg kafka.Message) error
}

type redisStatsStore struct{ client *redis.Client }

func (s redisStatsStore) SetStats(ctx context.Context, key string, data []byte) error {
	return s.client.Set(ctx, key, data, 0).Err()
}

func (s redisStatsStore) AddActiveMatch(ctx context.Context, matchID string) error {
	return s.client.SAdd(ctx, "matches:active", matchID).Err()
}

type kafkaStatsPublisher struct{ writer *kafka.Writer }

func (p kafkaStatsPublisher) Publish(ctx context.Context, msg kafka.Message) error {
	return p.writer.WriteMessages(ctx, msg)
}

var (
	ctx     = context.Background()
	rdb     *redis.Client
	store   statsStore
	writer  *kafka.Writer
	pub     statsPublisher
	statsMu sync.Mutex
	stats   = make(map[string]*MatchStats)
	tracer  = otel.Tracer("event-processor")

	eventsProcessed = prometheus.NewCounter(
		prometheus.CounterOpts{Name: "event_processor_events_processed_total", Help: "Total events processed"},
	)
	kafkaReadErrors = prometheus.NewCounter(
		prometheus.CounterOpts{Name: "event_processor_kafka_read_errors_total", Help: "Total errors reading from Kafka"},
	)
	kafkaWriteErrors = prometheus.NewCounter(
		prometheus.CounterOpts{Name: "event_processor_kafka_write_errors_total", Help: "Total errors publishing stats to Kafka"},
	)
	redisWriteErrors = prometheus.NewCounter(
		prometheus.CounterOpts{Name: "event_processor_redis_write_errors_total", Help: "Total errors writing stats to Redis"},
	)
)

func init() {
	prometheus.MustRegister(eventsProcessed, kafkaReadErrors, kafkaWriteErrors, redisWriteErrors)
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
		resource.WithAttributes(semconv.ServiceName("event-processor")),
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

// loadActiveMatchesFromRedis rehydrates the in-memory stats map from the
// last snapshot each active match had in Redis. Without this, a restart of
// this service mid-match would silently reset every running match's score,
// shots, cards, etc. back to zero even though Kafka would resume consuming
// from the correct offset.
func loadActiveMatchesFromRedis() {
	matchIDs, err := rdb.SMembers(ctx, "matches:active").Result()
	if err != nil {
		log.Printf("WARNING: could not load active matches from Redis: %v", err)
		return
	}

	restored := 0
	for _, id := range matchIDs {
		data, err := rdb.Get(ctx, fmt.Sprintf("match:%s:stats", id)).Result()
		if err != nil {
			log.Printf("WARNING: could not load stats for match %s: %v", id, err)
			continue
		}

		var matchStats MatchStats
		if err := json.Unmarshal([]byte(data), &matchStats); err != nil {
			log.Printf("WARNING: could not parse stats for match %s: %v", id, err)
			continue
		}

		stats[id] = &matchStats
		restored++
	}

	if restored > 0 {
		log.Printf("Restored in-memory state for %d active match(es) from Redis", restored)
	}
}

func main() {
	shutdown := initTracer()
	defer shutdown()

	kafkaBroker := common.GetEnv("KAFKA_BROKER", "localhost:9092")
	redisAddr := common.GetEnv("REDIS_URL", "localhost:6379")
	metricsPort := common.GetEnv("METRICS_PORT", "8081")

	rdb = redis.NewClient(&redis.Options{Addr: redisAddr})
	store = redisStatsStore{client: rdb}
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Printf("WARNING: Redis not available: %v", err)
	} else {
		log.Println("Connected to Redis")
		loadActiveMatchesFromRedis()
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{kafkaBroker},
		Topic:          "match-events",
		GroupID:        "event-processor",
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
	})
	defer reader.Close()

	writer = &kafka.Writer{
		Addr:         kafka.TCP(kafkaBroker),
		Topic:        "match-stats",
		Balancer:     &kafka.RoundRobin{},
		BatchTimeout: 10 * time.Millisecond,
	}
	pub = kafkaStatsPublisher{writer: writer}
	defer writer.Close()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "service": "event-processor"})
	})
	mux.Handle("/metrics", promhttp.Handler())
	go func() {
		if err := http.ListenAndServe(":"+metricsPort, mux); err != nil {
			log.Printf("WARNING: metrics server stopped: %v", err)
		}
	}()

	log.Printf("event-processor starting (kafka: %s, redis: %s, metrics: :%s)", kafkaBroker, redisAddr, metricsPort)

	go func() {
		readBackoff := time.Second
		const maxReadBackoff = 30 * time.Second

		for {
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				kafkaReadErrors.Inc()
				log.Printf("ERROR reading: %v (retrying in %s)", err, readBackoff)
				time.Sleep(readBackoff)
				readBackoff *= 2
				if readBackoff > maxReadBackoff {
					readBackoff = maxReadBackoff
				}
				continue
			}
			readBackoff = time.Second

			var event MatchEvent
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				log.Printf("ERROR parsing: %v", err)
				continue
			}

			// Extract trace context from Kafka headers
			headers := msg.Headers
			extractedCtx := otel.GetTextMapPropagator().Extract(ctx, common.KafkaHeaderCarrier{Headers: &headers})

			processEvent(extractedCtx, event)
		}
	}()

	<-sigChan
	log.Println("Shutting down...")
}

// applyEvent mutates matchStats in place to reflect event, and reports
// whether the event was attributed to the home team. It has no I/O, so it
// can be unit-tested directly without a mutex, Redis, or Kafka.
//
// Note on delivery semantics: this function is purely additive — it does
// not check event IDs or deduplicate. If the same event is delivered twice
// (e.g. an at-least-once Kafka redelivery after a consumer restart before
// the offset commit), it will be counted twice. There is currently no
// idempotency guarantee anywhere in this pipeline.
func applyEvent(matchStats *MatchStats, event MatchEvent) (isHome bool) {
	// Only learn home/away identity from events that actually carry a team;
	// otherwise an empty-team event (e.g. a whistle/foul with no team set)
	// would permanently lock HomeTeam to "", breaking isHome forever after.
	if event.Team != "" {
		if matchStats.HomeTeam == "" {
			matchStats.HomeTeam = event.Team
		} else if matchStats.AwayTeam == "" && event.Team != matchStats.HomeTeam {
			matchStats.AwayTeam = event.Team
		}
	}

	isHome = event.Team != "" && event.Team == matchStats.HomeTeam

	switch event.EventType {
	case "goal":
		if isHome {
			matchStats.HomeGoals++
		} else {
			matchStats.AwayGoals++
		}
	case "shot":
		if isHome {
			matchStats.HomeShots++
			if event.Detail == "on_target" {
				matchStats.HomeShotsOT++
			}
		} else {
			matchStats.AwayShots++
			if event.Detail == "on_target" {
				matchStats.AwayShotsOT++
			}
		}
	case "foul":
		if isHome {
			matchStats.HomeFouls++
		} else {
			matchStats.AwayFouls++
		}
	case "corner":
		if isHome {
			matchStats.HomeCorners++
		} else {
			matchStats.AwayCorners++
		}
	case "card":
		if event.Detail == "yellow" {
			if isHome {
				matchStats.HomeYellow++
			} else {
				matchStats.AwayYellow++
			}
		} else if event.Detail == "red" {
			if isHome {
				matchStats.HomeRed++
			} else {
				matchStats.AwayRed++
			}
		}
	default:
		// Unknown event types (and events without a recognized EventType)
		// still update minute/last-event/history below, they just don't
		// bump any per-type counter.
	}

	matchStats.Minute = event.Minute
	matchStats.LastEvent = event
	matchStats.Events = append(matchStats.Events, event)
	matchStats.UpdatedAt = time.Now().UTC().Format(time.RFC3339)

	return isHome
}

func processEvent(parentCtx context.Context, event MatchEvent) {
	spanCtx, span := tracer.Start(parentCtx, "process-match-event")
	defer span.End()

	span.SetAttributes(
		attribute.String("match.id", event.MatchID),
		attribute.String("event.type", event.EventType),
		attribute.Int("event.minute", event.Minute),
	)

	statsMu.Lock()

	matchStats, exists := stats[event.MatchID]
	if !exists {
		matchStats = &MatchStats{MatchID: event.MatchID, Events: []MatchEvent{}}
		stats[event.MatchID] = matchStats
	}

	applyEvent(matchStats, event)

	statsSnapshot := *matchStats
	statsMu.Unlock()

	eventsProcessed.Inc()

	data, err := json.Marshal(statsSnapshot)
	if err != nil {
		span.RecordError(err)
		log.Printf("ERROR: failed to marshal stats for match %s: %v", event.MatchID, err)
		return
	}

	_, redisSpan := tracer.Start(spanCtx, "redis-write-stats")
	if err := store.SetStats(ctx, fmt.Sprintf("match:%s:stats", event.MatchID), data); err != nil {
		redisWriteErrors.Inc()
		log.Printf("ERROR: redis write (stats) failed for match %s: %v", event.MatchID, err)
	}
	if err := store.SetStats(ctx, fmt.Sprintf("match:%s:latest", event.MatchID), data); err != nil {
		redisWriteErrors.Inc()
		log.Printf("ERROR: redis write (latest) failed for match %s: %v", event.MatchID, err)
	}
	if err := store.AddActiveMatch(ctx, event.MatchID); err != nil {
		redisWriteErrors.Inc()
		log.Printf("ERROR: redis SAdd failed for match %s: %v", event.MatchID, err)
	}
	redisSpan.End()

	publishCtx, kafkaSpan := tracer.Start(spanCtx, "kafka-publish-stats")

	var outHeaders []kafka.Header
	otel.GetTextMapPropagator().Inject(publishCtx, common.KafkaHeaderCarrier{Headers: &outHeaders})

	if err := pub.Publish(ctx, kafka.Message{
		Key:     []byte(event.MatchID),
		Value:   data,
		Headers: outHeaders,
	}); err != nil {
		kafkaWriteErrors.Inc()
		span.RecordError(err)
		log.Printf("ERROR: kafka publish failed for match %s: %v", event.MatchID, err)
	}
	kafkaSpan.End()

	log.Printf("[%s] min:%d %s %s | Score: %s %d - %d %s",
		event.MatchID, event.Minute, event.EventType, event.Player,
		statsSnapshot.HomeTeam, statsSnapshot.HomeGoals, statsSnapshot.AwayGoals, statsSnapshot.AwayTeam)
}
