package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

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

var (
	ctx    = context.Background()
	rdb    *redis.Client
	writer *kafka.Writer
	stats  = make(map[string]*MatchStats)
	tracer = otel.Tracer("event-processor")
)

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
	redisAddr := getEnv("REDIS_URL", "localhost:6379")

	rdb = redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Printf("WARNING: Redis not available: %v", err)
	} else {
		log.Println("Connected to Redis")
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
	defer writer.Close()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("event-processor starting (kafka: %s, redis: %s)", kafkaBroker, redisAddr)

	go func() {
		for {
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				log.Printf("ERROR reading: %v", err)
				continue
			}

			var event MatchEvent
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				log.Printf("ERROR parsing: %v", err)
				continue
			}

			// Extract trace context from Kafka headers
			headers := msg.Headers
			extractedCtx := otel.GetTextMapPropagator().Extract(ctx, kafkaHeaderCarrier{headers: &headers})

			processEvent(extractedCtx, event)
		}
	}()

	<-sigChan
	log.Println("Shutting down...")
}

func processEvent(parentCtx context.Context, event MatchEvent) {
	spanCtx, span := tracer.Start(parentCtx, "process-match-event")
	defer span.End()

	span.SetAttributes(
		attribute.String("match.id", event.MatchID),
		attribute.String("event.type", event.EventType),
		attribute.Int("event.minute", event.Minute),
	)

	matchStats, exists := stats[event.MatchID]
	if !exists {
		matchStats = &MatchStats{MatchID: event.MatchID, Events: []MatchEvent{}}
		stats[event.MatchID] = matchStats
	}

	if matchStats.HomeTeam == "" {
		matchStats.HomeTeam = event.Team
	} else if matchStats.AwayTeam == "" && event.Team != matchStats.HomeTeam {
		matchStats.AwayTeam = event.Team
	}

	isHome := event.Team == matchStats.HomeTeam

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
	}

	matchStats.Minute = event.Minute
	matchStats.LastEvent = event
	matchStats.Events = append(matchStats.Events, event)
	matchStats.UpdatedAt = time.Now().UTC().Format(time.RFC3339)

	data, _ := json.Marshal(matchStats)

	_, redisSpan := tracer.Start(spanCtx, "redis-write-stats")
	rdb.Set(ctx, fmt.Sprintf("match:%s:stats", event.MatchID), data, 0)
	rdb.Set(ctx, fmt.Sprintf("match:%s:latest", event.MatchID), data, 0)
	rdb.SAdd(ctx, "matches:active", event.MatchID)
	redisSpan.End()

	publishCtx, kafkaSpan := tracer.Start(spanCtx, "kafka-publish-stats")

	var outHeaders []kafka.Header
	otel.GetTextMapPropagator().Inject(publishCtx, kafkaHeaderCarrier{headers: &outHeaders})

	writer.WriteMessages(ctx, kafka.Message{
		Key:     []byte(event.MatchID),
		Value:   data,
		Headers: outHeaders,
	})
	kafkaSpan.End()

	log.Printf("[%s] min:%d %s %s | Score: %s %d - %d %s",
		event.MatchID, event.Minute, event.EventType, event.Player,
		matchStats.HomeTeam, matchStats.HomeGoals, matchStats.AwayGoals, matchStats.AwayTeam)
}
