package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/nhatminh06/matchsense/common"
)

type MatchEvent = common.MatchEvent

var (
	homePlayers = []string{"Saka", "Rice", "Odegaard", "Havertz", "Saliba", "White", "Timber", "Raya", "Trossard", "Martinelli", "Gabriel"}
	awayPlayers = []string{"Haaland", "De Bruyne", "Foden", "Rodri", "Stones", "Walker", "Gvardiol", "Ederson", "Grealish", "Silva", "Doku"}
)

const (
	matchMinutes    = 90
	halfTimeMinute  = 45
	maxSendAttempts = 3
	sendBackoffBase = 200 * time.Millisecond
)

// errPermanent marks a failure that retrying cannot fix.
var errPermanent = errors.New("permanent failure")

// simulator generates a synthetic match. All randomness is drawn from its
// own *rand.Rand rather than the global math/rand source, so a given seed
// always produces the same sequence of events. That determinism is what
// lets a test assert exact statistics instead of guessing at whatever the
// simulator happened to emit.
type simulator struct {
	rng      *rand.Rand
	matchID  string
	homeTeam string
	awayTeam string
	// now and newID are injected so a seeded run is fully reproducible in
	// tests, including the fields that are otherwise time- or
	// crypto-random.
	now   func() time.Time
	newID func() string
}

func newSimulator(seed int64, matchID, homeTeam, awayTeam string) *simulator {
	return &simulator{
		rng:      rand.New(rand.NewSource(seed)),
		matchID:  matchID,
		homeTeam: homeTeam,
		awayTeam: awayTeam,
		now:      time.Now,
		newID:    common.NewEventID,
	}
}

func (s *simulator) pickTeamAndPlayer() (string, string) {
	if s.rng.Float64() < 0.5 {
		return s.homeTeam, homePlayers[s.rng.Intn(len(homePlayers))]
	}
	return s.awayTeam, awayPlayers[s.rng.Intn(len(awayPlayers))]
}

// newEvent builds an event with the fields every event shares already set.
func (s *simulator) newEvent(minute int, eventType, team, player string) MatchEvent {
	return MatchEvent{
		EventID:   s.newID(),
		MatchID:   s.matchID,
		Minute:    minute,
		EventType: eventType,
		Team:      team,
		Player:    player,
		Timestamp: s.now().UTC().Format(time.RFC3339),
	}
}

// generateMinuteEvents returns the events occurring in a single match
// minute. Each event carries its own ID, so a retried send is deduplicated
// downstream rather than double-counted.
func (s *simulator) generateMinuteEvents(minute int) []MatchEvent {
	events := []MatchEvent{}

	// ~30 shots per match = ~0.33 per minute
	if s.rng.Float64() < 0.33 {
		team, player := s.pickTeamAndPlayer()
		onTarget := "off_target"
		if s.rng.Float64() < 0.4 {
			onTarget = "on_target"
		}

		// Shot location: attacking third
		x := 75.0 + s.rng.Float64()*25.0
		y := 20.0 + s.rng.Float64()*60.0
		if team == s.awayTeam {
			x = 100.0 - x
		}

		shot := s.newEvent(minute, "shot", team, player)
		shot.X, shot.Y, shot.Detail = x, y, onTarget
		events = append(events, shot)

		// ~25% of shots on target become goals
		if onTarget == "on_target" && s.rng.Float64() < 0.25 {
			goal := s.newEvent(minute, "goal", team, player)
			goal.X, goal.Y, goal.Detail = x, y, "open_play"
			events = append(events, goal)
		}
	}

	// ~25 fouls per match
	if s.rng.Float64() < 0.28 {
		team, player := s.pickTeamAndPlayer()
		foul := s.newEvent(minute, "foul", team, player)
		foul.X, foul.Y = s.rng.Float64()*100, s.rng.Float64()*100
		events = append(events, foul)

		// ~15% of fouls draw a yellow card
		if s.rng.Float64() < 0.15 {
			card := s.newEvent(minute, "card", team, player)
			card.X, card.Y, card.Detail = s.rng.Float64()*100, s.rng.Float64()*100, "yellow"
			events = append(events, card)
		}
	}

	// ~10 corners per match
	if s.rng.Float64() < 0.11 {
		team, player := s.pickTeamAndPlayer()
		corner := s.newEvent(minute, "corner", team, player)
		corner.X, corner.Y = 100, 0
		events = append(events, corner)
	}

	return events
}

// eventSender posts a single event to event-api.
type eventSender interface {
	Send(ctx context.Context, event MatchEvent) error
}

type httpEventSender struct {
	client *http.Client
	url    string
}

func (h httpEventSender) Send(ctx context.Context, event MatchEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.url+"/events", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := h.client.Do(req)
	if err != nil {
		return fmt.Errorf("post event: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 500 {
		// Server-side failures are worth retrying.
		return fmt.Errorf("event-api returned %d", resp.StatusCode)
	}
	if resp.StatusCode >= 400 {
		// A 4xx means this event will never be accepted; retrying would
		// just spin.
		return fmt.Errorf("%w: event-api returned %d", errPermanent, resp.StatusCode)
	}
	return nil
}

// sendWithRetry retries a send a bounded number of times with exponential
// backoff. It never loops indefinitely: attempts are capped, permanent
// (4xx) failures stop immediately, and a cancelled context aborts at once.
// Because each event carries a stable EventID, a retry whose predecessor
// actually did reach event-api is deduplicated rather than double-counted.
func sendWithRetry(ctx context.Context, sender eventSender, event MatchEvent) error {
	var lastErr error

	for attempt := 1; attempt <= maxSendAttempts; attempt++ {
		err := sender.Send(ctx, event)
		if err == nil {
			return nil
		}
		lastErr = err

		if errors.Is(err, errPermanent) || ctx.Err() != nil {
			return err
		}
		if attempt == maxSendAttempts {
			break
		}

		backoff := sendBackoffBase * time.Duration(1<<(attempt-1))
		if !sleepCtx(ctx, backoff) {
			return ctx.Err()
		}
	}

	return fmt.Errorf("giving up after %d attempts: %w", maxSendAttempts, lastErr)
}

// run plays the match through, respecting context cancellation so the
// process shuts down promptly instead of finishing all 90 minutes.
func (s *simulator) run(ctx context.Context, sender eventSender, minuteDelay time.Duration) {
	for minute := 1; minute <= matchMinutes; minute++ {
		if ctx.Err() != nil {
			log.Printf("simulation cancelled at minute %d", minute)
			return
		}

		for _, event := range s.generateMinuteEvents(minute) {
			if err := sendWithRetry(ctx, sender, event); err != nil {
				if ctx.Err() != nil {
					return
				}
				// One undeliverable event must not abort the match.
				log.Printf("ERROR sending event %s: %v", event.EventID, err)
				continue
			}
			log.Printf("min:%d %s %s - %s (%s)",
				event.Minute, event.EventType, event.Team, event.Player, event.Detail)
		}

		if minute == halfTimeMinute {
			log.Println("=== HALF TIME ===")
			if !sleepCtx(ctx, minuteDelay) {
				return
			}
		}

		if !sleepCtx(ctx, minuteDelay) {
			return
		}
	}

	log.Println("=== FULL TIME ===")
}

// sleepCtx waits for d, returning false if the context was cancelled first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// resolveMinuteDelay picks the per-minute delay from configuration.
// SIMULATOR_EVENT_INTERVAL supersedes SPEED; SPEED is still honoured so
// existing Compose/Kubernetes configuration keeps working unchanged.
func resolveMinuteDelay(speed, interval string) time.Duration {
	delay := 2 * time.Second

	if speed != "" {
		if secs, err := strconv.Atoi(speed); err == nil && secs >= 0 {
			delay = time.Duration(secs) * time.Second
		} else {
			log.Printf("WARNING: invalid SPEED %q, using %s", speed, delay)
		}
	}
	if interval != "" {
		if d, err := time.ParseDuration(interval); err == nil && d >= 0 {
			delay = d
		} else {
			log.Printf("WARNING: invalid SIMULATOR_EVENT_INTERVAL %q, using %s", interval, delay)
		}
	}
	return delay
}

// resolveSeed returns the seed to use and whether it came from
// configuration. An unset seed keeps the historical behaviour of a
// different match every run.
func resolveSeed(raw string) (seed int64, seeded bool) {
	if raw == "" {
		return time.Now().UnixNano(), false
	}
	parsed, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		log.Printf("WARNING: invalid SIMULATOR_SEED %q, using a time-based seed", raw)
		return time.Now().UnixNano(), false
	}
	return parsed, true
}

func main() {
	eventAPIURL := common.GetEnv("EVENT_API_URL", "http://localhost:8080")
	matchID := common.GetEnv("SIMULATOR_MATCH_ID", common.GetEnv("MATCH_ID", fmt.Sprintf("match_%d", time.Now().Unix())))
	homeTeam := common.GetEnv("SIMULATOR_HOME_TEAM", common.GetEnv("HOME_TEAM", "Arsenal"))
	awayTeam := common.GetEnv("SIMULATOR_AWAY_TEAM", common.GetEnv("AWAY_TEAM", "Manchester City"))
	healthPort := common.GetEnv("HEALTH_PORT", "8082")

	minuteDelay := resolveMinuteDelay(common.GetEnv("SPEED", ""), common.GetEnv("SIMULATOR_EVENT_INTERVAL", ""))
	seed, seeded := resolveSeed(common.GetEnv("SIMULATOR_SEED", ""))
	autoStart := common.GetEnv("SIMULATOR_AUTO_START", "true") != "false"

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "service": "match-simulator"})
	})
	srv := &http.Server{Addr: ":" + healthPort, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("WARNING: health server stopped: %v", err)
		}
	}()

	if !autoStart {
		// Used by the end-to-end tests, which submit their own
		// deterministic events and must not race a background match.
		log.Println("SIMULATOR_AUTO_START=false — serving /health only, not simulating")
		<-ctx.Done()
		shutdownServer(srv)
		return
	}

	log.Printf("Simulating: %s vs %s (match: %s)", homeTeam, awayTeam, matchID)
	if seeded {
		log.Printf("Seed: %d (reproducible)", seed)
	} else {
		log.Printf("Seed: %d (time-based; set SIMULATOR_SEED to reproduce)", seed)
	}
	log.Printf("Minute interval: %s", minuteDelay)
	log.Printf("Event API: %s", eventAPIURL)

	sender := httpEventSender{
		client: &http.Client{Timeout: 5 * time.Second},
		url:    eventAPIURL,
	}
	newSimulator(seed, matchID, homeTeam, awayTeam).run(ctx, sender, minuteDelay)

	stop()
	shutdownServer(srv)
}

func shutdownServer(srv *http.Server) {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("WARNING: health server shutdown: %v", err)
	}
}
