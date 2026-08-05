package main

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/segmentio/kafka-go"
)

func newStats(matchID string) *MatchStats {
	return &MatchStats{MatchID: matchID, Events: []MatchEvent{}}
}

func TestApplyEvent_GoalAggregation(t *testing.T) {
	stats := newStats("m1")
	applyEvent(stats, MatchEvent{MatchID: "m1", Team: "Arsenal", EventType: "goal", Minute: 10})
	applyEvent(stats, MatchEvent{MatchID: "m1", Team: "Man City", EventType: "goal", Minute: 20})
	applyEvent(stats, MatchEvent{MatchID: "m1", Team: "Arsenal", EventType: "goal", Minute: 30})

	if stats.HomeTeam != "Arsenal" || stats.AwayTeam != "Man City" {
		t.Fatalf("home/away team detection wrong: home=%q away=%q", stats.HomeTeam, stats.AwayTeam)
	}
	if stats.HomeGoals != 2 || stats.AwayGoals != 1 {
		t.Fatalf("goal counts wrong: home=%d away=%d", stats.HomeGoals, stats.AwayGoals)
	}
}

func TestApplyEvent_ShotAndOnTarget(t *testing.T) {
	stats := newStats("m1")
	applyEvent(stats, MatchEvent{Team: "Arsenal", EventType: "shot", Detail: "on_target"})
	applyEvent(stats, MatchEvent{Team: "Arsenal", EventType: "shot", Detail: "off_target"})
	applyEvent(stats, MatchEvent{Team: "Man City", EventType: "shot", Detail: "on_target"})

	if stats.HomeShots != 2 || stats.HomeShotsOT != 1 {
		t.Fatalf("home shots wrong: shots=%d onTarget=%d", stats.HomeShots, stats.HomeShotsOT)
	}
	if stats.AwayShots != 1 || stats.AwayShotsOT != 1 {
		t.Fatalf("away shots wrong: shots=%d onTarget=%d", stats.AwayShots, stats.AwayShotsOT)
	}
}

func TestApplyEvent_CornersFoulsCards(t *testing.T) {
	stats := newStats("m1")
	applyEvent(stats, MatchEvent{Team: "Arsenal", EventType: "corner"})
	applyEvent(stats, MatchEvent{Team: "Man City", EventType: "corner"})
	applyEvent(stats, MatchEvent{Team: "Arsenal", EventType: "foul"})
	applyEvent(stats, MatchEvent{Team: "Arsenal", EventType: "card", Detail: "yellow"})
	applyEvent(stats, MatchEvent{Team: "Man City", EventType: "card", Detail: "red"})

	if stats.HomeCorners != 1 || stats.AwayCorners != 1 {
		t.Fatalf("corner counts wrong: home=%d away=%d", stats.HomeCorners, stats.AwayCorners)
	}
	if stats.HomeFouls != 1 {
		t.Fatalf("home fouls wrong: got %d", stats.HomeFouls)
	}
	if stats.HomeYellow != 1 {
		t.Fatalf("home yellow cards wrong: got %d", stats.HomeYellow)
	}
	if stats.AwayRed != 1 {
		t.Fatalf("away red cards wrong: got %d", stats.AwayRed)
	}
}

func TestApplyEvent_UnknownEventTypeDoesNotPanicOrMiscount(t *testing.T) {
	stats := newStats("m1")
	applyEvent(stats, MatchEvent{Team: "Arsenal", EventType: "var_review", Minute: 5})

	if stats.HomeGoals != 0 || stats.HomeShots != 0 || stats.HomeFouls != 0 {
		t.Fatalf("unknown event type should not increment any counter, got %+v", stats)
	}
	if stats.Minute != 5 || len(stats.Events) != 1 {
		t.Fatalf("unknown event type should still update minute/history, got minute=%d events=%d", stats.Minute, len(stats.Events))
	}
}

func TestApplyEvent_EmptyTeamDoesNotCorruptHomeAwayDetection(t *testing.T) {
	stats := newStats("m1")
	// A team-less event (e.g. a stoppage/whistle) arrives first.
	applyEvent(stats, MatchEvent{EventType: "foul", Minute: 1})
	if stats.HomeTeam != "" {
		t.Fatalf("empty-team event should not set HomeTeam, got %q", stats.HomeTeam)
	}

	// The real teams should still be learned correctly afterward.
	applyEvent(stats, MatchEvent{Team: "Arsenal", EventType: "goal"})
	applyEvent(stats, MatchEvent{Team: "Man City", EventType: "goal"})

	if stats.HomeTeam != "Arsenal" || stats.AwayTeam != "Man City" {
		t.Fatalf("home/away detection corrupted by empty-team event: home=%q away=%q", stats.HomeTeam, stats.AwayTeam)
	}
	if stats.HomeGoals != 1 || stats.AwayGoals != 1 {
		t.Fatalf("goal counts wrong after empty-team event: home=%d away=%d", stats.HomeGoals, stats.AwayGoals)
	}
}

// TestApplyEvent_DuplicateEventIsCountedTwice documents the actual
// delivery/dedup behavior of this pipeline: applyEvent has no concept of
// event identity, so replaying the same event increments counters again.
// This is a real, current gap (no idempotency), not a guarantee — see the
// applyEvent doc comment.
func TestApplyEvent_DuplicateEventIsCountedTwice(t *testing.T) {
	stats := newStats("m1")
	goal := MatchEvent{MatchID: "m1", Team: "Arsenal", EventType: "goal", Minute: 10, Player: "Saka"}

	applyEvent(stats, goal)
	applyEvent(stats, goal) // same event redelivered

	if stats.HomeGoals != 2 {
		t.Fatalf("expected duplicate delivery to be double-counted (no dedup exists), got HomeGoals=%d", stats.HomeGoals)
	}
}

func TestApplyEvent_OutOfOrderEventsOverwriteMinuteForward(t *testing.T) {
	stats := newStats("m1")
	applyEvent(stats, MatchEvent{Team: "Arsenal", EventType: "goal", Minute: 50})
	applyEvent(stats, MatchEvent{Team: "Arsenal", EventType: "shot", Minute: 10}) // arrives out of order

	// applyEvent does not reorder or reject late events; Minute simply
	// reflects whatever event was processed most recently.
	if stats.Minute != 10 {
		t.Fatalf("expected Minute to reflect the last-processed event (10), got %d", stats.Minute)
	}
	if stats.HomeGoals != 1 || stats.HomeShots != 1 {
		t.Fatalf("both events should still be aggregated regardless of order: goals=%d shots=%d", stats.HomeGoals, stats.HomeShots)
	}
}

// --- processEvent tests using fakes for statsStore / statsPublisher ---

type fakeStatsStore struct {
	mu        sync.Mutex
	setCalls  int
	saddCalls int
	setErr    error
	saddErr   error
}

func (f *fakeStatsStore) SetStats(_ context.Context, _ string, _ []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.setCalls++
	return f.setErr
}

func (f *fakeStatsStore) AddActiveMatch(_ context.Context, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.saddCalls++
	return f.saddErr
}

type fakeStatsPublisher struct {
	mu        sync.Mutex
	published []kafka.Message
	err       error
}

func (f *fakeStatsPublisher) Publish(_ context.Context, msg kafka.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.published = append(f.published, msg)
	return nil
}

// resetGlobalState resets the package-level maps/wiring processEvent
// depends on, so tests don't leak state into each other. This mirrors what
// main() does at startup, minus the real Redis/Kafka connections.
func resetGlobalState(t *testing.T, s statsStore, p statsPublisher) {
	t.Helper()
	statsMu.Lock()
	stats = make(map[string]*MatchStats)
	statsMu.Unlock()
	store = s
	pub = p
}

func TestProcessEvent_PublishesAggregatedStats(t *testing.T) {
	fakeStore := &fakeStatsStore{}
	fakePub := &fakeStatsPublisher{}
	resetGlobalState(t, fakeStore, fakePub)

	processEvent(context.Background(), MatchEvent{MatchID: "m1", Team: "Arsenal", EventType: "goal", Minute: 12})

	if fakeStore.setCalls != 2 { // :stats and :latest
		t.Fatalf("expected 2 Redis Set calls, got %d", fakeStore.setCalls)
	}
	if fakeStore.saddCalls != 1 {
		t.Fatalf("expected 1 Redis SAdd call, got %d", fakeStore.saddCalls)
	}
	if len(fakePub.published) != 1 {
		t.Fatalf("expected 1 Kafka publish, got %d", len(fakePub.published))
	}

	statsMu.Lock()
	got := stats["m1"]
	statsMu.Unlock()
	if got == nil || got.HomeGoals != 1 {
		t.Fatalf("expected in-memory stats to record the goal, got %+v", got)
	}
}

func TestProcessEvent_RedisWriteFailureDoesNotBlockKafkaPublish(t *testing.T) {
	fakeStore := &fakeStatsStore{setErr: errors.New("redis unavailable")}
	fakePub := &fakeStatsPublisher{}
	resetGlobalState(t, fakeStore, fakePub)

	processEvent(context.Background(), MatchEvent{MatchID: "m1", Team: "Arsenal", EventType: "shot"})

	// A Redis outage is logged/counted (see redisWriteErrors in main.go) but
	// must not prevent event-processor from still publishing stats to Kafka.
	if len(fakePub.published) != 1 {
		t.Fatalf("expected Kafka publish to still happen despite Redis failure, got %d publishes", len(fakePub.published))
	}
}

func TestProcessEvent_KafkaPublishFailureDoesNotPanic(t *testing.T) {
	fakeStore := &fakeStatsStore{}
	fakePub := &fakeStatsPublisher{err: errors.New("kafka unavailable")}
	resetGlobalState(t, fakeStore, fakePub)

	processEvent(context.Background(), MatchEvent{MatchID: "m1", Team: "Arsenal", EventType: "shot"})

	if fakeStore.setCalls == 0 {
		t.Fatalf("expected Redis writes to still be attempted despite Kafka failure")
	}
}

func TestProcessEvent_MalformedEventStillTracksByMatchID(t *testing.T) {
	fakeStore := &fakeStatsStore{}
	fakePub := &fakeStatsPublisher{}
	resetGlobalState(t, fakeStore, fakePub)

	// An event with no MatchID is a malformed/degenerate case: it's
	// tracked under the empty-string key rather than rejected, since
	// processEvent (by design, upstream of this refactor) does no
	// validation — that happens in event-api, not here.
	processEvent(context.Background(), MatchEvent{EventType: "goal", Team: "Arsenal"})

	statsMu.Lock()
	_, exists := stats[""]
	statsMu.Unlock()
	if !exists {
		t.Fatalf("expected an event with empty MatchID to be tracked under the empty-string key")
	}
}
