package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nhatminh06/matchsense/common"
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

// TestApplyEvent_DuplicateEventIsCountedTwice pins down the deliberate
// division of responsibility: applyEvent is the pure aggregation step and
// has no concept of event identity, so calling it twice counts twice.
// Deduplication lives one level up in processEvent, which gates on the
// event ID before ever reaching applyEvent — see
// TestProcessEvent_DuplicateEventIsNotDoubleCounted.
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

// fakeStatsStore is an in-memory stand-in for Redis. The reservation
// methods model the same state machine as redisStatsStore (see
// common/dedupe.go) so deduplication behaviour can be tested without a
// real Redis instance.
type fakeStatsStore struct {
	mu        sync.Mutex
	setCalls  int
	saddCalls int
	setErr    error
	saddErr   error

	reservations map[string]string // eventID -> "processing" | "processed"
	reserveErr   error
	markErr      error
	reserveCalls int
	markCalls    int
	releaseCalls int
}

func (f *fakeStatsStore) ReserveEvent(_ context.Context, eventID string, _ time.Duration) (common.ReservationState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reserveCalls++
	if f.reserveErr != nil {
		return common.ReservationAcquired, f.reserveErr
	}
	if f.reservations == nil {
		f.reservations = map[string]string{}
	}
	switch f.reservations[eventID] {
	case "":
		f.reservations[eventID] = reservationProcessing
		return common.ReservationAcquired, nil
	case reservationProcessed:
		return common.ReservationAlreadyProcessed, nil
	default:
		return common.ReservationInFlight, nil
	}
}

func (f *fakeStatsStore) MarkProcessed(_ context.Context, eventID string, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.markCalls++
	if f.markErr != nil {
		return f.markErr
	}
	if f.reservations == nil {
		f.reservations = map[string]string{}
	}
	f.reservations[eventID] = reservationProcessed
	return nil
}

func (f *fakeStatsStore) ReleaseReservation(_ context.Context, eventID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.releaseCalls++
	delete(f.reservations, eventID)
	return nil
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

// --- deduplication / idempotency ---

func TestProcessEvent_DuplicateEventIsNotDoubleCounted(t *testing.T) {
	fakeStore := &fakeStatsStore{}
	fakePub := &fakeStatsPublisher{}
	resetGlobalState(t, fakeStore, fakePub)

	goal := MatchEvent{EventID: "evt-1", MatchID: "m1", Team: "Arsenal", EventType: "goal", Minute: 10}

	processEvent(context.Background(), goal)
	processEvent(context.Background(), goal) // Kafka redelivery of the same event

	statsMu.Lock()
	got := stats["m1"]
	statsMu.Unlock()

	if got.HomeGoals != 1 {
		t.Fatalf("expected the redelivered goal to be counted once, got HomeGoals=%d", got.HomeGoals)
	}
	if len(fakePub.published) != 1 {
		t.Fatalf("expected only the first delivery to publish stats, got %d publishes", len(fakePub.published))
	}
}

func TestProcessEvent_DistinctEventIDsAreBothCounted(t *testing.T) {
	fakeStore := &fakeStatsStore{}
	fakePub := &fakeStatsPublisher{}
	resetGlobalState(t, fakeStore, fakePub)

	// Two genuinely different goals that happen to look otherwise
	// identical must both count — dedup keys on event ID, not content.
	processEvent(context.Background(), MatchEvent{EventID: "evt-1", MatchID: "m1", Team: "Arsenal", EventType: "goal", Minute: 10})
	processEvent(context.Background(), MatchEvent{EventID: "evt-2", MatchID: "m1", Team: "Arsenal", EventType: "goal", Minute: 10})

	statsMu.Lock()
	got := stats["m1"]
	statsMu.Unlock()

	if got.HomeGoals != 2 {
		t.Fatalf("expected two distinct events to both count, got HomeGoals=%d", got.HomeGoals)
	}
}

func TestProcessEvent_SameEventIDAcrossDifferentMatchesIsStillDeduplicated(t *testing.T) {
	fakeStore := &fakeStatsStore{}
	fakePub := &fakeStatsPublisher{}
	resetGlobalState(t, fakeStore, fakePub)

	// Event IDs are globally unique by construction (see common.NewEventID),
	// so the dedup key is not namespaced by match. This test documents that
	// choice: reusing an ID across matches is a producer bug, and the second
	// event is suppressed rather than silently corrupting a second match.
	processEvent(context.Background(), MatchEvent{EventID: "evt-shared", MatchID: "m1", Team: "Arsenal", EventType: "goal"})
	processEvent(context.Background(), MatchEvent{EventID: "evt-shared", MatchID: "m2", Team: "Chelsea", EventType: "goal"})

	statsMu.Lock()
	m1, m2 := stats["m1"], stats["m2"]
	statsMu.Unlock()

	if m1 == nil || m1.HomeGoals != 1 {
		t.Fatalf("first match should have counted its goal, got %+v", m1)
	}
	if m2 != nil {
		t.Fatalf("second match reused an event ID and must be suppressed, got %+v", m2)
	}
}

func TestProcessEvent_EventWithoutIDIsProcessedWithoutDeduplication(t *testing.T) {
	fakeStore := &fakeStatsStore{}
	fakePub := &fakeStatsPublisher{}
	resetGlobalState(t, fakeStore, fakePub)

	// Backward compatibility: a producer that predates event_id must not
	// have its events dropped. They are processed, just not deduplicated.
	noID := MatchEvent{MatchID: "m1", Team: "Arsenal", EventType: "goal"}
	processEvent(context.Background(), noID)
	processEvent(context.Background(), noID)

	statsMu.Lock()
	got := stats["m1"]
	statsMu.Unlock()

	if got.HomeGoals != 2 {
		t.Fatalf("events without an ID cannot be deduplicated and should both count, got %d", got.HomeGoals)
	}
	if fakeStore.reserveCalls != 0 {
		t.Fatalf("expected no reservation attempts for events without an ID, got %d", fakeStore.reserveCalls)
	}
}

func TestProcessEvent_DedupeStoreFailureFailsOpen(t *testing.T) {
	fakeStore := &fakeStatsStore{reserveErr: errors.New("redis unavailable")}
	fakePub := &fakeStatsPublisher{}
	resetGlobalState(t, fakeStore, fakePub)

	processEvent(context.Background(), MatchEvent{EventID: "evt-1", MatchID: "m1", Team: "Arsenal", EventType: "goal"})

	statsMu.Lock()
	got := stats["m1"]
	statsMu.Unlock()

	// Fail-open: losing an event outright is worse than risking a
	// double-count during a Redis outage, so the event is still processed.
	if got == nil || got.HomeGoals != 1 {
		t.Fatalf("expected the event to be processed despite a dedup store failure, got %+v", got)
	}
}

func TestProcessEvent_MarkProcessedFailureDoesNotLoseTheEvent(t *testing.T) {
	fakeStore := &fakeStatsStore{markErr: errors.New("redis unavailable")}
	fakePub := &fakeStatsPublisher{}
	resetGlobalState(t, fakeStore, fakePub)

	processEvent(context.Background(), MatchEvent{EventID: "evt-1", MatchID: "m1", Team: "Arsenal", EventType: "goal"})

	statsMu.Lock()
	got := stats["m1"]
	statsMu.Unlock()

	// The aggregate must still reflect the event even if we could not
	// record that fact; the reservation simply expires with its TTL.
	if got == nil || got.HomeGoals != 1 {
		t.Fatalf("expected the event to be aggregated even when MarkProcessed fails, got %+v", got)
	}
	if len(fakePub.published) != 1 {
		t.Fatalf("expected stats to still be published, got %d", len(fakePub.published))
	}
}

// TestProcessEvent_EventIsNotLostWhenProcessingNeverCompletes models the
// crash-after-reserve case: a consumer claims the event and dies before
// marking it processed. The reservation is left in "processing", and once
// its TTL expires (simulated here by clearing it) the redelivery must be
// aggregated rather than suppressed forever.
func TestProcessEvent_EventIsNotLostWhenProcessingNeverCompletes(t *testing.T) {
	fakeStore := &fakeStatsStore{}
	fakePub := &fakeStatsPublisher{}
	resetGlobalState(t, fakeStore, fakePub)

	event := MatchEvent{EventID: "evt-1", MatchID: "m1", Team: "Arsenal", EventType: "goal"}

	// Consumer A reserves, then "crashes" before applying the event.
	state, err := fakeStore.ReserveEvent(context.Background(), event.EventID, dedupeTTL)
	if err != nil || state != common.ReservationAcquired {
		t.Fatalf("expected to acquire the reservation, got %v (err=%v)", state, err)
	}

	// While the reservation is held, a redelivery is correctly skipped.
	processEvent(context.Background(), event)
	statsMu.Lock()
	duringHold := stats["m1"]
	statsMu.Unlock()
	if duringHold != nil {
		t.Fatalf("an in-flight reservation should suppress the redelivery, got %+v", duringHold)
	}

	// TTL expiry releases the abandoned reservation.
	if err := fakeStore.ReleaseReservation(context.Background(), event.EventID); err != nil {
		t.Fatalf("release failed: %v", err)
	}

	// The next redelivery must now be processed — the event is not lost.
	processEvent(context.Background(), event)
	statsMu.Lock()
	afterExpiry := stats["m1"]
	statsMu.Unlock()

	if afterExpiry == nil || afterExpiry.HomeGoals != 1 {
		t.Fatalf("expected the event to be recovered after the reservation expired, got %+v", afterExpiry)
	}
}

func TestProcessEvent_ConcurrentDuplicatesAreCountedOnce(t *testing.T) {
	fakeStore := &fakeStatsStore{}
	fakePub := &fakeStatsPublisher{}
	resetGlobalState(t, fakeStore, fakePub)

	event := MatchEvent{EventID: "evt-1", MatchID: "m1", Team: "Arsenal", EventType: "goal"}

	const workers = 8
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			processEvent(context.Background(), event)
		}()
	}
	wg.Wait()

	statsMu.Lock()
	got := stats["m1"]
	statsMu.Unlock()

	if got == nil || got.HomeGoals != 1 {
		t.Fatalf("expected concurrent duplicates to be counted exactly once, got %+v", got)
	}
}

// --- DLQ / malformed message handling ---

type fakeDLQPublisher struct {
	mu        sync.Mutex
	published []common.DLQRecord
	failFor   int // fail this many calls before succeeding
	calls     int
}

func (f *fakeDLQPublisher) PublishDLQ(_ context.Context, record common.DLQRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.calls <= f.failFor {
		return errors.New("dlq broker unavailable")
	}
	f.published = append(f.published, record)
	return nil
}

func (f *fakeDLQPublisher) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func TestHandleMessage_MalformedJSONRoutesToDLQ(t *testing.T) {
	fakeStore := &fakeStatsStore{}
	fakePub := &fakeStatsPublisher{}
	fakeDLQ := &fakeDLQPublisher{}
	resetGlobalState(t, fakeStore, fakePub)
	dlq = fakeDLQ

	msg := kafka.Message{Topic: "match-events", Partition: 2, Offset: 42, Value: []byte(`{not valid json`)}
	handleMessage(context.Background(), msg)

	if len(fakeDLQ.published) != 1 {
		t.Fatalf("expected 1 DLQ record, got %d", len(fakeDLQ.published))
	}
	rec := fakeDLQ.published[0]
	if rec.OriginalTopic != "match-events" || rec.OriginalPartition != 2 || rec.OriginalOffset != 42 {
		t.Fatalf("DLQ record lost the original message coordinates: %+v", rec)
	}
	if string(rec.Payload) != `{not valid json` {
		t.Fatalf("DLQ record did not preserve the original payload: %q", rec.Payload)
	}
	if rec.FailureReason == "" {
		t.Fatal("expected a non-empty failure reason")
	}
	// The malformed message must not have reached the aggregate.
	statsMu.Lock()
	n := len(stats)
	statsMu.Unlock()
	if n != 0 {
		t.Fatalf("a DLQ'd message must not be aggregated, got %d matches in memory", n)
	}
}

func TestHandleMessage_ValidEventIsProcessedNotDLQd(t *testing.T) {
	fakeStore := &fakeStatsStore{}
	fakePub := &fakeStatsPublisher{}
	fakeDLQ := &fakeDLQPublisher{}
	resetGlobalState(t, fakeStore, fakePub)
	dlq = fakeDLQ

	event := MatchEvent{EventID: "evt-1", MatchID: "m1", Team: "Arsenal", EventType: "goal"}
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("failed to marshal fixture event: %v", err)
	}

	handleMessage(context.Background(), kafka.Message{Topic: "match-events", Value: data})

	if len(fakeDLQ.published) != 0 {
		t.Fatalf("a valid event must not be routed to the DLQ, got %d records", len(fakeDLQ.published))
	}
	statsMu.Lock()
	got := stats["m1"]
	statsMu.Unlock()
	if got == nil || got.HomeGoals != 1 {
		t.Fatalf("expected the valid event to be aggregated, got %+v", got)
	}
}

func TestRouteToDLQ_RetriesTransientFailureThenSucceeds(t *testing.T) {
	fakeDLQ := &fakeDLQPublisher{failFor: 2}
	dlq = fakeDLQ

	routeToDLQ(context.Background(), kafka.Message{Topic: "match-events", Offset: 1}, errors.New("bad json"))

	if fakeDLQ.callCount() != 3 {
		t.Fatalf("expected 3 attempts (2 failures + 1 success), got %d", fakeDLQ.callCount())
	}
	if len(fakeDLQ.published) != 1 {
		t.Fatalf("expected the record to eventually be published, got %d", len(fakeDLQ.published))
	}
	if fakeDLQ.published[0].Attempts != 3 {
		t.Fatalf("expected the record to report 3 attempts, got %d", fakeDLQ.published[0].Attempts)
	}
}

func TestRouteToDLQ_GivesUpAfterBoundedAttemptsWithoutPanicking(t *testing.T) {
	// This is the double-failure case: the message can't be parsed AND the
	// DLQ itself is unreachable. The bound must still hold — the process
	// must not retry forever or crash, even though the message is lost.
	fakeDLQ := &fakeDLQPublisher{failFor: 1000}
	dlq = fakeDLQ

	routeToDLQ(context.Background(), kafka.Message{Topic: "match-events", Offset: 1}, errors.New("bad json"))

	if fakeDLQ.callCount() != maxDLQAttempts {
		t.Fatalf("expected exactly %d attempts, got %d", maxDLQAttempts, fakeDLQ.callCount())
	}
	if len(fakeDLQ.published) != 0 {
		t.Fatalf("expected no successful publish, got %d", len(fakeDLQ.published))
	}
}

func TestHandleMessage_DLQPublishFailureDoesNotPanicOrBlockFurtherMessages(t *testing.T) {
	fakeStore := &fakeStatsStore{}
	fakePub := &fakeStatsPublisher{}
	fakeDLQ := &fakeDLQPublisher{failFor: 1000}
	resetGlobalState(t, fakeStore, fakePub)
	dlq = fakeDLQ

	// A message that can't be parsed AND can't be DLQ'd must not prevent
	// the next (valid) message on the same "partition" from being handled.
	handleMessage(context.Background(), kafka.Message{Topic: "match-events", Value: []byte(`not json`)})

	valid := MatchEvent{EventID: "evt-2", MatchID: "m1", Team: "Arsenal", EventType: "shot"}
	data, err := json.Marshal(valid)
	if err != nil {
		t.Fatalf("failed to marshal fixture event: %v", err)
	}
	handleMessage(context.Background(), kafka.Message{Topic: "match-events", Value: data})

	statsMu.Lock()
	got := stats["m1"]
	statsMu.Unlock()
	if got == nil || got.HomeShots != 1 {
		t.Fatalf("expected the second, valid message to still be processed, got %+v", got)
	}
}
