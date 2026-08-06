package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newTestSimulator returns a simulator whose every source of
// non-determinism is pinned: the RNG seed, the clock, and the event-ID
// generator. Two simulators built with the same seed must therefore emit
// byte-identical events.
func newTestSimulator(t *testing.T, seed int64) *simulator {
	t.Helper()
	s := newSimulator(seed, "test-match", "Arsenal", "Manchester City")
	s.now = func() time.Time { return time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC) }
	var n int64
	s.newID = func() string {
		n++
		return fmt.Sprintf("evt-test-%d", n)
	}
	return s
}

func collectMatch(s *simulator) []MatchEvent {
	var all []MatchEvent
	for minute := 1; minute <= matchMinutes; minute++ {
		all = append(all, s.generateMinuteEvents(minute)...)
	}
	return all
}

func TestSimulator_SameSeedProducesIdenticalMatch(t *testing.T) {
	a := collectMatch(newTestSimulator(t, 42))
	b := collectMatch(newTestSimulator(t, 42))

	if len(a) == 0 {
		t.Fatal("expected the simulated match to produce at least one event")
	}
	if len(a) != len(b) {
		t.Fatalf("same seed produced different event counts: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("same seed diverged at event %d:\n  a=%+v\n  b=%+v", i, a[i], b[i])
		}
	}
}

func TestSimulator_DifferentSeedsProduceDifferentMatches(t *testing.T) {
	a := collectMatch(newTestSimulator(t, 1))
	b := collectMatch(newTestSimulator(t, 2))

	// Not a guarantee for every possible seed pair, but two ~90-minute
	// matches drawn from different streams coming out identical would mean
	// the seed is not actually wired to the RNG.
	if len(a) == len(b) {
		identical := true
		for i := range a {
			if a[i] != b[i] {
				identical = false
				break
			}
		}
		if identical {
			t.Fatal("different seeds produced identical matches — the seed is not being used")
		}
	}
}

func TestSimulator_GlobalRandDoesNotAffectSeededOutput(t *testing.T) {
	// Guards against a regression to package-level math/rand: draining the
	// global source must not shift this simulator's stream.
	baseline := collectMatch(newTestSimulator(t, 7))

	s := newTestSimulator(t, 7)
	for i := 0; i < 100; i++ {
		_ = rand.Float64()
	}
	after := collectMatch(s)

	if len(baseline) != len(after) {
		t.Fatalf("global rand usage changed the seeded output: %d vs %d events", len(baseline), len(after))
	}
	for i := range baseline {
		if baseline[i] != after[i] {
			t.Fatalf("global rand usage changed the seeded output at event %d", i)
		}
	}
}

func TestSimulator_GeneratedEventsAreWellFormed(t *testing.T) {
	s := newTestSimulator(t, 99)

	sawShot, sawFoul, sawCorner := false, false, false
	seenIDs := map[string]bool{}

	for minute := 1; minute <= matchMinutes; minute++ {
		for _, e := range s.generateMinuteEvents(minute) {
			if e.EventID == "" {
				t.Fatalf("event is missing an event ID: %+v", e)
			}
			if seenIDs[e.EventID] {
				t.Fatalf("duplicate event ID within one match: %q", e.EventID)
			}
			seenIDs[e.EventID] = true

			if e.MatchID != "test-match" {
				t.Fatalf("wrong match ID: %+v", e)
			}
			if e.Minute != minute {
				t.Fatalf("event minute %d does not match the requested minute %d", e.Minute, minute)
			}
			if e.Team != "Arsenal" && e.Team != "Manchester City" {
				t.Fatalf("event team is neither home nor away: %+v", e)
			}
			if e.Player == "" {
				t.Fatalf("event is missing a player: %+v", e)
			}
			if e.X < 0 || e.X > 100 || e.Y < 0 || e.Y > 100 {
				t.Fatalf("event coordinates outside the 0-100 pitch: %+v", e)
			}
			if e.Timestamp == "" {
				t.Fatalf("event is missing a timestamp: %+v", e)
			}

			switch e.EventType {
			case "shot":
				sawShot = true
				if e.Detail != "on_target" && e.Detail != "off_target" {
					t.Fatalf("unexpected shot detail %q", e.Detail)
				}
			case "goal":
				// asserted separately below
			case "foul":
				sawFoul = true
			case "card":
				if e.Detail != "yellow" && e.Detail != "red" {
					t.Fatalf("unexpected card detail %q", e.Detail)
				}
			case "corner":
				sawCorner = true
			default:
				t.Fatalf("unrecognised event type %q", e.EventType)
			}
		}
	}

	if !sawShot || !sawFoul || !sawCorner {
		t.Fatalf("expected shots, fouls and corners over a full match: shot=%v foul=%v corner=%v",
			sawShot, sawFoul, sawCorner)
	}
}

func TestSimulator_GoalAlwaysFollowsAnOnTargetShot(t *testing.T) {
	for seed := int64(0); seed < 25; seed++ {
		s := newTestSimulator(t, seed)
		for minute := 1; minute <= matchMinutes; minute++ {
			events := s.generateMinuteEvents(minute)
			for i, e := range events {
				if e.EventType != "goal" {
					continue
				}
				if i == 0 {
					t.Fatalf("seed %d: goal with no preceding shot: %+v", seed, events)
				}
				prev := events[i-1]
				if prev.EventType != "shot" || prev.Detail != "on_target" ||
					prev.Team != e.Team || prev.Player != e.Player {
					t.Fatalf("seed %d: goal not preceded by a matching on-target shot: goal=%+v prev=%+v", seed, e, prev)
				}
			}
		}
	}
}

func TestSimulator_PickTeamAndPlayerAlwaysReturnsAValidPair(t *testing.T) {
	s := newTestSimulator(t, 11)
	homeSeen, awaySeen := false, false

	for i := 0; i < 200; i++ {
		team, player := s.pickTeamAndPlayer()

		var roster []string
		switch team {
		case "Arsenal":
			homeSeen, roster = true, homePlayers
		case "Manchester City":
			awaySeen, roster = true, awayPlayers
		default:
			t.Fatalf("unknown team %q", team)
		}

		found := false
		for _, p := range roster {
			if p == player {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("player %q is not on %s's roster", player, team)
		}
	}

	if !homeSeen || !awaySeen {
		t.Fatalf("expected both teams across 200 draws: home=%v away=%v", homeSeen, awaySeen)
	}
}

// --- send / retry behaviour ---

type fakeSender struct {
	mu       sync.Mutex
	attempts int
	failFor  int   // fail this many attempts before succeeding
	err      error // error returned while failing
	sent     []MatchEvent
}

func (f *fakeSender) Send(_ context.Context, event MatchEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts++
	if f.attempts <= f.failFor {
		return f.err
	}
	f.sent = append(f.sent, event)
	return nil
}

func (f *fakeSender) attemptCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts
}

func TestSendWithRetry_SucceedsFirstTime(t *testing.T) {
	f := &fakeSender{}
	if err := sendWithRetry(context.Background(), f, MatchEvent{EventID: "evt-1"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if f.attemptCount() != 1 {
		t.Fatalf("expected exactly 1 attempt, got %d", f.attemptCount())
	}
}

func TestSendWithRetry_RetriesTransientFailureThenSucceeds(t *testing.T) {
	f := &fakeSender{failFor: 2, err: errors.New("event-api returned 503")}

	if err := sendWithRetry(context.Background(), f, MatchEvent{EventID: "evt-1"}); err != nil {
		t.Fatalf("expected the retry to eventually succeed, got %v", err)
	}
	if f.attemptCount() != 3 {
		t.Fatalf("expected 3 attempts, got %d", f.attemptCount())
	}
	if len(f.sent) != 1 {
		t.Fatalf("expected the event to be delivered once, got %d", len(f.sent))
	}
}

func TestSendWithRetry_GivesUpAfterBoundedAttempts(t *testing.T) {
	// The key property: retries are bounded, so a permanently unreachable
	// event-api cannot spin the simulator forever.
	f := &fakeSender{failFor: 1000, err: errors.New("connection refused")}

	if err := sendWithRetry(context.Background(), f, MatchEvent{EventID: "evt-1"}); err == nil {
		t.Fatal("expected an error once the attempt budget is exhausted")
	}
	if f.attemptCount() != maxSendAttempts {
		t.Fatalf("expected exactly %d attempts, got %d", maxSendAttempts, f.attemptCount())
	}
}

func TestSendWithRetry_DoesNotRetryPermanentFailure(t *testing.T) {
	// A 4xx will never succeed; retrying it just wastes the budget.
	f := &fakeSender{failFor: 1000, err: fmt.Errorf("%w: event-api returned 400", errPermanent)}

	if err := sendWithRetry(context.Background(), f, MatchEvent{EventID: "evt-1"}); err == nil {
		t.Fatal("expected a permanent failure to be returned")
	}
	if f.attemptCount() != 1 {
		t.Fatalf("expected a permanent failure to stop after 1 attempt, got %d", f.attemptCount())
	}
}

func TestSendWithRetry_AbortsPromptlyOnCancellation(t *testing.T) {
	f := &fakeSender{failFor: 1000, err: errors.New("connection refused")}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	if err := sendWithRetry(ctx, f, MatchEvent{EventID: "evt-1"}); err == nil {
		t.Fatal("expected an error when the context is already cancelled")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("cancellation should abort immediately, took %s", elapsed)
	}
	if f.attemptCount() != 1 {
		t.Fatalf("expected 1 attempt before honouring cancellation, got %d", f.attemptCount())
	}
}

func TestHTTPEventSender_ClassifiesResponses(t *testing.T) {
	cases := []struct {
		name          string
		status        int
		wantErr       bool
		wantPermanent bool
	}{
		{"202 accepted", http.StatusAccepted, false, false},
		{"400 bad request is permanent", http.StatusBadRequest, true, true},
		{"500 is retryable", http.StatusInternalServerError, true, false},
		{"503 is retryable", http.StatusServiceUnavailable, true, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()

			sender := httpEventSender{client: srv.Client(), url: srv.URL}
			err := sender.Send(context.Background(), MatchEvent{EventID: "evt-1", MatchID: "m1"})

			if tc.wantErr && err == nil {
				t.Fatalf("expected an error for status %d", tc.status)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error for status %d: %v", tc.status, err)
			}
			if got := errors.Is(err, errPermanent); got != tc.wantPermanent {
				t.Fatalf("permanent classification for status %d = %v, want %v", tc.status, got, tc.wantPermanent)
			}
		})
	}
}

func TestHTTPEventSender_SendsTheEventAsJSON(t *testing.T) {
	received := make(chan MatchEvent, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var e MatchEvent
		if err := json.NewDecoder(r.Body).Decode(&e); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		received <- e
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	sender := httpEventSender{client: srv.Client(), url: srv.URL}
	sent := MatchEvent{EventID: "evt-1", MatchID: "m1", EventType: "goal", Team: "Arsenal", Minute: 12}
	if err := sender.Send(context.Background(), sent); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := <-received
	if got.EventID != sent.EventID || got.MatchID != sent.MatchID || got.EventType != sent.EventType {
		t.Fatalf("event was not transmitted faithfully: got %+v, sent %+v", got, sent)
	}
}

// --- run loop ---

func TestRun_StopsPromptlyWhenContextCancelled(t *testing.T) {
	s := newTestSimulator(t, 3)
	f := &fakeSender{}

	ctx, cancel := context.WithCancel(context.Background())
	var done int32

	go func() {
		// A full match at this interval would take far longer than the
		// deadline below; cancellation must cut it short.
		s.run(ctx, f, 50*time.Millisecond)
		atomic.StoreInt32(&done, 1)
	}()

	time.Sleep(120 * time.Millisecond)
	cancel()

	deadline := time.After(2 * time.Second)
	for atomic.LoadInt32(&done) == 0 {
		select {
		case <-deadline:
			t.Fatal("run did not return promptly after cancellation")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestRun_ContinuesAfterAnUndeliverableEvent(t *testing.T) {
	s := newTestSimulator(t, 5)
	// Always fails permanently: every event is rejected, but the match must
	// still play to full time rather than aborting on the first failure.
	f := &fakeSender{failFor: 1000, err: fmt.Errorf("%w: event-api returned 400", errPermanent)}

	done := make(chan struct{})
	go func() {
		s.run(context.Background(), f, 0)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("run did not complete when every event failed")
	}

	if f.attemptCount() == 0 {
		t.Fatal("expected the simulator to have attempted sends")
	}
}

// --- configuration ---

func TestResolveSeed(t *testing.T) {
	seed, seeded := resolveSeed("12345")
	if !seeded || seed != 12345 {
		t.Fatalf("expected the configured seed to be used, got seed=%d seeded=%v", seed, seeded)
	}
	if _, seeded := resolveSeed(""); seeded {
		t.Fatal("an unset seed should not be reported as configured")
	}
	if _, seeded := resolveSeed("not-a-number"); seeded {
		t.Fatal("an invalid seed should fall back to a time-based seed")
	}
}

func TestResolveMinuteDelay(t *testing.T) {
	cases := []struct {
		name     string
		speed    string
		interval string
		want     time.Duration
	}{
		{"defaults", "", "", 2 * time.Second},
		{"SPEED honoured for backward compatibility", "5", "", 5 * time.Second},
		{"interval supersedes SPEED", "5", "250ms", 250 * time.Millisecond},
		{"zero interval runs flat out", "", "0s", 0},
		{"invalid SPEED falls back to the default", "abc", "", 2 * time.Second},
		{"invalid interval falls back to SPEED", "5", "nonsense", 5 * time.Second},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveMinuteDelay(tc.speed, tc.interval); got != tc.want {
				t.Fatalf("resolveMinuteDelay(%q, %q) = %s, want %s", tc.speed, tc.interval, got, tc.want)
			}
		})
	}
}

func TestSleepCtx_ReturnsFalseWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if sleepCtx(ctx, time.Hour) {
		t.Fatal("sleepCtx should report cancellation rather than waiting")
	}
}
