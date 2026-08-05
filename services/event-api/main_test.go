package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/nhatminh06/matchsense/common"
	"github.com/segmentio/kafka-go"
)

type fakePublisher struct {
	mu        sync.Mutex
	published []kafka.Message
	err       error
}

func (f *fakePublisher) Publish(_ context.Context, msg kafka.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.published = append(f.published, msg)
	return nil
}

func postEvent(t *testing.T, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	eventsHandler(rec, req)
	return rec
}

func TestEventsHandler_ValidEventIsPublished(t *testing.T) {
	fp := &fakePublisher{}
	pub = fp

	rec := postEvent(t, `{"match_id":"m1","event_type":"goal","team":"Arsenal","minute":10}`)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rec.Code, rec.Body.String())
	}
	if len(fp.published) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(fp.published))
	}

	var published MatchEvent
	if err := json.Unmarshal(fp.published[0].Value, &published); err != nil {
		t.Fatalf("published message is not valid JSON: %v", err)
	}
	if published.MatchID != "m1" || published.EventType != "goal" {
		t.Fatalf("published event does not match input: %+v", published)
	}
	if published.Timestamp == "" {
		t.Fatalf("expected a timestamp to be filled in when the request omitted one")
	}
}

func TestEventsHandler_InvalidJSON(t *testing.T) {
	fp := &fakePublisher{}
	pub = fp

	rec := postEvent(t, `{not valid json`)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid JSON, got %d", rec.Code)
	}
	if len(fp.published) != 0 {
		t.Fatalf("invalid JSON must not be published, got %d messages", len(fp.published))
	}
}

func TestEventsHandler_MissingRequiredFields(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"missing match_id", `{"event_type":"goal"}`},
		{"missing event_type", `{"match_id":"m1"}`},
		{"empty body object", `{}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fp := &fakePublisher{}
			pub = fp

			rec := postEvent(t, tc.body)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d", rec.Code)
			}
			if len(fp.published) != 0 {
				t.Fatalf("event missing required fields must not be published")
			}
		})
	}
}

func TestEventsHandler_MethodNotAllowed(t *testing.T) {
	fp := &fakePublisher{}
	pub = fp

	req := httptest.NewRequest(http.MethodGet, "/events", nil)
	rec := httptest.NewRecorder()
	eventsHandler(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 for GET /events, got %d", rec.Code)
	}
}

func TestEventsHandler_KafkaPublishFailureReturns500(t *testing.T) {
	fp := &fakePublisher{err: errors.New("kafka unavailable")}
	pub = fp

	rec := postEvent(t, `{"match_id":"m1","event_type":"shot"}`)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 when Kafka publish fails, got %d", rec.Code)
	}
}

func TestEventsHandler_PreservesClientSuppliedTimestamp(t *testing.T) {
	fp := &fakePublisher{}
	pub = fp

	rec := postEvent(t, `{"match_id":"m1","event_type":"goal","timestamp":"2026-01-01T00:00:00Z"}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rec.Code)
	}

	var published MatchEvent
	if err := json.Unmarshal(fp.published[0].Value, &published); err != nil {
		t.Fatalf("published message is not valid JSON: %v", err)
	}
	if published.Timestamp != "2026-01-01T00:00:00Z" {
		t.Fatalf("expected client-supplied timestamp to be preserved, got %q", published.Timestamp)
	}
}

func TestHealthHandler(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	healthHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("health response is not valid JSON: %v", err)
	}
	if body["status"] != "ok" {
		t.Fatalf("expected status=ok, got %+v", body)
	}
}

// --- event_id behavior ---

func TestEventsHandler_GeneratesEventIDWhenAbsent(t *testing.T) {
	fp := &fakePublisher{}
	pub = fp

	rec := postEvent(t, `{"match_id":"m1","event_type":"goal"}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rec.Code)
	}

	var published MatchEvent
	if err := json.Unmarshal(fp.published[0].Value, &published); err != nil {
		t.Fatalf("published message is not valid JSON: %v", err)
	}
	if published.EventID == "" {
		t.Fatalf("expected an event_id to be generated when the client omitted one")
	}
	if err := common.ValidateEventID(published.EventID); err != nil {
		t.Fatalf("generated event_id is not valid: %v", err)
	}
}

func TestEventsHandler_GeneratedEventIDsAreUnique(t *testing.T) {
	fp := &fakePublisher{}
	pub = fp

	const n = 50
	for i := 0; i < n; i++ {
		if rec := postEvent(t, `{"match_id":"m1","event_type":"shot"}`); rec.Code != http.StatusAccepted {
			t.Fatalf("request %d: expected 202, got %d", i, rec.Code)
		}
	}

	seen := make(map[string]bool, n)
	for _, msg := range fp.published {
		var e MatchEvent
		if err := json.Unmarshal(msg.Value, &e); err != nil {
			t.Fatalf("published message is not valid JSON: %v", err)
		}
		if seen[e.EventID] {
			t.Fatalf("generated a duplicate event_id: %q", e.EventID)
		}
		seen[e.EventID] = true
	}
	if len(seen) != n {
		t.Fatalf("expected %d distinct event IDs, got %d", n, len(seen))
	}
}

func TestEventsHandler_PreservesClientSuppliedEventID(t *testing.T) {
	fp := &fakePublisher{}
	pub = fp

	rec := postEvent(t, `{"event_id":"evt-client-123","match_id":"m1","event_type":"goal"}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rec.Code)
	}

	var published MatchEvent
	if err := json.Unmarshal(fp.published[0].Value, &published); err != nil {
		t.Fatalf("published message is not valid JSON: %v", err)
	}
	// A client that supplies its own ID is relying on it surviving intact
	// so that its retries deduplicate downstream.
	if published.EventID != "evt-client-123" {
		t.Fatalf("expected client-supplied event_id to be preserved, got %q", published.EventID)
	}
}

func TestEventsHandler_RejectsMalformedEventID(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"contains colon", `{"event_id":"evt:123","match_id":"m1","event_type":"goal"}`},
		{"contains whitespace", `{"event_id":"evt 123","match_id":"m1","event_type":"goal"}`},
		{"contains newline", `{"event_id":"evt\n123","match_id":"m1","event_type":"goal"}`},
		{"too long", `{"event_id":"` + strings.Repeat("a", common.MaxEventIDLength+1) + `","match_id":"m1","event_type":"goal"}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fp := &fakePublisher{}
			pub = fp

			rec := postEvent(t, tc.body)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected 400 for a malformed event_id, got %d", rec.Code)
			}
			if len(fp.published) != 0 {
				t.Fatalf("an event with a malformed event_id must not be published")
			}
		})
	}
}
