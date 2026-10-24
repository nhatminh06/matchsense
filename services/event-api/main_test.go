package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nhatminh06/matchsense/common"
	"github.com/segmentio/kafka-go"
)

type fakePublisher struct {
	mu        sync.Mutex
	published []kafka.Message
	err       error
	publish   func(context.Context, kafka.Message) error
}

func (f *fakePublisher) Publish(ctx context.Context, msg kafka.Message) error {
	if f.publish != nil {
		return f.publish(ctx, msg)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.published = append(f.published, msg)
	return nil
}

var testAPIKey = strings.Repeat("test-", 7)

func postEvent(t *testing.T, body string) *httptest.ResponseRecorder {
	t.Helper()
	apiKey = testAPIKey
	limiter = newTokenBucket(10000, 10000)
	publishTimeout = 3 * time.Second
	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", testAPIKey)
	rec := httptest.NewRecorder()
	apiKeyMiddleware(http.HandlerFunc(eventsHandler)).ServeHTTP(rec, req)
	return rec
}

func requestEvent(t *testing.T, key, contentType, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("X-API-Key", key)
	rec := httptest.NewRecorder()
	apiKeyMiddleware(http.HandlerFunc(eventsHandler)).ServeHTTP(rec, req)
	return rec
}

func TestAPIKeyMiddlewareRedactsHeader(t *testing.T) {
	apiKey = testAPIKey
	var downstreamHeader string
	handler := apiKeyMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		downstreamHeader = r.Header.Get("X-API-Key")
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("X-API-Key", testAPIKey)
	handler.ServeHTTP(httptest.NewRecorder(), req)
	if downstreamHeader != "" {
		t.Fatal("API key reached tracing or downstream handlers")
	}
}

func TestEventsHandler_Authentication(t *testing.T) {
	apiKey = testAPIKey
	limiter = newTokenBucket(100, 100)
	body := `{"match_id":"m1","event_type":"goal"}`
	for _, tc := range []struct {
		name string
		key  string
	}{
		{"missing", ""},
		{"invalid", "wrong"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := requestEvent(t, tc.key, "application/json", body)
			if rec.Code != http.StatusUnauthorized || strings.Contains(rec.Body.String(), testAPIKey) {
				t.Fatalf("expected non-sensitive 401, got %d: %s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestEventsHandler_NoLocalhostAuthenticationBypass(t *testing.T) {
	apiKey = testAPIKey
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/events", bytes.NewBufferString(`{"match_id":"m1","event_type":"goal"}`))
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	apiKeyMiddleware(http.HandlerFunc(eventsHandler)).ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("localhost request without a key = %d, want 401", rec.Code)
	}
}

func TestRequiredAPIKey(t *testing.T) {
	if _, err := requiredAPIKey(""); err == nil {
		t.Fatal("missing key must fail configuration")
	}
	if _, err := requiredAPIKey("too-short"); err == nil {
		t.Fatal("short key must fail configuration")
	}
	if got, err := requiredAPIKey(testAPIKey); err != nil || got != testAPIKey {
		t.Fatalf("valid key rejected: %v", err)
	}
}

func TestRateAndTimeoutConfiguration(t *testing.T) {
	t.Setenv("EVENT_API_RATE_LIMIT", "")
	if got, err := positiveFloatEnv("EVENT_API_RATE_LIMIT", 20); err != nil || got != 20 {
		t.Fatalf("default rate = %v, %v", got, err)
	}
	t.Setenv("EVENT_API_RATE_LIMIT", "0")
	if _, err := positiveFloatEnv("EVENT_API_RATE_LIMIT", 20); err == nil {
		t.Fatal("zero rate must fail configuration")
	}
	for _, invalid := range []string{"NaN", "+Inf"} {
		t.Setenv("EVENT_API_RATE_LIMIT", invalid)
		if _, err := positiveFloatEnv("EVENT_API_RATE_LIMIT", 20); err == nil {
			t.Fatalf("rate %q must fail configuration", invalid)
		}
	}
	t.Setenv("EVENT_API_RATE_BURST", "-1")
	if _, err := positiveIntEnv("EVENT_API_RATE_BURST", 40); err == nil {
		t.Fatal("negative burst must fail configuration")
	}
	t.Setenv("EVENT_API_PUBLISH_TIMEOUT", "nonsense")
	if _, err := positiveDurationEnv("EVENT_API_PUBLISH_TIMEOUT", 3*time.Second); err == nil {
		t.Fatal("invalid publish timeout must fail configuration")
	}
}

func TestEventsHandler_RequestProtection(t *testing.T) {
	apiKey = testAPIKey
	limiter = newTokenBucket(100, 100)
	tests := []struct {
		name        string
		contentType string
		body        string
		status      int
	}{
		{"oversized", "application/json", `{"match_id":"` + strings.Repeat("a", maxRequestBody) + `","event_type":"goal"}`, http.StatusRequestEntityTooLarge},
		{"oversized trailing whitespace", "application/json", `{"match_id":"m1","event_type":"goal"}` + strings.Repeat(" ", maxRequestBody), http.StatusRequestEntityTooLarge},
		{"malformed", "application/json", `{`, http.StatusBadRequest},
		{"extra document", "application/json", `{"match_id":"m1","event_type":"goal"} {}`, http.StatusBadRequest},
		{"wrong content type", "text/plain", `{"match_id":"m1","event_type":"goal"}`, http.StatusUnsupportedMediaType},
		{"unbounded metric label", "application/json", `{"match_id":"m1","event_type":"attacker-value"}`, http.StatusBadRequest},
		{"bad timestamp", "application/json", `{"match_id":"m1","event_type":"goal","timestamp":"yesterday"}`, http.StatusBadRequest},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := requestEvent(t, testAPIKey, tc.contentType, tc.body)
			if rec.Code != tc.status {
				t.Fatalf("expected %d, got %d: %s", tc.status, rec.Code, rec.Body.String())
			}
		})
	}
}

func TestEventsHandler_RateLimitExceeded(t *testing.T) {
	apiKey = testAPIKey
	limiter = newTokenBucket(0.0001, 1)
	pub = &fakePublisher{}
	body := `{"match_id":"m1","event_type":"goal"}`
	if got := requestEvent(t, testAPIKey, "application/json", body).Code; got != http.StatusAccepted {
		t.Fatalf("first request = %d, want 202", got)
	}
	if got := requestEvent(t, testAPIKey, "application/json", body).Code; got != http.StatusTooManyRequests {
		t.Fatalf("second request = %d, want 429", got)
	}
}

func TestEventsHandler_PublishDeadlineAndCancellation(t *testing.T) {
	apiKey = testAPIKey
	limiter = newTokenBucket(100, 100)
	publishTimeout = 20 * time.Millisecond
	pub = &fakePublisher{publish: func(ctx context.Context, _ kafka.Message) error {
		<-ctx.Done()
		return ctx.Err()
	}}
	rec := requestEvent(t, testAPIKey, "application/json", `{"match_id":"m1","event_type":"goal"}`)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("publish timeout = %d, want 500", rec.Code)
	}

	publishTimeout = time.Second
	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewBufferString(`{"match_id":"m1","event_type":"goal"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", testAPIKey)
	ctx, cancel := context.WithCancel(req.Context())
	cancel()
	rec = httptest.NewRecorder()
	apiKeyMiddleware(http.HandlerFunc(eventsHandler)).ServeHTTP(rec, req.WithContext(ctx))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("cancelled publish = %d, want 500", rec.Code)
	}
}

func TestEventsHandler_DoesNotLogAPIKey(t *testing.T) {
	apiKey = testAPIKey
	limiter = newTokenBucket(100, 100)
	var logs bytes.Buffer
	old := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(old)
	rec := requestEvent(t, testAPIKey+"wrong", "application/json", `{}`)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
	if strings.Contains(logs.String()+rec.Body.String(), testAPIKey) {
		t.Fatal("API key leaked through logs or response")
	}
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
	apiKeyMiddleware(http.HandlerFunc(eventsHandler)).ServeHTTP(rec, req)

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
