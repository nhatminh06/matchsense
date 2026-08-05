package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/redis/go-redis/v9"
)

type fakeMatchStore struct {
	activeIDs    []string
	activeErr    error
	values       map[string]string
	getErrForKey map[string]error
}

func (f *fakeMatchStore) ActiveMatchIDs(_ context.Context) ([]string, error) {
	if f.activeErr != nil {
		return nil, f.activeErr
	}
	return f.activeIDs, nil
}

func (f *fakeMatchStore) Get(_ context.Context, key string) (string, error) {
	if err, ok := f.getErrForKey[key]; ok {
		return "", err
	}
	v, ok := f.values[key]
	if !ok {
		return "", redis.Nil
	}
	return v, nil
}

func TestMatchesHandler_ListsActiveMatches(t *testing.T) {
	store = &fakeMatchStore{
		activeIDs: []string{"m1", "m2"},
		values: map[string]string{
			"match:m1:stats": `{"match_id":"m1","home_goals":1}`,
			"match:m2:stats": `{"match_id":"m2","home_goals":0}`,
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/matches", nil)
	rec := httptest.NewRecorder()
	matchesHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var body struct {
		Count   int               `json:"count"`
		Matches []json.RawMessage `json:"matches"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response is not valid JSON: %v", err)
	}
	if body.Count != 2 || len(body.Matches) != 2 {
		t.Fatalf("expected 2 matches, got count=%d matches=%d", body.Count, len(body.Matches))
	}
}

func TestMatchesHandler_SkipsMatchWithMissingStats(t *testing.T) {
	// One active match ID has no corresponding stats key in Redis (e.g. a
	// race between SAdd and Set) — it should be silently skipped, not
	// error the whole listing.
	store = &fakeMatchStore{
		activeIDs: []string{"m1", "m2"},
		values: map[string]string{
			"match:m1:stats": `{"match_id":"m1"}`,
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/matches", nil)
	rec := httptest.NewRecorder()
	matchesHandler(rec, req)

	var body struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response is not valid JSON: %v", err)
	}
	if body.Count != 1 {
		t.Fatalf("expected 1 match (the other has no stats key), got %d", body.Count)
	}
}

func TestMatchesHandler_RedisUnavailable(t *testing.T) {
	store = &fakeMatchStore{activeErr: errors.New("redis unavailable")}

	req := httptest.NewRequest(http.MethodGet, "/matches", nil)
	rec := httptest.NewRecorder()
	matchesHandler(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 when Redis is unavailable, got %d", rec.Code)
	}
}

func TestMatchDetailHandler_ExistingMatch(t *testing.T) {
	store = &fakeMatchStore{
		values: map[string]string{
			"match:m1:stats": `{"match_id":"m1","home_goals":2}`,
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/matches/m1", nil)
	rec := httptest.NewRecorder()
	matchDetailHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != `{"match_id":"m1","home_goals":2}` {
		t.Fatalf("unexpected body: %s", rec.Body.String())
	}
}

func TestMatchDetailHandler_MissingMatch(t *testing.T) {
	store = &fakeMatchStore{values: map[string]string{}}

	req := httptest.NewRequest(http.MethodGet, "/matches/does-not-exist", nil)
	rec := httptest.NewRecorder()
	matchDetailHandler(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for a match that was never seen, got %d", rec.Code)
	}
}

func TestMatchDetailHandler_StatsPresentPredictionsAbsent(t *testing.T) {
	store = &fakeMatchStore{
		values: map[string]string{
			"match:m1:stats": `{"match_id":"m1"}`,
			// no "match:m1:predictions" key yet
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/matches/m1/predictions", nil)
	rec := httptest.NewRecorder()
	matchDetailHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 with a placeholder body, got %d", rec.Code)
	}

	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response is not valid JSON: %v", err)
	}
	if body["status"] != "no predictions yet" {
		t.Fatalf("expected a 'no predictions yet' placeholder, got %+v", body)
	}
}

func TestMatchDetailHandler_EmptyMatchID(t *testing.T) {
	store = &fakeMatchStore{}

	req := httptest.NewRequest(http.MethodGet, "/matches/", nil)
	rec := httptest.NewRecorder()
	matchDetailHandler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for an empty match ID, got %d", rec.Code)
	}
}

func TestMatchDetailHandler_RedisUnavailable(t *testing.T) {
	store = &fakeMatchStore{
		getErrForKey: map[string]error{
			"match:m1:stats": errors.New("redis unavailable"),
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/matches/m1", nil)
	rec := httptest.NewRecorder()
	matchDetailHandler(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 when Redis is unavailable, got %d", rec.Code)
	}
}

func TestHealthHandler(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	healthHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}
