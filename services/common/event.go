package common

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
)

// MatchEvent is a single in-match event (goal, shot, foul, card, etc.)
// produced by match-simulator, ingested by event-api, and consumed by
// event-processor.
type MatchEvent struct {
	// EventID uniquely identifies this event across the whole pipeline.
	// It is the deduplication key used by event-processor: two messages
	// carrying the same EventID describe the same real-world occurrence
	// and must only be aggregated once.
	//
	// Clients may supply their own EventID (useful when the client wants
	// its own retries to be idempotent). If a client omits it, event-api
	// generates one at ingestion — see NewEventID. It is therefore always
	// populated by the time an event reaches Kafka.
	EventID   string  `json:"event_id,omitempty"`
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

// MaxEventIDLength bounds a client-supplied event ID. Event IDs become
// part of a Redis key, so an unbounded value would let a caller drive
// arbitrary memory use in the deduplication store.
const MaxEventIDLength = 128

// NewEventID returns a fresh random event ID with an "evt-" prefix.
func NewEventID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand failing is not something this service can
		// meaningfully recover from or paper over with a weaker source:
		// a predictable ID would silently break deduplication.
		panic(fmt.Sprintf("common: cannot generate event ID: %v", err))
	}
	return "evt-" + hex.EncodeToString(b)
}

// ValidateEventID reports whether a client-supplied event ID is safe to
// use as part of a Redis key. Empty IDs are rejected here; callers that
// want to allow omission should generate one via NewEventID first.
func ValidateEventID(id string) error {
	if id == "" {
		return fmt.Errorf("event_id must not be empty")
	}
	if len(id) > MaxEventIDLength {
		return fmt.Errorf("event_id must be at most %d characters, got %d", MaxEventIDLength, len(id))
	}
	// Reject whitespace and Redis key-delimiter characters so an event ID
	// can never be crafted to collide with or escape its key namespace.
	if strings.ContainsAny(id, " \t\r\n:{}") {
		return fmt.Errorf("event_id must not contain whitespace, ':', '{' or '}'")
	}
	return nil
}
