package common

import (
	"strings"
	"testing"
)

func TestNewEventID_IsValidAndUnique(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		id := NewEventID()

		if err := ValidateEventID(id); err != nil {
			t.Fatalf("generated ID %q failed its own validation: %v", id, err)
		}
		if !strings.HasPrefix(id, "evt-") {
			t.Fatalf("generated ID %q is missing the evt- prefix", id)
		}
		if seen[id] {
			t.Fatalf("generated a duplicate event ID after %d iterations: %q", i, id)
		}
		seen[id] = true
	}
}

func TestValidateEventID(t *testing.T) {
	cases := []struct {
		name    string
		id      string
		wantErr bool
	}{
		{"typical generated ID", "evt-0123456789abcdef0123456789abcdef", false},
		{"client-supplied simple ID", "my-event-1", false},
		{"exactly at the length limit", strings.Repeat("a", MaxEventIDLength), false},

		{"empty", "", true},
		{"one over the length limit", strings.Repeat("a", MaxEventIDLength+1), true},
		// The characters below would let a crafted ID collide with or
		// escape its Redis key namespace (event:<id>:dedupe), or break
		// Redis Cluster hash-slot tags.
		{"contains colon", "evt:123", true},
		{"contains space", "evt 123", true},
		{"contains tab", "evt\t123", true},
		{"contains newline", "evt\n123", true},
		{"contains carriage return", "evt\r123", true},
		{"contains opening brace", "evt{123", true},
		{"contains closing brace", "evt}123", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateEventID(tc.id)
			if tc.wantErr && err == nil {
				t.Fatalf("expected %q to be rejected, but it was accepted", tc.id)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("expected %q to be accepted, got error: %v", tc.id, err)
			}
		})
	}
}

func TestDedupeKey_IsNamespacedAndInjectionSafe(t *testing.T) {
	if got, want := DedupeKey("evt-1"), "event:evt-1:dedupe"; got != want {
		t.Fatalf("DedupeKey = %q, want %q", got, want)
	}

	// Any ID that passes validation cannot introduce an extra ':'
	// separator, so it cannot be crafted to look like a different key.
	id := "evt-abc"
	if err := ValidateEventID(id); err != nil {
		t.Fatalf("precondition: %q should be valid: %v", id, err)
	}
	if n := strings.Count(DedupeKey(id), ":"); n != 2 {
		t.Fatalf("expected exactly 2 ':' separators in the key, got %d", n)
	}
}

func TestReservationState_String(t *testing.T) {
	// These strings are used as a Prometheus label value, so they must be
	// stable and low-cardinality.
	cases := map[ReservationState]string{
		ReservationAcquired:         "acquired",
		ReservationAlreadyProcessed: "already_processed",
		ReservationInFlight:         "in_flight",
	}
	for state, want := range cases {
		if got := state.String(); got != want {
			t.Fatalf("ReservationState(%d).String() = %q, want %q", int(state), got, want)
		}
	}
}
