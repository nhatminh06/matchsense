package common

import "fmt"

// Delivery semantics
//
// Kafka gives this pipeline at-least-once delivery: a consumer that
// crashes after handling a message but before committing its offset will
// see that message again on restart. Nothing in MatchSense provides
// exactly-once processing, and this file does not add it.
//
// What it does add is *idempotent handling* of those redeliveries: an
// event that has already been folded into a match's statistics is
// recognised by its EventID and skipped, so the aggregate stays correct
// even though the message was delivered more than once.
//
// Reservation states
//
// A naive "mark as seen, then process" approach loses events: if the
// process dies between marking and processing, the event is never
// aggregated and the marker suppresses the redelivery forever. To avoid
// that, a reservation moves through two states:
//
//	processing — claimed by a consumer, outcome not yet known
//	processed  — successfully folded into the match statistics
//
// A consumer that fails mid-processing releases its reservation, so the
// redelivery is reprocessed rather than silently dropped. If the consumer
// dies without releasing, the reservation's TTL expires and the event
// becomes eligible again — the TTL is the bound on how long a crash can
// suppress a redelivery.
//
// Trade-off: the deduplication window is the TTL, not "forever". A
// redelivery arriving after the TTL expires is aggregated a second time.
// The TTL is therefore sized to comfortably exceed the longest plausible
// redelivery delay (see DefaultDedupeTTL) rather than to bound memory
// tightly.

// ReservationState is the outcome of trying to claim an event ID.
type ReservationState int

const (
	// ReservationAcquired means this consumer now owns the event and
	// must process it, then call MarkProcessed or Release.
	ReservationAcquired ReservationState = iota
	// ReservationAlreadyProcessed means the event was already folded
	// into the statistics and must be skipped.
	ReservationAlreadyProcessed
	// ReservationInFlight means another consumer holds the reservation
	// and its outcome is not yet known. The safe action is to skip:
	// either the holder succeeds (and the event is counted once), or it
	// fails/crashes and the redelivery is reprocessed after release or
	// TTL expiry.
	ReservationInFlight
)

func (s ReservationState) String() string {
	switch s {
	case ReservationAcquired:
		return "acquired"
	case ReservationAlreadyProcessed:
		return "already_processed"
	case ReservationInFlight:
		return "in_flight"
	default:
		return fmt.Sprintf("unknown(%d)", int(s))
	}
}

// DedupeKey returns the Redis key holding the reservation for an event.
// Event IDs are validated (see ValidateEventID) before reaching here, so
// the key cannot escape this namespace.
func DedupeKey(eventID string) string {
	return "event:" + eventID + ":dedupe"
}
