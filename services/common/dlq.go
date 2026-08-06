package common

import "time"

// DLQRecord describes a message that could not be processed and was
// routed to a dead-letter topic instead of being silently dropped.
//
// Payload is the original message bytes verbatim. MatchEvent has no
// secret-bearing fields, so no redaction is applied here; a future
// message type that does carry sensitive fields would need to scrub
// Payload before it reaches this struct, not after.
type DLQRecord struct {
	OriginalTopic     string    `json:"original_topic"`
	OriginalPartition int       `json:"original_partition"`
	OriginalOffset    int64     `json:"original_offset"`
	Payload           []byte    `json:"payload"`
	FailureReason     string    `json:"failure_reason"`
	Attempts          int       `json:"attempts"`
	FailedAt          time.Time `json:"failed_at"`
	// TraceID is best-effort: populated when the failing message carried
	// an extractable trace context, empty otherwise.
	TraceID string `json:"trace_id,omitempty"`
}
