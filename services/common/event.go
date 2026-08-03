package common

// MatchEvent is a single in-match event (goal, shot, foul, card, etc.)
// produced by match-simulator, ingested by event-api, and consumed by
// event-processor.
type MatchEvent struct {
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
