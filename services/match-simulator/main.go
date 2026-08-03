package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/nhatminh06/matchsense/common"
)

type MatchEvent = common.MatchEvent

var (
	homePlayers = []string{"Saka", "Rice", "Odegaard", "Havertz", "Saliba", "White", "Timber", "Raya", "Trossard", "Martinelli", "Gabriel"}
	awayPlayers = []string{"Haaland", "De Bruyne", "Foden", "Rodri", "Stones", "Walker", "Gvardiol", "Ederson", "Grealish", "Silva", "Doku"}
)

func main() {
	eventAPIURL := common.GetEnv("EVENT_API_URL", "http://localhost:8080")
	matchID := common.GetEnv("MATCH_ID", fmt.Sprintf("match_%d", time.Now().Unix()))
	homeTeam := common.GetEnv("HOME_TEAM", "Arsenal")
	awayTeam := common.GetEnv("AWAY_TEAM", "Manchester City")
	speedStr := common.GetEnv("SPEED", "2") // seconds per minute of match time
	healthPort := common.GetEnv("HEALTH_PORT", "8082")

	speed := 2
	fmt.Sscanf(speedStr, "%d", &speed)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "service": "match-simulator"})
	})
	go func() {
		if err := http.ListenAndServe(":"+healthPort, mux); err != nil {
			log.Printf("WARNING: health server stopped: %v", err)
		}
	}()

	log.Printf("Simulating: %s vs %s (match: %s)", homeTeam, awayTeam, matchID)
	log.Printf("Speed: 1 match-minute = %d real-seconds", speed)
	log.Printf("Event API: %s", eventAPIURL)

	client := &http.Client{Timeout: 5 * time.Second}

	for minute := 1; minute <= 90; minute++ {
		events := generateMinuteEvents(matchID, minute, homeTeam, awayTeam)

		for _, event := range events {
			data, err := json.Marshal(event)
			if err != nil {
				log.Printf("ERROR marshaling event: %v", err)
				continue
			}
			resp, err := client.Post(eventAPIURL+"/events", "application/json", bytes.NewBuffer(data))
			if err != nil {
				log.Printf("ERROR sending event: %v", err)
				continue
			}
			resp.Body.Close()

			log.Printf("min:%d %s %s - %s (%s)",
				event.Minute, event.EventType, event.Team, event.Player, event.Detail)
		}

		if minute == 45 {
			log.Println("=== HALF TIME ===")
			time.Sleep(time.Duration(speed) * time.Second)
		}

		time.Sleep(time.Duration(speed) * time.Second)
	}

	log.Println("=== FULL TIME ===")
}

func generateMinuteEvents(matchID string, minute int, homeTeam, awayTeam string) []MatchEvent {
	events := []MatchEvent{}

	// Each minute has a chance of various events
	r := rand.Float64()

	// ~30 shots per match = ~0.33 per minute
	if r < 0.33 {
		team, player := pickTeamAndPlayer(homeTeam, awayTeam)
		onTarget := "off_target"
		if rand.Float64() < 0.4 {
			onTarget = "on_target"
		}

		// Shot location: attacking third
		x := 75.0 + rand.Float64()*25.0
		y := 20.0 + rand.Float64()*60.0
		if team == awayTeam {
			x = 100.0 - x
		}

		events = append(events, MatchEvent{
			MatchID:   matchID,
			Minute:    minute,
			EventType: "shot",
			Team:      team,
			Player:    player,
			X:         x,
			Y:         y,
			Detail:    onTarget,
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		})

		// ~10% of shots on target are goals
		if onTarget == "on_target" && rand.Float64() < 0.25 {
			events = append(events, MatchEvent{
				MatchID:   matchID,
				Minute:    minute,
				EventType: "goal",
				Team:      team,
				Player:    player,
				X:         x,
				Y:         y,
				Detail:    "open_play",
				Timestamp: time.Now().UTC().Format(time.RFC3339),
			})
		}
	}

	// ~25 fouls per match
	if rand.Float64() < 0.28 {
		team, player := pickTeamAndPlayer(homeTeam, awayTeam)
		events = append(events, MatchEvent{
			MatchID:   matchID,
			Minute:    minute,
			EventType: "foul",
			Team:      team,
			Player:    player,
			X:         rand.Float64() * 100,
			Y:         rand.Float64() * 100,
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		})

		// ~5% of fouls result in a yellow card
		if rand.Float64() < 0.15 {
			events = append(events, MatchEvent{
				MatchID:   matchID,
				Minute:    minute,
				EventType: "card",
				Team:      team,
				Player:    player,
				X:         rand.Float64() * 100,
				Y:         rand.Float64() * 100,
				Detail:    "yellow",
				Timestamp: time.Now().UTC().Format(time.RFC3339),
			})
		}
	}

	// ~10 corners per match
	if rand.Float64() < 0.11 {
		team, player := pickTeamAndPlayer(homeTeam, awayTeam)
		events = append(events, MatchEvent{
			MatchID:   matchID,
			Minute:    minute,
			EventType: "corner",
			Team:      team,
			Player:    player,
			X:         100,
			Y:         0,
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		})
	}

	return events
}

func pickTeamAndPlayer(homeTeam, awayTeam string) (string, string) {
	if rand.Float64() < 0.5 {
		return homeTeam, homePlayers[rand.Intn(len(homePlayers))]
	}
	return awayTeam, awayPlayers[rand.Intn(len(awayPlayers))]
}
