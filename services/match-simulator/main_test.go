package main

import "testing"

// generateMinuteEvents is driven by the global math/rand source and has no
// seed parameter, so these tests check structural/schema invariants across
// many iterations rather than asserting exact output — the function is not
// currently deterministic and reseeding it would be a bigger refactor than
// this test suite needs.

func TestGenerateMinuteEvents_SchemaAndBoundsAreAlwaysValid(t *testing.T) {
	const iterations = 500
	homeTeam, awayTeam := "Arsenal", "Manchester City"

	sawShot, sawGoal, sawFoul, sawCard, sawCorner := false, false, false, false, false

	for i := 0; i < iterations; i++ {
		minute := (i % 90) + 1
		events := generateMinuteEvents("m1", minute, homeTeam, awayTeam)

		for _, e := range events {
			if e.MatchID != "m1" {
				t.Fatalf("event has wrong match ID: %+v", e)
			}
			if e.Minute != minute {
				t.Fatalf("event minute %d does not match requested minute %d: %+v", e.Minute, minute, e)
			}
			if e.Team != homeTeam && e.Team != awayTeam {
				t.Fatalf("event team %q is neither home (%q) nor away (%q): %+v", e.Team, homeTeam, awayTeam, e)
			}
			if e.X < 0 || e.X > 100 || e.Y < 0 || e.Y > 100 {
				t.Fatalf("event coordinates out of the expected 0-100 pitch range: %+v", e)
			}
			if e.Timestamp == "" {
				t.Fatalf("event is missing a timestamp: %+v", e)
			}
			if e.Player == "" {
				t.Fatalf("event is missing a player: %+v", e)
			}

			switch e.EventType {
			case "shot":
				sawShot = true
				if e.Detail != "on_target" && e.Detail != "off_target" {
					t.Fatalf("shot event has unexpected detail %q: %+v", e.Detail, e)
				}
			case "goal":
				sawGoal = true
			case "foul":
				sawFoul = true
			case "card":
				sawCard = true
				if e.Detail != "yellow" && e.Detail != "red" {
					t.Fatalf("card event has unexpected detail %q: %+v", e.Detail, e)
				}
			case "corner":
				sawCorner = true
			default:
				t.Fatalf("unrecognized event type %q: %+v", e.EventType, e)
			}
		}
	}

	// Over 500 iterations at the documented per-minute probabilities
	// (~33% shot, ~28% foul, ~11% corner), every event type should have
	// appeared at least once. Goal/card are much rarer (chained
	// probabilities), so they're not asserted here to avoid test flakiness.
	if !sawShot || !sawFoul || !sawCorner {
		t.Fatalf("expected to see shot, foul, and corner events across %d iterations: shot=%v foul=%v corner=%v",
			iterations, sawShot, sawFoul, sawCorner)
	}
	_ = sawGoal
	_ = sawCard
}

func TestGenerateMinuteEvents_GoalOnlyFollowsOnTargetShot(t *testing.T) {
	// A goal should never appear in a minute's events without a
	// corresponding on-target shot for the same team/player earlier in
	// that same event slice (goals are only generated as a follow-on to
	// an on-target shot).
	for i := 0; i < 1000; i++ {
		events := generateMinuteEvents("m1", 10, "Arsenal", "Manchester City")

		for idx, e := range events {
			if e.EventType != "goal" {
				continue
			}
			if idx == 0 {
				t.Fatalf("goal event appeared without a preceding shot: %+v", events)
			}
			prev := events[idx-1]
			if prev.EventType != "shot" || prev.Detail != "on_target" || prev.Team != e.Team || prev.Player != e.Player {
				t.Fatalf("goal event not preceded by a matching on-target shot: goal=%+v preceding=%+v", e, prev)
			}
		}
	}
}

func TestPickTeamAndPlayer_AlwaysReturnsValidPair(t *testing.T) {
	homeTeam, awayTeam := "Arsenal", "Manchester City"
	homeSeen, awaySeen := false, false

	for i := 0; i < 200; i++ {
		team, player := pickTeamAndPlayer(homeTeam, awayTeam)

		if team != homeTeam && team != awayTeam {
			t.Fatalf("pickTeamAndPlayer returned an unknown team: %q", team)
		}
		if player == "" {
			t.Fatalf("pickTeamAndPlayer returned an empty player name")
		}

		if team == homeTeam {
			homeSeen = true
			found := false
			for _, p := range homePlayers {
				if p == player {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("player %q returned for home team is not in homePlayers", player)
			}
		} else {
			awaySeen = true
			found := false
			for _, p := range awayPlayers {
				if p == player {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("player %q returned for away team is not in awayPlayers", player)
			}
		}
	}

	if !homeSeen || !awaySeen {
		t.Fatalf("expected both home and away teams to be picked across 200 iterations: home=%v away=%v", homeSeen, awaySeen)
	}
}
