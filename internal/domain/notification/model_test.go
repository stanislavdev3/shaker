package notification

import (
	"testing"
	"time"

	"github.com/example/earthquake-service/internal/domain/earthquake"
)

func ptr[T any](v T) *T { return &v }
func TestTriggers(t *testing.T) {
	now := time.Date(2026, 6, 29, 10, 0, 0, 0, time.UTC)
	s := Subscription{Status: "active", MinimumMagnitude: ptr(5.0), NotifyOnNew: true, NotifyOnThresholdCrossing: true, NotifyOnTsunamiChange: true, NotifyOnAlertIncrease: true, MaximumEventAge: 2 * time.Hour}
	current := earthquake.Event{OccurredAt: now.Add(-time.Minute), Magnitude: ptr(5.4), Tsunami: ptr(true), AlertLevel: ptr("orange")}
	old := current
	old.Magnitude = ptr(4.9)
	old.Tsunami = ptr(false)
	old.AlertLevel = ptr("yellow")
	got := Triggers(s, &old, current, "realtime", now, true)
	if len(got) != 3 {
		t.Fatalf("got %v", got)
	}
	if got[0] != MagnitudeThresholdCrossed || got[1] != TsunamiActivated || got[2] != AlertLevelIncreased {
		t.Fatalf("got %v", got)
	}
}
func TestNewEventRequiresRealtimeBaselineAndFreshness(t *testing.T) {
	now := time.Now()
	s := Subscription{Status: "active", NotifyOnNew: true, MaximumEventAge: 2 * time.Hour}
	e := earthquake.Event{OccurredAt: now.Add(-time.Hour)}
	if len(Triggers(s, nil, e, "baseline", now, false)) != 0 {
		t.Fatal("baseline notified")
	}
	if len(Triggers(s, nil, e, "backfill", now, true)) != 0 {
		t.Fatal("backfill notified")
	}
	if got := Triggers(s, nil, e, "realtime", now, true); len(got) != 1 || got[0] != NewEvent {
		t.Fatalf("got %v", got)
	}
	e.OccurredAt = now.Add(-3 * time.Hour)
	if len(Triggers(s, nil, e, "realtime", now, true)) != 0 {
		t.Fatal("old event notified")
	}
}
func TestAlertSeverity(t *testing.T) {
	for i, v := range []string{"none", "green", "yellow", "orange", "red"} {
		if got := AlertSeverity(&v); got != i {
			t.Fatalf("%s=%d", v, got)
		}
	}
}
