package eventstream

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/example/earthquake-service/internal/domain/earthquake"
)

func TestProviderObservationRoundTripAndDeterministicIdentity(t *testing.T) {
	now := time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC)
	magnitude := 3.7
	event := earthquake.Event{
		Provider: "kndc", ExternalID: "6396", OccurredAt: now.Add(-time.Minute), SourceUpdatedAt: now,
		Latitude: 42.7936, Longitude: 74.6332, Magnitude: &magnitude, RawPayload: json.RawMessage(`{"id":"6396"}`),
		ObservationChannel: "kndc_alarm_bulletin", SolutionClass: earthquake.ConfirmedSolution,
	}
	first, err := NewProviderObservation(event, "realtime", true, now)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewProviderObservation(event, "realtime", true, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if first.MessageID != second.MessageID {
		t.Fatalf("message ids differ: %s %s", first.MessageID, second.MessageID)
	}
	encoded, err := Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalProviderObservation(encoded)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decoded.Event()
	if err != nil {
		t.Fatal(err)
	}
	if got.Provider != event.Provider || got.ExternalID != event.ExternalID || got.OccurredAt != event.OccurredAt ||
		string(got.RawPayload) != string(event.RawPayload) {
		t.Fatalf("event=%+v", got)
	}
}

func TestProviderObservationRejectsUnknownSchema(t *testing.T) {
	_, err := UnmarshalProviderObservation([]byte(`{"schema":"provider.observation.v2"}`))
	if !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("error=%v", err)
	}
}

func TestIncidentChangeIdentityIsPerVersion(t *testing.T) {
	now := time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC)
	previous := earthquake.Event{ID: [16]byte{1}, Provider: "emsc", ExternalID: "event", Version: 1, UpdatedAt: now.Add(-time.Minute)}
	event := earthquake.Event{ID: [16]byte{1}, Provider: "emsc", ExternalID: "event", Version: 2, UpdatedAt: now}
	first := NewIncidentChanged(earthquake.Change{Kind: earthquake.Updated, Previous: &previous, Current: event}, "realtime", true, now)
	second := NewIncidentChanged(earthquake.Change{Kind: earthquake.Updated, Current: event}, "realtime", true, now.Add(time.Minute))
	if first.MessageID != second.MessageID {
		t.Fatalf("message ids differ: %s %s", first.MessageID, second.MessageID)
	}
	event.Version++
	third := NewIncidentChanged(earthquake.Change{Kind: earthquake.Updated, Current: event}, "realtime", true, now)
	if first.MessageID == third.MessageID {
		t.Fatal("different incident versions share a message id")
	}
	encoded, err := Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalIncidentChanged(encoded)
	if err != nil {
		t.Fatal(err)
	}
	change, err := decoded.Change()
	if err != nil {
		t.Fatal(err)
	}
	if change.Previous == nil || change.Previous.Version != 1 || change.Current.Version != 2 || !decoded.NotificationsEligible {
		t.Fatalf("decoded change=%+v message=%+v", change, decoded)
	}
}
