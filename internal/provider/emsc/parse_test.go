package emsc

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/example/earthquake-service/internal/domain/earthquake"
)

func TestParseFDSN(t *testing.T) {
	data := fixture(t, "fdsn_feed.json")
	events, invalid, err := ParseFDSN(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || invalid != 1 {
		t.Fatalf("events=%d invalid=%d", len(events), invalid)
	}
	event := events[0]
	if event.Provider != ProviderName || event.ExternalID != "20260714_0000123" {
		t.Fatalf("unexpected identity: %#v", event)
	}
	if event.ObservationChannel != FDSNChannel || event.SolutionClass != earthquake.ConfirmedSolution {
		t.Fatalf("unexpected observation metadata: %#v", event)
	}
	if event.EventType == nil || *event.EventType != "earthquake" || event.Magnitude == nil || *event.Magnitude != 4.6 {
		t.Fatalf("unexpected normalized values: %#v", event)
	}
}

func TestParseStandingOrder(t *testing.T) {
	event, err := ParseStandingOrder(fixture(t, "standing_order_insert.json"))
	if err != nil {
		t.Fatal(err)
	}
	if event.ExternalID != "20260714_0000123" || event.ObservationChannel != WebSocketChannel {
		t.Fatalf("unexpected event: %#v", event)
	}
	if event.SolutionClass != earthquake.PreliminarySolution {
		t.Fatalf("solution=%q", event.SolutionClass)
	}
}

func TestParseStandingOrderDelete(t *testing.T) {
	data := fixture(t, "standing_order_insert.json")
	data = bytes.Replace(data, []byte(`"insert"`), []byte(`"delete"`), 1)
	event, err := ParseStandingOrder(data)
	if err != nil {
		t.Fatal(err)
	}
	if event.SolutionClass != earthquake.RetractedSolution {
		t.Fatalf("solution=%q", event.SolutionClass)
	}
	if !bytes.Contains(event.RawPayload, []byte(`"action": "delete"`)) {
		t.Fatalf("standing-order envelope was not preserved: %s", event.RawPayload)
	}
}

func TestParseStandingOrderRejectsUnknownAction(t *testing.T) {
	_, err := ParseStandingOrder([]byte(`{"action":"noop","data":{}}`))
	if err == nil {
		t.Fatal("expected an error")
	}
}

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "testdata", "emsc", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}
