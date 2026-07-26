package providerworker

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/example/earthquake-service/internal/domain/earthquake"
	"github.com/example/earthquake-service/internal/eventstream"
	"github.com/example/earthquake-service/internal/kafka"
	"github.com/example/earthquake-service/internal/provider"
)

type providerStub struct {
	events []earthquake.Event
	meta   provider.FetchMetadata
}

func (p *providerStub) Name() string { return "kndc" }
func (p *providerStub) FetchRealtime(context.Context, provider.CacheValidators) ([]earthquake.Event, provider.FetchMetadata, error) {
	return p.events, p.meta, nil
}
func (p *providerStub) FetchHistorical(context.Context, time.Time, time.Time, *string) ([]earthquake.Event, *string, provider.FetchMetadata, error) {
	return nil, nil, provider.FetchMetadata{}, nil
}

type publisherStub struct{ messages []kafka.Message }

func (p *publisherStub) Publish(_ context.Context, message kafka.Message) error {
	p.messages = append(p.messages, message)
	return nil
}

type stateStoreStub struct{ state State }

func (s *stateStoreStub) Load(context.Context) (State, error) { return s.state, nil }
func (s *stateStoreStub) Save(_ context.Context, state State) error {
	s.state = state
	return nil
}

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

func TestPollPublishesBaselineThenRealtime(t *testing.T) {
	now := time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC)
	magnitude := 3.7
	source := &providerStub{events: []earthquake.Event{{
		Provider: "kndc", ExternalID: "6396", OccurredAt: now.Add(-time.Minute), SourceUpdatedAt: now,
		Latitude: 42.7936, Longitude: 74.6332, Magnitude: &magnitude, RawPayload: json.RawMessage(`{"id":"6396"}`),
		ObservationChannel: "kndc_alarm_bulletin", SolutionClass: earthquake.ConfirmedSolution,
	}}, meta: provider.FetchMetadata{ETag: "etag-1"}}
	publisher := &publisherStub{}
	state := &stateStoreStub{}
	service := New(source, publisher, state, fixedClock{now: now}, slog.Default())
	if err := service.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := service.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(publisher.messages) != 2 || !state.state.BaselineComplete || state.state.Validators.ETag != "etag-1" {
		t.Fatalf("messages=%d state=%+v", len(publisher.messages), state.state)
	}
	first, err := eventstream.UnmarshalProviderObservation(publisher.messages[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	second, err := eventstream.UnmarshalProviderObservation(publisher.messages[1].Value)
	if err != nil {
		t.Fatal(err)
	}
	if first.Mode != "baseline" || first.BaselineComplete || second.Mode != "realtime" || !second.BaselineComplete {
		t.Fatalf("first=%+v second=%+v", first, second)
	}
	if first.MessageID != second.MessageID {
		t.Fatalf("duplicate observation IDs differ: %s %s", first.MessageID, second.MessageID)
	}
}

func TestFileStateStoreRoundTrip(t *testing.T) {
	path := t.TempDir() + "/provider/state.json"
	store, err := NewFileStateStore(path)
	if err != nil {
		t.Fatal(err)
	}
	want := State{Provider: "emsc", BaselineComplete: true, Checkpoint: time.Now().UTC()}
	if err := store.Save(context.Background(), want); err != nil {
		t.Fatal(err)
	}
	got, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Provider != want.Provider || got.BaselineComplete != want.BaselineComplete || !got.Checkpoint.Equal(want.Checkpoint) {
		t.Fatalf("state=%+v", got)
	}
}
