package notification

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/example/earthquake-service/internal/domain/earthquake"
	"github.com/example/earthquake-service/internal/eventstream"
	"github.com/example/earthquake-service/internal/kafka"
	"github.com/example/earthquake-service/internal/repository/postgres"
)

type incidentConsumerClock struct{ now time.Time }

func (c incidentConsumerClock) Now() time.Time { return c.now }

type stubIncidentRecordConsumer struct {
	record     kafka.Record
	cancel     context.CancelFunc
	committed  bool
	rebalanced bool
	closed     bool
}

func (c *stubIncidentRecordConsumer) Next(ctx context.Context) (kafka.Record, error) {
	if c.committed {
		<-ctx.Done()
		return kafka.Record{}, ctx.Err()
	}
	return c.record, nil
}

func (c *stubIncidentRecordConsumer) Commit(context.Context, kafka.Record) error {
	c.committed = true
	c.cancel()
	return nil
}

func (c *stubIncidentRecordConsumer) AllowRebalance() { c.rebalanced = true }
func (c *stubIncidentRecordConsumer) Close()          { c.closed = true }

type stubIncidentRepository struct {
	position  postgres.MessagePosition
	message   eventstream.IncidentChangedV1
	received  time.Time
	processed bool
}

func (r *stubIncidentRepository) ApplyIncidentMessage(_ context.Context, position postgres.MessagePosition,
	message eventstream.IncidentChangedV1, received time.Time,
) (bool, error) {
	r.position, r.message, r.received = position, message, received
	r.processed = true
	return true, nil
}

func TestIncidentConsumerAppliesBeforeCommitting(t *testing.T) {
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	message := eventstream.NewIncidentChanged(earthquake.Change{
		Kind:    earthquake.Inserted,
		Current: earthquake.Event{ID: [16]byte{1}, Provider: "kndc", ExternalID: "42", Version: 1},
	}, "realtime", true, now)
	payload, err := eventstream.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	consumer := &stubIncidentRecordConsumer{cancel: cancel, record: kafka.Record{
		Message:   kafka.Message{Topic: eventstream.IncidentChangesTopic, Key: message.Incident.ID.String(), Value: payload},
		Partition: 3, Offset: 17,
	}}
	repository := &stubIncidentRepository{}
	processor := NewIncidentConsumer(consumer, repository, incidentConsumerClock{now: now},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	err = processor.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	if !repository.processed || !consumer.committed || !consumer.rebalanced {
		t.Fatalf("processed=%v committed=%v rebalanced=%v", repository.processed, consumer.committed, consumer.rebalanced)
	}
	if repository.position.Topic != eventstream.IncidentChangesTopic || repository.position.Partition != 3 ||
		repository.position.Offset != 17 || repository.received != now {
		t.Fatalf("position=%+v received=%v", repository.position, repository.received)
	}
}
