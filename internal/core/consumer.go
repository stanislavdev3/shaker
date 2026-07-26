package core

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/example/earthquake-service/internal/clock"
	"github.com/example/earthquake-service/internal/eventstream"
	"github.com/example/earthquake-service/internal/kafka"
	"github.com/example/earthquake-service/internal/repository/postgres"
)

type ObservationConsumer struct {
	consumer RecordConsumer
	repo     *postgres.Repository
	clock    clock.Clock
	log      *slog.Logger
}

type RecordConsumer interface {
	Next(context.Context) (kafka.Record, error)
	Commit(context.Context, kafka.Record) error
	AllowRebalance()
	Close()
}

func NewObservationConsumer(consumer RecordConsumer, repo *postgres.Repository, c clock.Clock,
	log *slog.Logger,
) *ObservationConsumer {
	return &ObservationConsumer{consumer: consumer, repo: repo, clock: c, log: log}
}

func (c *ObservationConsumer) Run(ctx context.Context) error {
	for {
		record, err := c.consumer.Next(ctx)
		if err != nil {
			return err
		}
		message, err := eventstream.UnmarshalProviderObservation(record.Value)
		if err != nil {
			c.consumer.AllowRebalance()
			return fmt.Errorf("decode provider observation at %s/%d/%d: %w",
				record.Topic, record.Partition, record.Offset, err)
		}
		expectedKey := message.Observation.Provider + ":" + message.Observation.ExternalID
		if record.Key != expectedKey {
			c.consumer.AllowRebalance()
			return fmt.Errorf("provider observation key %q does not match %q", record.Key, expectedKey)
		}
		stats, processed, err := c.repo.ApplyProviderMessage(ctx, postgres.MessagePosition{
			Topic: record.Topic, Partition: record.Partition, Offset: record.Offset,
		}, message, c.clock.Now())
		if err != nil {
			c.consumer.AllowRebalance()
			return fmt.Errorf("apply provider observation %s: %w", message.MessageID, err)
		}
		if err := c.consumer.Commit(ctx, record); err != nil {
			c.consumer.AllowRebalance()
			return fmt.Errorf("commit provider observation %s: %w", message.MessageID, err)
		}
		c.consumer.AllowRebalance()
		c.log.Info("provider observation consumed", "message_id", message.MessageID,
			"provider", message.Observation.Provider, "external_id", message.Observation.ExternalID,
			"processed", processed, "inserted", stats.Inserted, "updated", stats.Updated,
			"unchanged", stats.Unchanged)
	}
}

func (c *ObservationConsumer) Close() { c.consumer.Close() }
