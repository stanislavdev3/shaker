package core

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/example/earthquake-service/internal/clock"
	"github.com/example/earthquake-service/internal/eventstream"
	"github.com/example/earthquake-service/internal/kafka"
	"github.com/example/earthquake-service/internal/observability"
	"github.com/example/earthquake-service/internal/repository/postgres"
)

type ObservationConsumer struct {
	consumer RecordConsumer
	repo     *postgres.Repository
	clock    clock.Clock
	log      *slog.Logger
	metrics  *observability.Metrics
}

type RecordConsumer interface {
	Next(context.Context) (kafka.Record, error)
	Commit(context.Context, kafka.Record) error
	AllowRebalance()
	Close()
}

func NewObservationConsumer(consumer RecordConsumer, repo *postgres.Repository, c clock.Clock,
	log *slog.Logger, metrics ...*observability.Metrics,
) *ObservationConsumer {
	processor := &ObservationConsumer{consumer: consumer, repo: repo, clock: c, log: log}
	if len(metrics) > 0 {
		processor.metrics = metrics[0]
	}
	return processor
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
		started := c.clock.Now()
		stats, processed, err := c.repo.ApplyProviderMessage(ctx, postgres.MessagePosition{
			Topic: record.Topic, Partition: record.Partition, Offset: record.Offset,
		}, message, c.clock.Now())
		if err != nil {
			if c.metrics != nil {
				c.metrics.ObserveCoreObservation(message.Observation.Provider, "error", c.clock.Now().Sub(started))
			}
			c.consumer.AllowRebalance()
			return fmt.Errorf("apply provider observation %s: %w", message.MessageID, err)
		}
		if err := c.consumer.Commit(ctx, record); err != nil {
			c.consumer.AllowRebalance()
			return fmt.Errorf("commit provider observation %s: %w", message.MessageID, err)
		}
		c.consumer.AllowRebalance()
		if c.metrics != nil {
			result := "duplicate"
			if processed {
				switch {
				case stats.Inserted > 0:
					result = "inserted"
				case stats.Updated > 0:
					result = "updated"
				default:
					result = "unchanged"
				}
			}
			c.metrics.ObserveCoreObservation(message.Observation.Provider, result, c.clock.Now().Sub(started))
		}
		c.log.Info("provider observation consumed", "message_id", message.MessageID,
			"provider", message.Observation.Provider, "external_id", message.Observation.ExternalID,
			"processed", processed, "inserted", stats.Inserted, "updated", stats.Updated,
			"unchanged", stats.Unchanged)
	}
}

func (c *ObservationConsumer) Close() { c.consumer.Close() }
