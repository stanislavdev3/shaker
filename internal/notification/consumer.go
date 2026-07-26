package notification

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/example/earthquake-service/internal/clock"
	"github.com/example/earthquake-service/internal/eventstream"
	"github.com/example/earthquake-service/internal/kafka"
	"github.com/example/earthquake-service/internal/repository/postgres"
)

type IncidentConsumer struct {
	consumer IncidentRecordConsumer
	repo     IncidentRepository
	clock    clock.Clock
	log      *slog.Logger
}

type IncidentRecordConsumer interface {
	Next(context.Context) (kafka.Record, error)
	Commit(context.Context, kafka.Record) error
	AllowRebalance()
	Close()
}

type IncidentRepository interface {
	ApplyIncidentMessage(context.Context, postgres.MessagePosition, eventstream.IncidentChangedV1, time.Time) (bool, error)
}

func NewIncidentConsumer(consumer IncidentRecordConsumer, repo IncidentRepository, c clock.Clock,
	log *slog.Logger,
) *IncidentConsumer {
	return &IncidentConsumer{consumer: consumer, repo: repo, clock: c, log: log}
}

func (c *IncidentConsumer) Run(ctx context.Context) error {
	for {
		record, err := c.consumer.Next(ctx)
		if err != nil {
			return err
		}
		message, err := eventstream.UnmarshalIncidentChanged(record.Value)
		if err != nil {
			c.consumer.AllowRebalance()
			return fmt.Errorf("decode incident change at %s/%d/%d: %w",
				record.Topic, record.Partition, record.Offset, err)
		}
		expectedKey := message.Incident.ID.String()
		if record.Key != expectedKey {
			c.consumer.AllowRebalance()
			return fmt.Errorf("incident change key %q does not match %q", record.Key, expectedKey)
		}
		processed, err := c.repo.ApplyIncidentMessage(ctx, postgres.MessagePosition{
			Topic: record.Topic, Partition: record.Partition, Offset: record.Offset,
		}, message, c.clock.Now())
		if err != nil {
			c.consumer.AllowRebalance()
			return fmt.Errorf("apply incident change %s: %w", message.MessageID, err)
		}
		if err := c.consumer.Commit(ctx, record); err != nil {
			c.consumer.AllowRebalance()
			return fmt.Errorf("commit incident change %s: %w", message.MessageID, err)
		}
		c.consumer.AllowRebalance()
		c.log.Info("incident change consumed", "message_id", message.MessageID,
			"earthquake_id", message.Incident.ID, "earthquake_version", message.Incident.Version,
			"processed", processed, "notifications_eligible", message.NotificationsEligible)
	}
}

func (c *IncidentConsumer) Close() { c.consumer.Close() }
