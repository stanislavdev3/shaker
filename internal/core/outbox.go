package core

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/example/earthquake-service/internal/clock"
	"github.com/example/earthquake-service/internal/kafka"
	"github.com/example/earthquake-service/internal/repository/postgres"
)

type OutboxRelay struct {
	repo                   *postgres.Repository
	publisher              MessagePublisher
	clock                  clock.Clock
	log                    *slog.Logger
	workerID               string
	batch                  int
	lockTimeout, pollDelay time.Duration
}

type MessagePublisher interface {
	Publish(context.Context, kafka.Message) error
}

func NewOutboxRelay(repo *postgres.Repository, publisher MessagePublisher, c clock.Clock, log *slog.Logger,
	workerID string, batch int, lockTimeout, pollDelay time.Duration,
) *OutboxRelay {
	return &OutboxRelay{repo: repo, publisher: publisher, clock: c, log: log, workerID: workerID,
		batch: batch, lockTimeout: lockTimeout, pollDelay: pollDelay}
}

func (r *OutboxRelay) Run(ctx context.Context) {
	ticker := time.NewTicker(r.pollDelay)
	defer ticker.Stop()
	for {
		r.process(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *OutboxRelay) process(ctx context.Context) {
	messages, err := r.repo.ClaimCoreOutbox(ctx, r.workerID, r.batch, r.lockTimeout, r.clock.Now())
	if err != nil {
		r.log.Error("claim core outbox", "error", err)
		return
	}
	for _, message := range messages {
		headers := map[string]string{"schema": message.Schema, "message_id": message.ID.String()}
		if len(message.Headers) > 0 {
			if err := json.Unmarshal(message.Headers, &headers); err != nil {
				r.fail(ctx, message, err)
				continue
			}
			if headers == nil {
				headers = make(map[string]string, 2)
			}
			headers["schema"] = message.Schema
			headers["message_id"] = message.ID.String()
		}
		err := r.publisher.Publish(ctx, kafka.Message{
			Topic: message.Topic, Key: message.Key, Value: message.Payload, Headers: headers,
		})
		if err != nil {
			r.fail(ctx, message, err)
			continue
		}
		if err := r.repo.CompleteCoreOutbox(context.WithoutCancel(ctx), message.ID, r.workerID, r.clock.Now()); err != nil {
			r.log.Error("complete core outbox", "message_id", message.ID, "error", err)
		}
	}
}

func (r *OutboxRelay) fail(ctx context.Context, message postgres.OutboxMessage, publishErr error) {
	now := r.clock.Now()
	delay := time.Second << min(message.AttemptCount, 8)
	if err := r.repo.FailCoreOutbox(context.WithoutCancel(ctx), message.ID, r.workerID, publishErr.Error(),
		now.Add(delay), now); err != nil {
		r.log.Error("fail core outbox", "message_id", message.ID, "error", err)
	}
}
