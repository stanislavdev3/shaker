package providerworker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/example/earthquake-service/internal/clock"
	"github.com/example/earthquake-service/internal/domain/earthquake"
	"github.com/example/earthquake-service/internal/eventstream"
	"github.com/example/earthquake-service/internal/kafka"
	"github.com/example/earthquake-service/internal/observability"
	"github.com/example/earthquake-service/internal/provider"
)

type Publisher interface {
	Publish(context.Context, kafka.Message) error
}

type Service struct {
	provider  provider.Provider
	publisher Publisher
	state     StateStore
	clock     clock.Clock
	log       *slog.Logger
	metrics   *observability.Metrics
}

func New(p provider.Provider, publisher Publisher, state StateStore, c clock.Clock, log *slog.Logger,
	metrics ...*observability.Metrics,
) *Service {
	service := &Service{provider: p, publisher: publisher, state: state, clock: c, log: log}
	if len(metrics) > 0 {
		service.metrics = metrics[0]
	}
	return service
}

func (s *Service) Poll(ctx context.Context) error {
	started := s.clock.Now()
	mode := "unknown"
	result := "error"
	fetched := 0
	invalid := 0
	defer func() {
		if s.metrics != nil {
			completedAt := s.clock.Now()
			s.metrics.ObserveProviderPoll(s.provider.Name(), mode, result, completedAt, completedAt.Sub(started), fetched, invalid)
		}
	}()
	state, err := s.loadState(ctx)
	if err != nil {
		return err
	}
	mode = "realtime"
	if !state.BaselineComplete {
		mode = "baseline"
	}
	events, metadata, err := s.provider.FetchRealtime(ctx, state.Validators)
	if err != nil {
		return err
	}
	fetched = len(events)
	invalid = metadata.InvalidCount
	for _, event := range events {
		if err := s.publish(ctx, event, mode, state.BaselineComplete); err != nil {
			return err
		}
	}
	if metadata.ETag != "" {
		state.Validators.ETag = metadata.ETag
	}
	if metadata.LastModified != "" {
		state.Validators.LastModified = metadata.LastModified
	}
	state.BaselineComplete = true
	state.Checkpoint = s.clock.Now().UTC()
	if err := s.state.Save(ctx, state); err != nil {
		return err
	}
	result = "success"
	s.log.Info("provider poll published", "provider", s.provider.Name(), "mode", mode,
		"fetched", len(events), "invalid", metadata.InvalidCount, "not_modified", metadata.NotModified)
	return nil
}

func (s *Service) PublishRealtime(ctx context.Context, event earthquake.Event) error {
	return s.publish(ctx, event, "realtime", true)
}

func (s *Service) Recover(ctx context.Context, overlap, chunk time.Duration) error {
	state, err := s.loadState(ctx)
	if err != nil {
		return err
	}
	now := s.clock.Now().UTC()
	if state.Checkpoint.IsZero() || now.Sub(state.Checkpoint) <= 24*time.Hour {
		return nil
	}
	from := state.Checkpoint.Add(-overlap)
	for start := from; start.Before(now); {
		end := start.Add(chunk)
		if end.After(now) {
			end = now
		}
		var cursor *string
		for {
			events, next, _, err := s.provider.FetchHistorical(ctx, start, end, cursor)
			if err != nil {
				return err
			}
			for _, event := range events {
				if err := s.publish(ctx, event, "recovery", true); err != nil {
					return err
				}
			}
			if next == nil {
				break
			}
			cursor = next
		}
		state.Checkpoint = end
		if err := s.state.Save(ctx, state); err != nil {
			return err
		}
		start = end
	}
	return nil
}

func (s *Service) Run(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		return errors.New("provider poll interval must be positive")
	}
	if err := s.Poll(ctx); err != nil {
		s.log.Error("provider poll failed", "provider", s.provider.Name(), "error", err)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.Poll(ctx); err != nil {
				s.log.Error("provider poll failed", "provider", s.provider.Name(), "error", err)
			}
		}
	}
}

func (s *Service) loadState(ctx context.Context) (State, error) {
	state, err := s.state.Load(ctx)
	if err != nil {
		return State{}, err
	}
	if state.Provider != "" && state.Provider != s.provider.Name() {
		return State{}, fmt.Errorf("provider state belongs to %q, not %q", state.Provider, s.provider.Name())
	}
	state.Provider = s.provider.Name()
	return state, nil
}

func (s *Service) publish(ctx context.Context, event earthquake.Event, mode string, baselineComplete bool) error {
	message, err := eventstream.NewProviderObservation(event, mode, baselineComplete, s.clock.Now())
	if err != nil {
		return err
	}
	payload, err := eventstream.Marshal(message)
	if err != nil {
		return err
	}
	if err := s.publisher.Publish(ctx, kafka.Message{
		Topic:   eventstream.ProviderObservationsTopic,
		Key:     event.Provider + ":" + event.ExternalID,
		Value:   payload,
		Headers: map[string]string{"schema": message.Schema, "message_id": message.MessageID.String()},
	}); err != nil {
		return err
	}
	if s.metrics != nil {
		s.metrics.ObserveProviderPublished(event.Provider, mode)
	}
	return nil
}
