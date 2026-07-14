package ingestion

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/example/earthquake-service/internal/clock"
	"github.com/example/earthquake-service/internal/domain/earthquake"
	"github.com/example/earthquake-service/internal/provider"
	"github.com/example/earthquake-service/internal/repository/postgres"
)

const (
	stateETag         = "realtime_etag"
	stateLastModified = "realtime_last_modified"
	stateCheckpoint   = "realtime_checkpoint"
	stateBaseline     = "baseline_completed"
)

type Service struct {
	provider  provider.Provider
	repo      *postgres.Repository
	clock     clock.Clock
	log       *slog.Logger
	batchSize int
}

func New(p provider.Provider, repo *postgres.Repository, c clock.Clock, log *slog.Logger) *Service {
	return &Service{provider: p, repo: repo, clock: c, log: log, batchSize: 250}
}

// ApplyRealtime persists one push observation. Push streams do not replay a
// catalogue snapshot, so a newly received event is always eligible for alerts.
func (s *Service) ApplyRealtime(ctx context.Context, event earthquake.Event) error {
	now := s.clock.Now()
	runID, err := s.repo.StartRun(ctx, s.provider.Name(), "realtime", now)
	if err != nil {
		return err
	}
	stats, applyErr := s.repo.ApplyBatch(ctx, []earthquake.Event{event}, "realtime", true, now)
	if applyErr == nil {
		applyErr = s.repo.SetState(ctx, s.provider.Name(), stateCheckpoint, now.Format(time.RFC3339Nano), now)
	}
	status := "succeeded"
	if applyErr != nil {
		status = "failed"
	}
	metadata := map[string]any{"channel": event.EffectiveObservationChannel(), "external_id": event.ExternalID}
	if finishErr := s.repo.FinishRun(context.WithoutCancel(ctx), runID, status, stats, metadata, applyErr, s.clock.Now()); finishErr != nil {
		s.log.Error("finish push ingestion run", "error", finishErr, "ingestion_run_id", runID)
		if applyErr == nil {
			return finishErr
		}
	}
	return applyErr
}

func (s *Service) Poll(ctx context.Context) error {
	now := s.clock.Now()
	baselineValue, baseline, err := s.repo.State(ctx, s.provider.Name(), stateBaseline)
	if err != nil {
		return err
	}
	baseline = baseline && baselineValue == "true"
	mode := "realtime"
	if !baseline {
		mode = "baseline"
	}
	runID, err := s.repo.StartRun(ctx, s.provider.Name(), mode, now)
	if err != nil {
		return err
	}
	stats := postgres.RunStats{}
	meta := map[string]any{}
	finish := func(runErr error) {
		status := "succeeded"
		if runErr != nil {
			status = "failed"
		}
		if err := s.repo.FinishRun(context.WithoutCancel(ctx), runID, status, stats, meta, runErr, s.clock.Now()); err != nil {
			s.log.Error("finish ingestion run", "error", err, "ingestion_run_id", runID)
		}
	}
	etag, _, _ := s.repo.State(ctx, s.provider.Name(), stateETag)
	modified, _, _ := s.repo.State(ctx, s.provider.Name(), stateLastModified)
	events, fetched, err := s.provider.FetchRealtime(ctx, provider.CacheValidators{ETag: etag, LastModified: modified})
	if err != nil {
		finish(err)
		return err
	}
	stats.Fetched = len(events)
	stats.Invalid = fetched.InvalidCount
	meta["not_modified"] = fetched.NotModified
	for start := 0; start < len(events); start += s.batchSize {
		end := start + s.batchSize
		if end > len(events) {
			end = len(events)
		}
		part, err := s.repo.ApplyBatch(ctx, events[start:end], mode, baseline, now)
		stats.Inserted += part.Inserted
		stats.Updated += part.Updated
		stats.Unchanged += part.Unchanged
		if err != nil {
			finish(err)
			return err
		}
	}
	if fetched.ETag != "" {
		if err := s.repo.SetState(ctx, s.provider.Name(), stateETag, fetched.ETag, now); err != nil {
			finish(err)
			return err
		}
	}
	if fetched.LastModified != "" {
		if err := s.repo.SetState(ctx, s.provider.Name(), stateLastModified, fetched.LastModified, now); err != nil {
			finish(err)
			return err
		}
	}
	if err := s.repo.SetState(ctx, s.provider.Name(), stateCheckpoint, now.Format(time.RFC3339Nano), now); err != nil {
		finish(err)
		return err
	}
	if !baseline {
		if err := s.repo.SetState(ctx, s.provider.Name(), stateBaseline, "true", now); err != nil {
			finish(err)
			return err
		}
	}
	finish(nil)
	s.log.Info("ingestion poll completed", "provider", s.provider.Name(), "ingestion_run_id", runID, "mode", mode,
		"fetched", stats.Fetched, "inserted", stats.Inserted, "updated", stats.Updated, "unchanged", stats.Unchanged, "invalid", stats.Invalid)
	return nil
}

func (s *Service) Run(ctx context.Context, interval time.Duration) {
	if err := s.Poll(ctx); err != nil {
		s.log.Error("ingestion poll failed", "provider", s.provider.Name(), "error", err)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.Poll(ctx); err != nil {
				s.log.Error("ingestion poll failed", "provider", s.provider.Name(), "error", err)
			}
		}
	}
}

// Backfill processes bounded windows and persists the next window after each success.
func (s *Service) RunBackfill(ctx context.Context, from, to time.Time, chunk time.Duration, mode string) error {
	if !from.Before(to) {
		return fmt.Errorf("from must be before to")
	}
	key := "backfill_checkpoint:" + from.UTC().Format(time.RFC3339) + ":" + to.UTC().Format(time.RFC3339)
	if v, ok, err := s.repo.State(ctx, s.provider.Name(), key); err != nil {
		return err
	} else if ok {
		if t, e := time.Parse(time.RFC3339Nano, v); e == nil && t.After(from) {
			from = t
		}
	}
	for start := from; start.Before(to); {
		end := start.Add(chunk)
		if end.After(to) {
			end = to
		}
		runID, err := s.repo.StartRun(ctx, s.provider.Name(), mode, s.clock.Now())
		if err != nil {
			return err
		}
		stats := postgres.RunStats{}
		var cursor *string
		for {
			events, next, meta, err := s.provider.FetchHistorical(ctx, start, end, cursor)
			stats.Fetched += len(events)
			stats.Invalid += meta.InvalidCount
			if err == nil {
				for i := 0; i < len(events); i += s.batchSize {
					j := i + s.batchSize
					if j > len(events) {
						j = len(events)
					}
					var p postgres.RunStats
					p, err = s.repo.ApplyBatch(ctx, events[i:j], mode, true, s.clock.Now())
					stats.Inserted += p.Inserted
					stats.Updated += p.Updated
					stats.Unchanged += p.Unchanged
					if err != nil {
						break
					}
				}
			}
			if err != nil {
				if finishErr := s.repo.FinishRun(context.WithoutCancel(ctx), runID, "failed", stats, map[string]any{"from": start, "to": end}, err, s.clock.Now()); finishErr != nil {
					s.log.Error("finish failed backfill run", "error", finishErr, "ingestion_run_id", runID)
				}
				return err
			}
			if next == nil {
				break
			}
			cursor = next
		}
		if err := s.repo.SetState(ctx, s.provider.Name(), key, end.Format(time.RFC3339Nano), s.clock.Now()); err != nil {
			return err
		}
		if err := s.repo.FinishRun(ctx, runID, "succeeded", stats, map[string]any{"from": start, "to": end}, nil, s.clock.Now()); err != nil {
			return err
		}
		start = end
	}
	return nil
}

// RecoveryRange returns a historical recovery interval only after a prolonged outage.
func (s *Service) RecoveryRange(ctx context.Context, overlap time.Duration) (*time.Time, *time.Time, error) {
	value, ok, err := s.repo.State(ctx, s.provider.Name(), stateCheckpoint)
	if err != nil || !ok {
		return nil, nil, err
	}
	checkpoint, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid checkpoint: %w", err)
	}
	now := s.clock.Now()
	if now.Sub(checkpoint) <= 24*time.Hour {
		return nil, nil, nil
	}
	from := checkpoint.Add(-overlap)
	return &from, &now, nil
}
