package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/example/earthquake-service/internal/domain/earthquake"
	"github.com/example/earthquake-service/internal/domain/notification"
	"github.com/example/earthquake-service/internal/domain/shaking"
)

var ErrNotFound = errors.New("not found")

type Repository struct{ Pool *pgxpool.Pool }

func Open(ctx context.Context, databaseURL string, min, max int) (*Repository, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	cfg.MinConns, cfg.MaxConns = int32(min), int32(max)
	cfg.ConnConfig.RuntimeParams["application_name"] = "earthquake-service"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Repository{Pool: pool}, nil
}

func (r *Repository) Ready(ctx context.Context) error {
	if err := r.Pool.Ping(ctx); err != nil {
		return err
	}
	var postgis bool
	if err := r.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname='postgis')`).Scan(&postgis); err != nil {
		return err
	}
	if !postgis {
		return errors.New("PostGIS migration is not applied")
	}
	return nil
}

type RunStats struct{ Fetched, Inserted, Updated, Unchanged, Invalid int }

func (r *Repository) StartRun(ctx context.Context, provider, mode string, now time.Time) (uuid.UUID, error) {
	var id uuid.UUID
	err := r.Pool.QueryRow(ctx, `INSERT INTO ingestion_runs(id,provider,mode,started_at,status,metadata)
		VALUES(gen_random_uuid(),$1,$2,$3,'running','{}') RETURNING id`, provider, mode, now).Scan(&id)
	return id, err
}

func (r *Repository) FinishRun(ctx context.Context, id uuid.UUID, status string, s RunStats, metadata map[string]any, runErr error, now time.Time) error {
	var msg *string
	if runErr != nil {
		v := runErr.Error()
		if len(v) > 2000 {
			v = v[:2000]
		}
		msg = &v
	}
	meta, _ := json.Marshal(metadata)
	_, err := r.Pool.Exec(ctx, `UPDATE ingestion_runs SET finished_at=$2,status=$3,fetched_count=$4,inserted_count=$5,
		updated_count=$6,unchanged_count=$7,invalid_count=$8,error_message=$9,metadata=$10 WHERE id=$1`,
		id, now, status, s.Fetched, s.Inserted, s.Updated, s.Unchanged, s.Invalid, msg, meta)
	return err
}

func (r *Repository) State(ctx context.Context, provider, key string) (string, bool, error) {
	var value string
	err := r.Pool.QueryRow(ctx, `SELECT state_value FROM provider_state WHERE provider=$1 AND state_key=$2`, provider, key).Scan(&value)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	return value, err == nil, err
}

func setState(ctx context.Context, tx pgx.Tx, provider, key, value string, now time.Time) error {
	_, err := tx.Exec(ctx, `INSERT INTO provider_state(provider,state_key,state_value,updated_at) VALUES($1,$2,$3,$4)
		ON CONFLICT(provider,state_key) DO UPDATE SET state_value=EXCLUDED.state_value,updated_at=EXCLUDED.updated_at`,
		provider, key, value, now)
	return err
}

func (r *Repository) SetState(ctx context.Context, provider, key, value string, now time.Time) error {
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := setState(ctx, tx, provider, key, value, now); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *Repository) ApplyBatch(ctx context.Context, events []earthquake.Event, mode string, baselineComplete bool, now time.Time) (RunStats, error) {
	tx, err := r.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return RunStats{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	stats, err := applyBatch(ctx, tx, events, mode, baselineComplete, true, false, now)
	if err != nil {
		return stats, err
	}
	if err := tx.Commit(ctx); err != nil {
		return stats, err
	}
	return stats, nil
}

func applyBatch(ctx context.Context, tx pgx.Tx, events []earthquake.Event, mode string, baselineComplete,
	processNotifications, publishChanges bool, now time.Time,
) (RunStats, error) {
	stats := RunStats{Fetched: len(events)}
	for _, event := range events {
		change, err := applyEvent(ctx, tx, event, now)
		if err != nil {
			return stats, fmt.Errorf("%s/%s: %w", event.Provider, event.ExternalID, err)
		}
		switch change.Kind {
		case earthquake.Inserted:
			stats.Inserted++
		case earthquake.Updated:
			stats.Updated++
		case earthquake.Unchanged, earthquake.Stale:
			stats.Unchanged++
		}
		if processNotifications && (change.Kind == earthquake.Inserted || change.Kind == earthquake.Updated) && mode == "realtime" {
			if err := createDeliveries(ctx, tx, change, mode, baselineComplete, now); err != nil {
				return stats, err
			}
		}
		if processNotifications && change.Kind == earthquake.Updated {
			if err := refreshTelegramAlertMessages(ctx, tx, change.Current, now); err != nil {
				return stats, err
			}
		}
		if publishChanges && (change.Kind == earthquake.Inserted || change.Kind == earthquake.Updated) {
			if err := enqueueIncidentChange(ctx, tx, change, mode, baselineComplete, now); err != nil {
				return stats, err
			}
		}
	}
	return stats, nil
}

func applyEvent(ctx context.Context, tx pgx.Tx, incoming earthquake.Event, now time.Time) (earthquake.Change, error) {
	var existing earthquake.Event
	var sourceID uuid.UUID
	var oldHash []byte
	var sourceVersion int64
	var oldChannel string
	var oldSolution earthquake.SolutionClass
	err := tx.QueryRow(ctx, `SELECT e.id,e.preferred_source,e.preferred_external_id,e.occurred_at,e.source_updated_at,e.latitude,e.longitude,e.depth_km,e.magnitude,
		e.magnitude_type,e.place,e.title,e.status,e.event_type,e.alert_level,e.tsunami,e.significance,e.felt_reports,
		e.cdi,e.mmi,e.station_count,e.azimuthal_gap,e.minimum_distance,e.rms,e.source_url,e.detail_url,e.version,
		e.first_seen_at,e.last_seen_at,e.created_at,e.updated_at,e.lifecycle,s.id,s.payload_hash,s.version,
		s.latest_observation_channel,s.solution_class
		FROM earthquake_source_records s JOIN earthquakes e ON e.id=s.earthquake_id
		WHERE s.provider=$1 AND s.external_id=$2 FOR UPDATE`,
		incoming.Provider, incoming.ExternalID).Scan(scanEvent(&existing, &sourceID, &oldHash, &sourceVersion, &oldChannel, &oldSolution)...)
	hash, hashErr := incoming.CanonicalPayloadHash()
	if hashErr != nil {
		return earthquake.Change{}, fmt.Errorf("canonical payload hash: %w", hashErr)
	}
	channel := incoming.EffectiveObservationChannel()
	solution := incoming.EffectiveSolutionClass()
	if errors.Is(err, pgx.ErrNoRows) {
		decision, decisionErr := correlationDecision(ctx, tx, incoming)
		if decisionErr != nil {
			return earthquake.Change{}, decisionErr
		}
		if decision.Match != nil {
			return insertCorrelatedSource(ctx, tx, incoming, hash[:], channel, solution, decision, now)
		}
		incoming.Lifecycle = earthquake.ResolveLifecycle([]earthquake.SolutionClass{solution})
		provenance := canonicalProvenance(incoming, 1)
		err = tx.QueryRow(ctx, `INSERT INTO earthquakes(id,preferred_source,preferred_external_id,occurred_at,source_updated_at,
			latitude,longitude,depth_km,location,magnitude,magnitude_type,place,title,status,event_type,alert_level,tsunami,
			significance,felt_reports,cdi,mmi,station_count,azimuthal_gap,minimum_distance,rms,source_url,detail_url,
			version,first_seen_at,last_seen_at,created_at,updated_at,lifecycle,canonical_provenance)
			VALUES(gen_random_uuid(),$1,$2,$3,$4,$5,$6,$7,ST_SetSRID(ST_MakePoint($6,$5),4326)::geography,$8,$9,$10,$11,$12,$13,$14,$15,
			$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,1,$26,$26,$26,$26,$27,$28)
			ON CONFLICT(preferred_source,preferred_external_id) DO NOTHING RETURNING id`,
			incoming.Provider, incoming.ExternalID, incoming.OccurredAt, incoming.SourceUpdatedAt, incoming.Latitude, incoming.Longitude,
			incoming.DepthKM, incoming.Magnitude, incoming.MagnitudeType, incoming.Place, incoming.Title, incoming.Status, incoming.EventType,
			incoming.AlertLevel, incoming.Tsunami, incoming.Significance, incoming.FeltReports, incoming.CDI, incoming.MMI, incoming.StationCount,
			incoming.AzimuthalGap, incoming.MinimumDistance, incoming.RMS, incoming.SourceURL, incoming.DetailURL, now,
			incoming.Lifecycle, provenance).Scan(&incoming.ID)
		if errors.Is(err, pgx.ErrNoRows) {
			return applyEvent(ctx, tx, incoming, now)
		}
		if err != nil {
			return earthquake.Change{}, err
		}
		err = tx.QueryRow(ctx, `INSERT INTO earthquake_source_records(id,earthquake_id,provider,external_id,source_updated_at,
			payload_hash,raw_payload,source_url,detail_url,version,first_seen_at,last_seen_at,created_at,updated_at,
			latest_observation_channel,solution_class)
			VALUES(gen_random_uuid(),$1,$2,$3,$4,$5,$6,$7,$8,1,$9,$9,$9,$9,$10,$11) RETURNING id`,
			incoming.ID, incoming.Provider, incoming.ExternalID, incoming.SourceUpdatedAt, hash[:], incoming.RawPayload,
			incoming.SourceURL, incoming.DetailURL, now,
			channel, solution).Scan(&sourceID)
		if err != nil {
			return earthquake.Change{}, err
		}
		if err := insertProviderObservation(ctx, tx, sourceID, 1, channel, solution, incoming.SourceUpdatedAt, hash[:], incoming.RawPayload, now); err != nil {
			return earthquake.Change{}, err
		}
		associationAlgorithm := "identity-v1"
		associationEvidence := json.RawMessage(`{"reason":"first provider identity"}`)
		if len(decision.Ranked) > 0 {
			associationAlgorithm = earthquake.ProductionCorrelationPolicy().Version
			outcome := "below_acceptance_threshold"
			if decision.Ambiguous {
				outcome = "ambiguous_candidates"
			}
			associationEvidence, _ = json.Marshal(map[string]any{
				"reason": outcome, "ranked": decision.Ranked,
			})
		}
		_, err = tx.Exec(ctx, `INSERT INTO earthquake_source_associations(id,source_record_id,earthquake_id,method,
			confidence,algorithm_version,evidence,active,associated_at)
			VALUES(gen_random_uuid(),$1,$2,'new_incident',1,$3,$4,TRUE,$5)`,
			sourceID, incoming.ID, associationAlgorithm, associationEvidence, now)
		if err != nil {
			return earthquake.Change{}, err
		}
		incoming.Version = 1
		incoming.FirstSeenAt = now
		incoming.LastSeenAt = now
		incoming.CreatedAt = now
		incoming.UpdatedAt = now
		return earthquake.Change{Kind: earthquake.Inserted, Current: incoming, SourceRecordID: sourceID}, nil
	}
	if err != nil {
		return earthquake.Change{}, err
	}
	if incoming.SourceUpdatedAt.Before(existing.SourceUpdatedAt) {
		if err := insertProviderObservation(ctx, tx, sourceID, sourceVersion, channel, solution, incoming.SourceUpdatedAt, hash[:], incoming.RawPayload, now); err != nil {
			return earthquake.Change{}, err
		}
		promoted := earthquake.StrongerSolution(oldSolution, solution)
		_, err = tx.Exec(ctx, `UPDATE earthquake_source_records SET solution_class=$2,
			source_url=COALESCE($3,source_url),detail_url=COALESCE($4,detail_url),last_seen_at=$5,updated_at=$5 WHERE id=$1`,
			sourceID, promoted, incoming.SourceURL, incoming.DetailURL, now)
		if err != nil {
			return earthquake.Change{}, err
		}
		return applyLifecycleChange(ctx, tx, existing, sourceID, incoming, now, earthquake.Stale)
	}
	if incoming.SourceUpdatedAt.Equal(existing.SourceUpdatedAt) && bytesEqual(oldHash, hash[:]) {
		if err := insertProviderObservation(ctx, tx, sourceID, sourceVersion, channel, solution, incoming.SourceUpdatedAt, hash[:], incoming.RawPayload, now); err != nil {
			return earthquake.Change{}, err
		}
		promoted := earthquake.StrongerSolution(oldSolution, solution)
		latestChannel := channel
		if promoted != solution {
			latestChannel = oldChannel
		}
		_, err = tx.Exec(ctx, `UPDATE earthquake_source_records SET latest_observation_channel=$2,solution_class=$3,
			source_url=COALESCE($4,source_url),detail_url=COALESCE($5,detail_url),last_seen_at=$6,updated_at=$6 WHERE id=$1`,
			sourceID, latestChannel, promoted, incoming.SourceURL, incoming.DetailURL, now)
		if err != nil {
			return earthquake.Change{}, err
		}
		return applyLifecycleChange(ctx, tx, existing, sourceID, incoming, now, earthquake.Unchanged)
	}
	if solution != earthquake.RetractedSolution && oldSolution.Valid() &&
		earthquake.StrongerSolution(oldSolution, solution) == oldSolution && solution != oldSolution {
		if err := insertProviderObservation(ctx, tx, sourceID, sourceVersion, channel, solution, incoming.SourceUpdatedAt, hash[:], incoming.RawPayload, now); err != nil {
			return earthquake.Change{}, err
		}
		_, err = tx.Exec(ctx, `UPDATE earthquake_source_records SET source_url=COALESCE($2,source_url),
			detail_url=COALESCE($3,detail_url),last_seen_at=$4,updated_at=$4 WHERE id=$1`,
			sourceID, incoming.SourceURL, incoming.DetailURL, now)
		if err != nil {
			return earthquake.Change{}, err
		}
		return applyLifecycleChange(ctx, tx, existing, sourceID, incoming, now, earthquake.Unchanged)
	}
	sourceVersion++
	_, err = tx.Exec(ctx, `UPDATE earthquake_source_records SET source_updated_at=$2,payload_hash=$3,raw_payload=$4,
		source_url=COALESCE($5,source_url),detail_url=COALESCE($6,detail_url),version=$7,
		latest_observation_channel=$8,solution_class=$9,last_seen_at=$10,updated_at=$10 WHERE id=$1`,
		sourceID, incoming.SourceUpdatedAt, hash[:], incoming.RawPayload, incoming.SourceURL, incoming.DetailURL,
		sourceVersion, channel, solution, now)
	if err != nil {
		return earthquake.Change{}, err
	}
	if err := insertProviderObservation(ctx, tx, sourceID, sourceVersion, channel, solution, incoming.SourceUpdatedAt, hash[:], incoming.RawPayload, now); err != nil {
		return earthquake.Change{}, err
	}
	if existing.Provider != incoming.Provider || existing.ExternalID != incoming.ExternalID {
		preferredSolution, preferredErr := incidentPreferredSolution(ctx, tx, existing.ID)
		if preferredErr != nil {
			return earthquake.Change{}, preferredErr
		}
		if !earthquake.PreferCanonicalSource(existing.Provider, preferredSolution, incoming.Provider, solution) {
			return applyLifecycleChange(ctx, tx, existing, sourceID, incoming, now, earthquake.Updated)
		}
	}
	return updateCanonicalFromSource(ctx, tx, existing, incoming, sourceID, sourceVersion, now, nil)
}

func updateCanonicalFromSource(ctx context.Context, tx pgx.Tx, existing, incoming earthquake.Event,
	sourceID uuid.UUID, sourceVersion int64, now time.Time, additionalChanged map[string]any) (earthquake.Change, error) {
	var err error
	incoming.ID = existing.ID
	incoming.Version = existing.Version + 1
	incoming.FirstSeenAt = existing.FirstSeenAt
	incoming.LastSeenAt = now
	incoming.CreatedAt = existing.CreatedAt
	incoming.UpdatedAt = now
	incoming.Lifecycle, err = incidentLifecycle(ctx, tx, incoming.ID)
	if err != nil {
		return earthquake.Change{}, err
	}
	changed := changedFields(existing, incoming)
	for key, value := range additionalChanged {
		changed[key] = value
	}
	if existing.Lifecycle != incoming.Lifecycle {
		changed["lifecycle"] = map[string]any{"from": existing.Lifecycle, "to": incoming.Lifecycle}
	}
	changedJSON, _ := json.Marshal(changed)
	provenance := canonicalProvenance(incoming, sourceVersion)
	_, err = tx.Exec(ctx, `UPDATE earthquakes SET preferred_source=$2,preferred_external_id=$3,
		occurred_at=$4,source_updated_at=$5,latitude=$6,longitude=$7,depth_km=$8,
		location=ST_SetSRID(ST_MakePoint($7,$6),4326)::geography,magnitude=$9,magnitude_type=$10,place=$11,title=$12,
		status=$13,event_type=$14,alert_level=$15,tsunami=$16,significance=$17,felt_reports=$18,cdi=$19,mmi=$20,
		station_count=$21,azimuthal_gap=$22,minimum_distance=$23,rms=$24,source_url=$25,detail_url=$26,version=$27,
		last_seen_at=$28,updated_at=$28,lifecycle=$29,canonical_provenance=$30 WHERE id=$1`, incoming.ID,
		incoming.Provider, incoming.ExternalID, incoming.OccurredAt, incoming.SourceUpdatedAt, incoming.Latitude,
		incoming.Longitude, incoming.DepthKM, incoming.Magnitude, incoming.MagnitudeType, incoming.Place, incoming.Title, incoming.Status,
		incoming.EventType, incoming.AlertLevel, incoming.Tsunami, incoming.Significance, incoming.FeltReports, incoming.CDI, incoming.MMI,
		incoming.StationCount, incoming.AzimuthalGap, incoming.MinimumDistance, incoming.RMS, incoming.SourceURL, incoming.DetailURL,
		incoming.Version, now, incoming.Lifecycle, provenance)
	if err != nil {
		return earthquake.Change{}, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO earthquake_revisions(id,earthquake_id,source_record_id,version,source_updated_at,
		changed_fields,raw_payload,created_at) VALUES(gen_random_uuid(),$1,$2,$3,$4,$5,$6,$7)`,
		incoming.ID, sourceID, incoming.Version, incoming.SourceUpdatedAt, changedJSON, incoming.RawPayload, now)
	return earthquake.Change{Kind: earthquake.Updated, Previous: &existing, Current: incoming, ChangedFields: changed, SourceRecordID: sourceID}, err
}

func correlationDecision(ctx context.Context, tx pgx.Tx, incoming earthquake.Event) (earthquake.CorrelationDecision, error) {
	if !correlatedCatalogProvider(incoming.Provider) || incoming.Magnitude == nil {
		return earthquake.CorrelationDecision{}, nil
	}
	// Serialize creation of previously unseen cross-provider identities. The lock
	// is held only by the current ingestion transaction and prevents symmetric
	// Cross-provider catalogue arrivals from creating two incidents concurrently.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(2084096761)`); err != nil {
		return earthquake.CorrelationDecision{}, err
	}
	policy := earthquake.ProductionCorrelationPolicy()
	rows, err := tx.Query(ctx, `SELECT e.id,e.preferred_source,e.occurred_at,e.latitude,e.longitude,e.magnitude,e.depth_km
		FROM earthquakes e
		WHERE e.occurred_at BETWEEN $1::timestamptz-$2::interval AND $1::timestamptz+$2::interval
			AND e.magnitude IS NOT NULL
			AND ST_DWithin(e.location,ST_SetSRID(ST_MakePoint($3,$4),4326)::geography,$5)
			AND EXISTS (
				SELECT 1 FROM earthquake_source_associations a
				JOIN earthquake_source_records s ON s.id=a.source_record_id
				WHERE a.earthquake_id=e.id AND a.active AND s.provider IN ('emsc','usgs','geofon','kndc') AND s.provider<>$6
			)
			AND NOT EXISTS (
				SELECT 1 FROM earthquake_source_associations a
				JOIN earthquake_source_records s ON s.id=a.source_record_id
				WHERE a.earthquake_id=e.id AND a.active AND s.provider=$6
			)
		ORDER BY abs(EXTRACT(EPOCH FROM e.occurred_at-$1)),e.id
		LIMIT 100 FOR UPDATE OF e`, incoming.OccurredAt, policy.MaximumTimeDelta.String(), incoming.Longitude,
		incoming.Latitude, policy.MaximumDistanceKM*1000, incoming.Provider)
	if err != nil {
		return earthquake.CorrelationDecision{}, err
	}
	defer rows.Close()
	var candidates []earthquake.CorrelationCandidate
	for rows.Next() {
		var candidate earthquake.CorrelationCandidate
		if err := rows.Scan(&candidate.IncidentID, &candidate.Event.Provider, &candidate.Event.OccurredAt,
			&candidate.Event.Latitude, &candidate.Event.Longitude, &candidate.Event.Magnitude,
			&candidate.Event.DepthKM); err != nil {
			return earthquake.CorrelationDecision{}, err
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return earthquake.CorrelationDecision{}, err
	}
	decision := policy.Correlate(incoming, candidates)
	if len(candidates) == 100 {
		decision.Match = nil
		decision.Ambiguous = true
	}
	return decision, nil
}

func correlatedCatalogProvider(provider string) bool {
	switch provider {
	case "emsc", "usgs", "geofon", "kndc":
		return true
	default:
		return false
	}
}

func insertCorrelatedSource(ctx context.Context, tx pgx.Tx, incoming earthquake.Event, hash []byte,
	channel string, solution earthquake.SolutionClass, decision earthquake.CorrelationDecision,
	now time.Time) (earthquake.Change, error) {
	policy := earthquake.ProductionCorrelationPolicy()
	match := decision.Match
	if match == nil {
		return earthquake.Change{}, errors.New("correlated source requires a match")
	}
	existing, preferredSolution, err := loadIncidentWithPreferredSource(ctx, tx, match.IncidentID)
	if err != nil {
		return earthquake.Change{}, err
	}
	var sourceID uuid.UUID
	err = tx.QueryRow(ctx, `INSERT INTO earthquake_source_records(id,earthquake_id,provider,external_id,source_updated_at,
		payload_hash,raw_payload,source_url,detail_url,version,first_seen_at,last_seen_at,created_at,updated_at,
		latest_observation_channel,solution_class)
		VALUES(gen_random_uuid(),$1,$2,$3,$4,$5,$6,$7,$8,1,$9,$9,$9,$9,$10,$11) RETURNING id`,
		existing.ID, incoming.Provider, incoming.ExternalID, incoming.SourceUpdatedAt, hash, incoming.RawPayload,
		incoming.SourceURL, incoming.DetailURL, now, channel, solution).Scan(&sourceID)
	if err != nil {
		return earthquake.Change{}, err
	}
	if err := insertProviderObservation(ctx, tx, sourceID, 1, channel, solution,
		incoming.SourceUpdatedAt, hash, incoming.RawPayload, now); err != nil {
		return earthquake.Change{}, err
	}
	evidence, _ := json.Marshal(map[string]any{
		"policy": map[string]any{
			"maximum_time_delta_seconds": policy.MaximumTimeDelta.Seconds(),
			"maximum_distance_km":        policy.MaximumDistanceKM,
			"maximum_magnitude_diff":     policy.MaximumMagnitudeDiff,
			"maximum_depth_diff_km":      policy.MaximumDepthDiffKM,
			"acceptance_threshold":       policy.AcceptanceThreshold,
			"ambiguity_margin":           policy.AmbiguityMargin,
		},
		"selected": match,
		"ranked":   decision.Ranked,
	})
	if _, err := tx.Exec(ctx, `INSERT INTO earthquake_source_associations(id,source_record_id,earthquake_id,method,
		confidence,algorithm_version,evidence,active,associated_at)
		VALUES(gen_random_uuid(),$1,$2,'heuristic',$3,$4,$5,TRUE,$6)`,
		sourceID, existing.ID, match.Score, policy.Version, evidence, now); err != nil {
		return earthquake.Change{}, err
	}
	associationChange := map[string]any{"source_association": map[string]any{
		"provider": incoming.Provider, "external_id": incoming.ExternalID,
		"method": "heuristic", "score": match.Score, "algorithm_version": policy.Version,
	}}
	if earthquake.PreferCanonicalSource(existing.Provider, preferredSolution, incoming.Provider, solution) {
		return updateCanonicalFromSource(ctx, tx, existing, incoming, sourceID, 1, now, associationChange)
	}
	return updateIncidentAssociation(ctx, tx, existing, incoming, sourceID, associationChange, now)
}

func loadIncidentWithPreferredSource(ctx context.Context, tx pgx.Tx, id uuid.UUID) (earthquake.Event, earthquake.SolutionClass, error) {
	var event earthquake.Event
	var sourceID uuid.UUID
	var hash []byte
	var sourceVersion int64
	var channel string
	var solution earthquake.SolutionClass
	err := tx.QueryRow(ctx, `SELECT e.id,e.preferred_source,e.preferred_external_id,e.occurred_at,e.source_updated_at,
		e.latitude,e.longitude,e.depth_km,e.magnitude,e.magnitude_type,e.place,e.title,e.status,e.event_type,
		e.alert_level,e.tsunami,e.significance,e.felt_reports,e.cdi,e.mmi,e.station_count,e.azimuthal_gap,
		e.minimum_distance,e.rms,e.source_url,e.detail_url,e.version,e.first_seen_at,e.last_seen_at,e.created_at,
		e.updated_at,e.lifecycle,s.id,s.payload_hash,s.version,s.latest_observation_channel,s.solution_class
		FROM earthquakes e JOIN earthquake_source_records s
			ON s.earthquake_id=e.id AND s.provider=e.preferred_source AND s.external_id=e.preferred_external_id
		WHERE e.id=$1 FOR UPDATE OF e`, id).Scan(scanEvent(&event, &sourceID, &hash, &sourceVersion, &channel, &solution)...)
	return event, solution, err
}

func incidentPreferredSolution(ctx context.Context, tx pgx.Tx, id uuid.UUID) (earthquake.SolutionClass, error) {
	var solution earthquake.SolutionClass
	err := tx.QueryRow(ctx, `SELECT s.solution_class FROM earthquakes e JOIN earthquake_source_records s
		ON s.earthquake_id=e.id AND s.provider=e.preferred_source AND s.external_id=e.preferred_external_id
		WHERE e.id=$1`, id).Scan(&solution)
	return solution, err
}

func updateIncidentAssociation(ctx context.Context, tx pgx.Tx, existing, incoming earthquake.Event,
	sourceID uuid.UUID, changed map[string]any, now time.Time) (earthquake.Change, error) {
	current := existing
	current.Version++
	current.LastSeenAt = now
	current.UpdatedAt = now
	lifecycle, err := incidentLifecycle(ctx, tx, existing.ID)
	if err != nil {
		return earthquake.Change{}, err
	}
	current.Lifecycle = lifecycle
	if existing.Lifecycle != lifecycle {
		changed["lifecycle"] = map[string]any{"from": existing.Lifecycle, "to": lifecycle}
	}
	changedJSON, _ := json.Marshal(changed)
	if _, err := tx.Exec(ctx, `UPDATE earthquakes SET lifecycle=$2,version=$3,last_seen_at=$4,updated_at=$4 WHERE id=$1`,
		existing.ID, lifecycle, current.Version, now); err != nil {
		return earthquake.Change{}, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO earthquake_revisions(id,earthquake_id,source_record_id,version,source_updated_at,
		changed_fields,raw_payload,created_at) VALUES(gen_random_uuid(),$1,$2,$3,$4,$5,$6,$7)`,
		existing.ID, sourceID, current.Version, incoming.SourceUpdatedAt, changedJSON, incoming.RawPayload, now)
	return earthquake.Change{Kind: earthquake.Updated, Previous: &existing, Current: current,
		ChangedFields: changed, SourceRecordID: sourceID}, err
}

func scanEvent(e *earthquake.Event, sourceID *uuid.UUID, hash *[]byte, sourceVersion *int64, channel *string, solution *earthquake.SolutionClass) []any {
	return []any{&e.ID, &e.Provider, &e.ExternalID, &e.OccurredAt, &e.SourceUpdatedAt, &e.Latitude, &e.Longitude, &e.DepthKM, &e.Magnitude, &e.MagnitudeType,
		&e.Place, &e.Title, &e.Status, &e.EventType, &e.AlertLevel, &e.Tsunami, &e.Significance, &e.FeltReports, &e.CDI, &e.MMI,
		&e.StationCount, &e.AzimuthalGap, &e.MinimumDistance, &e.RMS, &e.SourceURL, &e.DetailURL, &e.Version, &e.FirstSeenAt,
		&e.LastSeenAt, &e.CreatedAt, &e.UpdatedAt, &e.Lifecycle, sourceID, hash, sourceVersion, channel, solution}
}

func insertProviderObservation(ctx context.Context, tx pgx.Tx, sourceID uuid.UUID, sourceVersion int64, channel string,
	solution earthquake.SolutionClass, sourceUpdatedAt time.Time, hash []byte, raw json.RawMessage, now time.Time) error {
	_, err := tx.Exec(ctx, `INSERT INTO provider_observations(id,source_record_id,source_version,channel,solution_class,
		source_updated_at,payload_hash,raw_payload,received_at)
		VALUES(gen_random_uuid(),$1,$2,$3,$4,$5,$6,$7,$8)
		ON CONFLICT(source_record_id,channel,solution_class,source_updated_at,payload_hash) DO NOTHING`,
		sourceID, sourceVersion, channel, solution, sourceUpdatedAt, hash, raw, now)
	return err
}

func incidentLifecycle(ctx context.Context, tx pgx.Tx, earthquakeID uuid.UUID) (earthquake.Lifecycle, error) {
	rows, err := tx.Query(ctx, `SELECT s.solution_class
		FROM earthquake_source_associations a
		JOIN earthquake_source_records s ON s.id=a.source_record_id
		WHERE a.earthquake_id=$1 AND a.active`, earthquakeID)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var solutions []earthquake.SolutionClass
	for rows.Next() {
		var solution string
		if err := rows.Scan(&solution); err != nil {
			return "", err
		}
		solutions = append(solutions, earthquake.SolutionClass(solution))
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return earthquake.ResolveLifecycle(solutions), nil
}

func applyLifecycleChange(ctx context.Context, tx pgx.Tx, existing earthquake.Event, sourceID uuid.UUID,
	incoming earthquake.Event, now time.Time, unchangedKind earthquake.ChangeKind) (earthquake.Change, error) {
	lifecycle, err := incidentLifecycle(ctx, tx, existing.ID)
	if err != nil {
		return earthquake.Change{}, err
	}
	if lifecycle == existing.Lifecycle {
		_, err = tx.Exec(ctx, `UPDATE earthquakes SET last_seen_at=$2,updated_at=$2 WHERE id=$1`, existing.ID, now)
		return earthquake.Change{Kind: unchangedKind, Previous: &existing, Current: existing, SourceRecordID: sourceID}, err
	}
	current := existing
	current.Lifecycle = lifecycle
	current.Version++
	current.LastSeenAt = now
	current.UpdatedAt = now
	changed := map[string]any{"lifecycle": map[string]any{"from": existing.Lifecycle, "to": lifecycle}}
	changedJSON, _ := json.Marshal(changed)
	_, err = tx.Exec(ctx, `UPDATE earthquakes SET lifecycle=$2,version=$3,last_seen_at=$4,updated_at=$4 WHERE id=$1`,
		existing.ID, lifecycle, current.Version, now)
	if err != nil {
		return earthquake.Change{}, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO earthquake_revisions(id,earthquake_id,source_record_id,version,source_updated_at,
		changed_fields,raw_payload,created_at) VALUES(gen_random_uuid(),$1,$2,$3,$4,$5,$6,$7)`,
		existing.ID, sourceID, current.Version, incoming.SourceUpdatedAt, changedJSON, incoming.RawPayload, now)
	return earthquake.Change{Kind: earthquake.Updated, Previous: &existing, Current: current, ChangedFields: changed, SourceRecordID: sourceID}, err
}

func canonicalProvenance(event earthquake.Event, sourceVersion int64) json.RawMessage {
	source := map[string]any{
		"provider":       event.Provider,
		"external_id":    event.ExternalID,
		"source_version": sourceVersion,
		"channel":        event.EffectiveObservationChannel(),
	}
	fields := []string{
		"occurred_at", "source_updated_at", "latitude", "longitude", "depth_km", "magnitude", "magnitude_type",
		"place", "title", "status", "event_type", "alert_level", "tsunami", "significance", "felt_reports", "cdi",
		"mmi", "station_count", "azimuthal_gap", "minimum_distance", "rms", "source_url", "detail_url",
	}
	provenance := make(map[string]any, len(fields))
	for _, field := range fields {
		provenance[field] = source
	}
	encoded, _ := json.Marshal(provenance)
	return encoded
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
func changedFields(a, b earthquake.Event) map[string]any {
	// Values are marshaled through JSON to compare nullable fields consistently.
	aj, _ := json.Marshal(a)
	bj, _ := json.Marshal(b)
	var am, bm map[string]any
	_ = json.Unmarshal(aj, &am)
	_ = json.Unmarshal(bj, &bm)
	out := map[string]any{}
	for k, v := range bm {
		if k == "id" || k == "version" || k == "first_seen_at" || k == "last_seen_at" || k == "created_at" || k == "updated_at" {
			continue
		}
		x, _ := json.Marshal(am[k])
		y, _ := json.Marshal(v)
		if !bytesEqual(x, y) {
			out[k] = map[string]any{"from": am[k], "to": v}
		}
	}
	return out
}

func createDeliveries(ctx context.Context, tx pgx.Tx, change earthquake.Change, mode string, baseline bool, now time.Time) error {
	candidateMinimumMMI := notification.IntensityDecisionBoundary(shaking.MinimumSupportedMMI)
	candidateRadiusKM := shaking.CandidateRadiusKM(change.Current.Magnitude, change.Current.DepthKM, candidateMinimumMMI)
	type matchingCounters struct {
		intensityCandidates, intensityEvaluations, notify, belowThreshold, estimateErrors, triggers int
	}
	var counters matchingCounters
	rows, err := tx.Query(ctx, `SELECT id,name,status,channel,webhook_url,encrypted_webhook_secret,minimum_magnitude,
		maximum_magnitude,minimum_intensity,notification_language,subscription_kind,
		center_latitude,center_longitude,radius_km,tsunami_only,allowed_alert_levels,allowed_event_types,
		notify_on_new,notify_on_threshold_crossing,notify_on_tsunami_change,notify_on_alert_increase,
		EXTRACT(EPOCH FROM maximum_event_age)::bigint,telegram_chat_id,
		CASE WHEN area IS NULL THEN NULL ELSE ST_Distance(area,ST_SetSRID(ST_MakePoint($1,$2),4326)::geography)/1000 END,
		created_at,updated_at FROM notification_subscriptions WHERE status='active' AND
		((minimum_intensity IS NOT NULL AND area IS NOT NULL AND $3>0 AND
			ST_DWithin(area,ST_SetSRID(ST_MakePoint($1,$2),4326)::geography,$3*1000))
		OR (minimum_intensity IS NULL AND (area IS NULL OR (radius_km IS NOT NULL AND
			ST_DWithin(area,ST_SetSRID(ST_MakePoint($1,$2),4326)::geography,radius_km*1000)))))`,
		change.Current.Longitude, change.Current.Latitude, candidateRadiusKM)
	if err != nil {
		return err
	}
	type candidate struct {
		subscription notification.Subscription
		distanceKM   *float64
	}
	var subscriptions []candidate
	for rows.Next() {
		var s notification.Subscription
		var webhookURL *string
		var encryptedSecret []byte
		var distanceKM *float64
		var maximumEventAgeSeconds int64
		if err := rows.Scan(&s.ID, &s.Name, &s.Status, &s.Channel, &webhookURL, &encryptedSecret, &s.MinimumMagnitude,
			&s.MaximumMagnitude, &s.MinimumIntensity, &s.NotificationLanguage, &s.SubscriptionKind,
			&s.CenterLatitude, &s.CenterLongitude, &s.RadiusKM, &s.TsunamiOnly, &s.AllowedAlertLevels,
			&s.AllowedEventTypes, &s.NotifyOnNew, &s.NotifyOnThresholdCrossing, &s.NotifyOnTsunamiChange, &s.NotifyOnAlertIncrease,
			&maximumEventAgeSeconds, &s.TelegramChatID, &distanceKM, &s.CreatedAt, &s.UpdatedAt); err != nil {
			return err
		}
		if webhookURL != nil {
			s.WebhookURL = *webhookURL
		}
		s.EncryptedWebhookSecret = encryptedSecret
		s.MaximumEventAge = time.Duration(maximumEventAgeSeconds) * time.Second
		subscriptions = append(subscriptions, candidate{subscription: s, distanceKM: distanceKM})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	sourceLinks, err := providerSourceLinks(ctx, tx, change.Current.ID)
	if err != nil {
		return err
	}
	for _, item := range subscriptions {
		s := item.subscription
		current := change.Current
		current.DistanceKM = item.distanceKM
		var estimate *shaking.Estimate
		var triggers []notification.Trigger
		if s.SubscriptionKind == "user" && s.Channel == "telegram" && s.MinimumIntensity != nil && item.distanceKM != nil {
			counters.intensityCandidates++
			calculated, estimateErr := shaking.EstimateAt(current.Magnitude, current.DepthKM, *item.distanceKM, current.MagnitudeType)
			if estimateErr != nil {
				counters.estimateErrors++
				continue
			}
			counters.intensityEvaluations++
			estimate = &calculated
			var oldUpper *float64
			if change.Previous != nil && s.CenterLatitude != nil && s.CenterLongitude != nil {
				oldDistance := shaking.SurfaceDistanceKM(*s.CenterLatitude, *s.CenterLongitude,
					change.Previous.Latitude, change.Previous.Longitude)
				if oldEstimate, oldErr := shaking.EstimateAt(change.Previous.Magnitude, change.Previous.DepthKM,
					oldDistance, change.Previous.MagnitudeType); oldErr == nil {
					oldUpper = &oldEstimate.UpperMMI
				}
			}
			triggers = notification.IntensityTriggers(s, change.Previous, current, oldUpper,
				calculated.UpperMMI, mode, now, baseline)
			decision := "below_threshold"
			if len(triggers) > 0 {
				decision = "notify"
				counters.notify++
			} else {
				counters.belowThreshold++
			}
			if err := insertIntensityEvaluation(ctx, tx, s.ID, current.ID, current.Version,
				*s.MinimumIntensity, calculated, decision, now); err != nil {
				return err
			}
		} else {
			triggers = notification.Triggers(s, change.Previous, change.Current, mode, now, baseline)
		}
		counters.triggers += len(triggers)
		for _, trigger := range triggers {
			deliveryID := uuid.New()
			payload := notificationPayload(deliveryID, string(trigger), current, sourceLinks,
				s.NotificationLanguage, estimate, now)
			if s.Channel == "telegram" {
				if _, err := upsertTelegramAlertMessage(ctx, tx, s.ID, current.ID, current.Version,
					string(current.Lifecycle), payload, now); err != nil {
					return err
				}
				continue
			}
			_, err := tx.Exec(ctx, `INSERT INTO notification_deliveries(id,subscription_id,earthquake_id,earthquake_version,
				trigger_type,status,attempt_count,next_attempt_at,payload,created_at,updated_at)
				VALUES($1,$2,$3,$4,$5,'pending',0,$6,$7,$6,$6) ON CONFLICT DO NOTHING`,
				deliveryID, s.ID, change.Current.ID, change.Current.Version, trigger, now, payload)
			if err != nil {
				return err
			}
		}
	}
	_, err = tx.Exec(ctx, `INSERT INTO notification_matching_audits(
		id,earthquake_id,earthquake_version,mode,baseline_complete,model_version,decision_policy_version,
		candidate_minimum_mmi,candidate_radius_km,
		selected_subscription_count,intensity_candidate_count,intensity_evaluation_count,notify_decision_count,
		below_threshold_count,estimate_error_count,trigger_count,created_at)
		VALUES(gen_random_uuid(),$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`,
		change.Current.ID, change.Current.Version, mode, baseline, shaking.ModelVersion,
		notification.IntensityDecisionPolicyVersion, candidateMinimumMMI, candidateRadiusKM,
		len(subscriptions), counters.intensityCandidates, counters.intensityEvaluations, counters.notify,
		counters.belowThreshold, counters.estimateErrors, counters.triggers, now)
	return err
}

func refreshTelegramAlertMessages(ctx context.Context, tx pgx.Tx, current earthquake.Event, now time.Time) error {
	rows, err := tx.Query(ctx, `SELECT a.subscription_id,s.minimum_intensity,s.notification_language,
		CASE WHEN s.area IS NULL THEN NULL ELSE ST_Distance(s.area,ST_SetSRID(ST_MakePoint($1,$2),4326)::geography)/1000 END
		FROM telegram_alert_messages a
		JOIN notification_subscriptions s ON s.id=a.subscription_id
		WHERE a.earthquake_id=$3 AND s.status<>'disabled'`, current.Longitude, current.Latitude, current.ID)
	if err != nil {
		return err
	}
	type existingAlert struct {
		subscriptionID   uuid.UUID
		minimumIntensity *float64
		language         *string
		distanceKM       *float64
	}
	var alerts []existingAlert
	for rows.Next() {
		var alert existingAlert
		if err := rows.Scan(&alert.subscriptionID, &alert.minimumIntensity, &alert.language, &alert.distanceKM); err != nil {
			rows.Close()
			return err
		}
		alerts = append(alerts, alert)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	sourceLinks, err := providerSourceLinks(ctx, tx, current.ID)
	if err != nil {
		return err
	}
	for _, alert := range alerts {
		event := current
		event.DistanceKM = alert.distanceKM
		var estimate *shaking.Estimate
		if alert.minimumIntensity != nil && alert.distanceKM != nil {
			calculated, estimateErr := shaking.EstimateAt(event.Magnitude, event.DepthKM, *alert.distanceKM, event.MagnitudeType)
			if estimateErr == nil {
				estimate = &calculated
				if err := insertIntensityEvaluation(ctx, tx, alert.subscriptionID, current.ID, current.Version,
					*alert.minimumIntensity, calculated, "refresh", now); err != nil {
					return err
				}
			}
		}
		payload := notificationPayload(uuid.New(), "earthquake_update", event, sourceLinks, alert.language, estimate, now)
		if _, err := upsertTelegramAlertMessage(ctx, tx, alert.subscriptionID, current.ID, current.Version,
			string(current.Lifecycle), payload, now); err != nil {
			return err
		}
	}
	return nil
}

func providerSourceLinks(ctx context.Context, tx pgx.Tx, earthquakeID uuid.UUID) (map[string]string, error) {
	rows, err := tx.Query(ctx, `SELECT DISTINCT ON (source.provider) source.provider,
		COALESCE(source.source_url,source.detail_url)
		FROM earthquake_source_associations association
		JOIN earthquake_source_records source ON source.id=association.source_record_id
		WHERE association.earthquake_id=$1 AND association.active
			AND COALESCE(source.source_url,source.detail_url) IS NOT NULL
		ORDER BY source.provider,source.source_updated_at DESC`, earthquakeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	links := map[string]string{}
	for rows.Next() {
		var provider, link string
		if err := rows.Scan(&provider, &link); err != nil {
			return nil, err
		}
		links[provider] = link
	}
	return links, rows.Err()
}

func insertIntensityEvaluation(ctx context.Context, tx pgx.Tx, subscriptionID, earthquakeID uuid.UUID,
	earthquakeVersion int64, threshold float64, estimate shaking.Estimate, decision string, now time.Time) error {
	assumptions, err := json.Marshal(estimate.Assumptions)
	if err != nil {
		return err
	}
	decisionBoundary := notification.IntensityDecisionBoundary(threshold)
	_, err = tx.Exec(ctx, `INSERT INTO notification_intensity_evaluations(
		id,subscription_id,earthquake_id,earthquake_version,model_name,model_version,mean_mmi,sigma_mmi,
		lower_mmi,upper_mmi,threshold_mmi,decision_boundary_mmi,decision_policy_version,
		epicentral_distance_km,hypocentral_distance_km,magnitude,
		depth_km,decision,assumptions,created_at)
		VALUES(gen_random_uuid(),$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
		ON CONFLICT(subscription_id,earthquake_id,earthquake_version,model_version) DO NOTHING`,
		subscriptionID, earthquakeID, earthquakeVersion, estimate.ModelName, estimate.ModelVersion,
		estimate.MeanMMI, estimate.SigmaMMI, estimate.LowerMMI, estimate.UpperMMI, threshold, decisionBoundary,
		notification.IntensityDecisionPolicyVersion,
		estimate.EpicentralDistanceKM, estimate.HypocentralDistanceKM, estimate.Magnitude, estimate.DepthKM,
		decision, assumptions, now)
	return err
}

func notificationPayload(deliveryID uuid.UUID, trigger string, event earthquake.Event, sourceLinks map[string]string,
	language *string, estimate *shaking.Estimate, now time.Time) json.RawMessage {
	lifecycle := event.Lifecycle
	if lifecycle == "" {
		lifecycle = earthquake.Confirmed
	}
	payload, _ := json.Marshal(map[string]any{
		"delivery_id": deliveryID,
		"type":        trigger,
		"lifecycle":   lifecycle,
		"created_at":  now,
		"earthquake":  event,
		"sources":     sourceLinks,
		"language":    language,
		"shaking":     estimate,
	})
	return payload
}
