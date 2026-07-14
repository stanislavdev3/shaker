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
		if (change.Kind == earthquake.Inserted || change.Kind == earthquake.Updated) && mode == "realtime" {
			if err := createDeliveries(ctx, tx, change, mode, baselineComplete, now); err != nil {
				return stats, err
			}
		}
		if change.Kind == earthquake.Updated {
			if err := refreshTelegramAlertMessages(ctx, tx, change.Current, now); err != nil {
				return stats, err
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return stats, err
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
	err := tx.QueryRow(ctx, `SELECT e.id,e.occurred_at,e.source_updated_at,e.latitude,e.longitude,e.depth_km,e.magnitude,
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
			payload_hash,raw_payload,version,first_seen_at,last_seen_at,created_at,updated_at,
			latest_observation_channel,solution_class)
			VALUES(gen_random_uuid(),$1,$2,$3,$4,$5,$6,1,$7,$7,$7,$7,$8,$9) RETURNING id`,
			incoming.ID, incoming.Provider, incoming.ExternalID, incoming.SourceUpdatedAt, hash[:], incoming.RawPayload, now,
			channel, solution).Scan(&sourceID)
		if err != nil {
			return earthquake.Change{}, err
		}
		if err := insertProviderObservation(ctx, tx, sourceID, 1, channel, solution, incoming.SourceUpdatedAt, hash[:], incoming.RawPayload, now); err != nil {
			return earthquake.Change{}, err
		}
		_, err = tx.Exec(ctx, `INSERT INTO earthquake_source_associations(id,source_record_id,earthquake_id,method,
			confidence,algorithm_version,evidence,active,associated_at)
			VALUES(gen_random_uuid(),$1,$2,'new_incident',1,'identity-v1',$3,TRUE,$4)`,
			sourceID, incoming.ID, json.RawMessage(`{"reason":"first provider identity"}`), now)
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
	existing.Provider = incoming.Provider
	existing.ExternalID = incoming.ExternalID
	if incoming.SourceUpdatedAt.Before(existing.SourceUpdatedAt) {
		if err := insertProviderObservation(ctx, tx, sourceID, sourceVersion, channel, solution, incoming.SourceUpdatedAt, hash[:], incoming.RawPayload, now); err != nil {
			return earthquake.Change{}, err
		}
		promoted := earthquake.StrongerSolution(oldSolution, solution)
		_, err = tx.Exec(ctx, `UPDATE earthquake_source_records SET solution_class=$2,last_seen_at=$3,updated_at=$3 WHERE id=$1`, sourceID, promoted, now)
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
		_, err = tx.Exec(ctx, `UPDATE earthquake_source_records SET latest_observation_channel=$2,solution_class=$3,
			last_seen_at=$4,updated_at=$4 WHERE id=$1`, sourceID, channel, promoted, now)
		if err != nil {
			return earthquake.Change{}, err
		}
		return applyLifecycleChange(ctx, tx, existing, sourceID, incoming, now, earthquake.Unchanged)
	}
	sourceVersion++
	_, err = tx.Exec(ctx, `UPDATE earthquake_source_records SET source_updated_at=$2,payload_hash=$3,raw_payload=$4,
		version=$5,latest_observation_channel=$6,solution_class=$7,last_seen_at=$8,updated_at=$8 WHERE id=$1`,
		sourceID, incoming.SourceUpdatedAt, hash[:], incoming.RawPayload, sourceVersion, channel, solution, now)
	if err != nil {
		return earthquake.Change{}, err
	}
	if err := insertProviderObservation(ctx, tx, sourceID, sourceVersion, channel, solution, incoming.SourceUpdatedAt, hash[:], incoming.RawPayload, now); err != nil {
		return earthquake.Change{}, err
	}
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
	if existing.Lifecycle != incoming.Lifecycle {
		changed["lifecycle"] = map[string]any{"from": existing.Lifecycle, "to": incoming.Lifecycle}
	}
	changedJSON, _ := json.Marshal(changed)
	provenance := canonicalProvenance(incoming, sourceVersion)
	_, err = tx.Exec(ctx, `UPDATE earthquakes SET occurred_at=$2,source_updated_at=$3,latitude=$4,longitude=$5,depth_km=$6,
		location=ST_SetSRID(ST_MakePoint($5,$4),4326)::geography,magnitude=$7,magnitude_type=$8,place=$9,title=$10,
		status=$11,event_type=$12,alert_level=$13,tsunami=$14,significance=$15,felt_reports=$16,cdi=$17,mmi=$18,
		station_count=$19,azimuthal_gap=$20,minimum_distance=$21,rms=$22,source_url=$23,detail_url=$24,version=$25,
		last_seen_at=$26,updated_at=$26,lifecycle=$27,canonical_provenance=$28 WHERE id=$1`, incoming.ID, incoming.OccurredAt, incoming.SourceUpdatedAt, incoming.Latitude,
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

func scanEvent(e *earthquake.Event, sourceID *uuid.UUID, hash *[]byte, sourceVersion *int64, channel *string, solution *earthquake.SolutionClass) []any {
	return []any{&e.ID, &e.OccurredAt, &e.SourceUpdatedAt, &e.Latitude, &e.Longitude, &e.DepthKM, &e.Magnitude, &e.MagnitudeType,
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
	rows, err := tx.Query(ctx, `SELECT id,name,status,channel,webhook_url,encrypted_webhook_secret,minimum_magnitude,
		maximum_magnitude,center_latitude,center_longitude,radius_km,tsunami_only,allowed_alert_levels,allowed_event_types,
		notify_on_new,notify_on_threshold_crossing,notify_on_tsunami_change,notify_on_alert_increase,
		EXTRACT(EPOCH FROM maximum_event_age)::bigint,telegram_chat_id,
		CASE WHEN area IS NULL THEN NULL ELSE ST_Distance(area,ST_SetSRID(ST_MakePoint($1,$2),4326)::geography)/1000 END,
		created_at,updated_at FROM notification_subscriptions WHERE status='active' AND
		(area IS NULL OR ST_DWithin(area,ST_SetSRID(ST_MakePoint($1,$2),4326)::geography,radius_km*1000))`,
		change.Current.Longitude, change.Current.Latitude)
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
			&s.MaximumMagnitude, &s.CenterLatitude, &s.CenterLongitude, &s.RadiusKM, &s.TsunamiOnly, &s.AllowedAlertLevels,
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
	for _, item := range subscriptions {
		s := item.subscription
		current := change.Current
		current.DistanceKM = item.distanceKM
		for _, trigger := range notification.Triggers(s, change.Previous, change.Current, mode, now, baseline) {
			deliveryID := uuid.New()
			payload := notificationPayload(deliveryID, string(trigger), current, now)
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
	return nil
}

func refreshTelegramAlertMessages(ctx context.Context, tx pgx.Tx, current earthquake.Event, now time.Time) error {
	rows, err := tx.Query(ctx, `SELECT a.subscription_id,
		CASE WHEN s.area IS NULL THEN NULL ELSE ST_Distance(s.area,ST_SetSRID(ST_MakePoint($1,$2),4326)::geography)/1000 END
		FROM telegram_alert_messages a
		JOIN notification_subscriptions s ON s.id=a.subscription_id
		WHERE a.earthquake_id=$3 AND s.status<>'disabled'`, current.Longitude, current.Latitude, current.ID)
	if err != nil {
		return err
	}
	type existingAlert struct {
		subscriptionID uuid.UUID
		distanceKM     *float64
	}
	var alerts []existingAlert
	for rows.Next() {
		var alert existingAlert
		if err := rows.Scan(&alert.subscriptionID, &alert.distanceKM); err != nil {
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
	for _, alert := range alerts {
		event := current
		event.DistanceKM = alert.distanceKM
		payload := notificationPayload(uuid.New(), "earthquake_update", event, now)
		if _, err := upsertTelegramAlertMessage(ctx, tx, alert.subscriptionID, current.ID, current.Version,
			string(current.Lifecycle), payload, now); err != nil {
			return err
		}
	}
	return nil
}

func notificationPayload(deliveryID uuid.UUID, trigger string, event earthquake.Event, now time.Time) json.RawMessage {
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
	})
	return payload
}
