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
	err := tx.QueryRow(ctx, `SELECT e.id,e.occurred_at,e.source_updated_at,e.latitude,e.longitude,e.depth_km,e.magnitude,
		e.magnitude_type,e.place,e.title,e.status,e.event_type,e.alert_level,e.tsunami,e.significance,e.felt_reports,
		e.cdi,e.mmi,e.station_count,e.azimuthal_gap,e.minimum_distance,e.rms,e.source_url,e.detail_url,e.version,
		e.first_seen_at,e.last_seen_at,e.created_at,e.updated_at,s.id,s.payload_hash
		FROM earthquake_source_records s JOIN earthquakes e ON e.id=s.earthquake_id
		WHERE s.provider=$1 AND s.external_id=$2 FOR UPDATE`,
		incoming.Provider, incoming.ExternalID).Scan(scanEvent(&existing, &sourceID, &oldHash)...)
	hash, hashErr := incoming.CanonicalPayloadHash()
	if hashErr != nil {
		return earthquake.Change{}, fmt.Errorf("canonical payload hash: %w", hashErr)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		err = tx.QueryRow(ctx, `INSERT INTO earthquakes(id,preferred_source,preferred_external_id,occurred_at,source_updated_at,
			latitude,longitude,depth_km,location,magnitude,magnitude_type,place,title,status,event_type,alert_level,tsunami,
			significance,felt_reports,cdi,mmi,station_count,azimuthal_gap,minimum_distance,rms,source_url,detail_url,
			version,first_seen_at,last_seen_at,created_at,updated_at)
			VALUES(gen_random_uuid(),$1,$2,$3,$4,$5,$6,$7,ST_SetSRID(ST_MakePoint($6,$5),4326)::geography,$8,$9,$10,$11,$12,$13,$14,$15,
			$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,1,$26,$26,$26,$26)
			ON CONFLICT(preferred_source,preferred_external_id) DO NOTHING RETURNING id`,
			incoming.Provider, incoming.ExternalID, incoming.OccurredAt, incoming.SourceUpdatedAt, incoming.Latitude, incoming.Longitude,
			incoming.DepthKM, incoming.Magnitude, incoming.MagnitudeType, incoming.Place, incoming.Title, incoming.Status, incoming.EventType,
			incoming.AlertLevel, incoming.Tsunami, incoming.Significance, incoming.FeltReports, incoming.CDI, incoming.MMI, incoming.StationCount,
			incoming.AzimuthalGap, incoming.MinimumDistance, incoming.RMS, incoming.SourceURL, incoming.DetailURL, now).Scan(&incoming.ID)
		if errors.Is(err, pgx.ErrNoRows) {
			return applyEvent(ctx, tx, incoming, now)
		}
		if err != nil {
			return earthquake.Change{}, err
		}
		err = tx.QueryRow(ctx, `INSERT INTO earthquake_source_records(id,earthquake_id,provider,external_id,source_updated_at,
			payload_hash,raw_payload,version,first_seen_at,last_seen_at,created_at,updated_at)
			VALUES(gen_random_uuid(),$1,$2,$3,$4,$5,$6,1,$7,$7,$7,$7) RETURNING id`,
			incoming.ID, incoming.Provider, incoming.ExternalID, incoming.SourceUpdatedAt, hash[:], incoming.RawPayload, now).Scan(&sourceID)
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
		_, err = tx.Exec(ctx, `UPDATE earthquake_source_records SET last_seen_at=$2,updated_at=$2 WHERE id=$1`, sourceID, now)
		return earthquake.Change{Kind: earthquake.Stale, Previous: &existing, Current: existing, SourceRecordID: sourceID}, err
	}
	if incoming.SourceUpdatedAt.Equal(existing.SourceUpdatedAt) && bytesEqual(oldHash, hash[:]) {
		_, err = tx.Exec(ctx, `UPDATE earthquake_source_records SET last_seen_at=$2,updated_at=$2 WHERE id=$1`, sourceID, now)
		if err == nil {
			_, err = tx.Exec(ctx, `UPDATE earthquakes SET last_seen_at=$2,updated_at=$2 WHERE id=$1`, existing.ID, now)
		}
		return earthquake.Change{Kind: earthquake.Unchanged, Previous: &existing, Current: existing, SourceRecordID: sourceID}, err
	}
	incoming.ID = existing.ID
	incoming.Version = existing.Version + 1
	incoming.FirstSeenAt = existing.FirstSeenAt
	incoming.LastSeenAt = now
	incoming.CreatedAt = existing.CreatedAt
	incoming.UpdatedAt = now
	changed := changedFields(existing, incoming)
	changedJSON, _ := json.Marshal(changed)
	_, err = tx.Exec(ctx, `UPDATE earthquakes SET occurred_at=$2,source_updated_at=$3,latitude=$4,longitude=$5,depth_km=$6,
		location=ST_SetSRID(ST_MakePoint($5,$4),4326)::geography,magnitude=$7,magnitude_type=$8,place=$9,title=$10,
		status=$11,event_type=$12,alert_level=$13,tsunami=$14,significance=$15,felt_reports=$16,cdi=$17,mmi=$18,
		station_count=$19,azimuthal_gap=$20,minimum_distance=$21,rms=$22,source_url=$23,detail_url=$24,version=$25,
		last_seen_at=$26,updated_at=$26 WHERE id=$1`, incoming.ID, incoming.OccurredAt, incoming.SourceUpdatedAt, incoming.Latitude,
		incoming.Longitude, incoming.DepthKM, incoming.Magnitude, incoming.MagnitudeType, incoming.Place, incoming.Title, incoming.Status,
		incoming.EventType, incoming.AlertLevel, incoming.Tsunami, incoming.Significance, incoming.FeltReports, incoming.CDI, incoming.MMI,
		incoming.StationCount, incoming.AzimuthalGap, incoming.MinimumDistance, incoming.RMS, incoming.SourceURL, incoming.DetailURL,
		incoming.Version, now)
	if err != nil {
		return earthquake.Change{}, err
	}
	_, err = tx.Exec(ctx, `UPDATE earthquake_source_records SET source_updated_at=$2,payload_hash=$3,raw_payload=$4,
		version=version+1,last_seen_at=$5,updated_at=$5 WHERE id=$1`, sourceID, incoming.SourceUpdatedAt, hash[:], incoming.RawPayload, now)
	if err != nil {
		return earthquake.Change{}, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO earthquake_revisions(id,earthquake_id,source_record_id,version,source_updated_at,
		changed_fields,raw_payload,created_at) VALUES(gen_random_uuid(),$1,$2,$3,$4,$5,$6,$7)`,
		incoming.ID, sourceID, incoming.Version, incoming.SourceUpdatedAt, changedJSON, incoming.RawPayload, now)
	return earthquake.Change{Kind: earthquake.Updated, Previous: &existing, Current: incoming, ChangedFields: changed, SourceRecordID: sourceID}, err
}

func scanEvent(e *earthquake.Event, sourceID *uuid.UUID, hash *[]byte) []any {
	return []any{&e.ID, &e.OccurredAt, &e.SourceUpdatedAt, &e.Latitude, &e.Longitude, &e.DepthKM, &e.Magnitude, &e.MagnitudeType,
		&e.Place, &e.Title, &e.Status, &e.EventType, &e.AlertLevel, &e.Tsunami, &e.Significance, &e.FeltReports, &e.CDI, &e.MMI,
		&e.StationCount, &e.AzimuthalGap, &e.MinimumDistance, &e.RMS, &e.SourceURL, &e.DetailURL, &e.Version, &e.FirstSeenAt,
		&e.LastSeenAt, &e.CreatedAt, &e.UpdatedAt, sourceID, hash}
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
		EXTRACT(EPOCH FROM maximum_event_age)::bigint,
		created_at,updated_at FROM notification_subscriptions WHERE status='active' AND
		(area IS NULL OR ST_DWithin(area,ST_SetSRID(ST_MakePoint($1,$2),4326)::geography,radius_km*1000))`,
		change.Current.Longitude, change.Current.Latitude)
	if err != nil {
		return err
	}
	var subscriptions []notification.Subscription
	for rows.Next() {
		var s notification.Subscription
		var maximumEventAgeSeconds int64
		if err := rows.Scan(&s.ID, &s.Name, &s.Status, &s.Channel, &s.WebhookURL, &s.EncryptedWebhookSecret, &s.MinimumMagnitude,
			&s.MaximumMagnitude, &s.CenterLatitude, &s.CenterLongitude, &s.RadiusKM, &s.TsunamiOnly, &s.AllowedAlertLevels,
			&s.AllowedEventTypes, &s.NotifyOnNew, &s.NotifyOnThresholdCrossing, &s.NotifyOnTsunamiChange, &s.NotifyOnAlertIncrease,
			&maximumEventAgeSeconds, &s.CreatedAt, &s.UpdatedAt); err != nil {
			return err
		}
		s.MaximumEventAge = time.Duration(maximumEventAgeSeconds) * time.Second
		subscriptions = append(subscriptions, s)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	for _, s := range subscriptions {
		for _, trigger := range notification.Triggers(s, change.Previous, change.Current, mode, now, baseline) {
			deliveryID := uuid.New()
			payload, _ := json.Marshal(map[string]any{"delivery_id": deliveryID, "type": trigger, "created_at": now, "earthquake": change.Current})
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
