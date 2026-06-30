package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/example/earthquake-service/internal/domain/notification"
)

func (r *Repository) CreateSubscription(ctx context.Context, s notification.Subscription, now time.Time) (notification.Subscription, error) {
	err := r.Pool.QueryRow(ctx, `INSERT INTO notification_subscriptions(id,name,status,channel,webhook_url,encrypted_webhook_secret,
		minimum_magnitude,maximum_magnitude,center_latitude,center_longitude,radius_km,area,tsunami_only,allowed_alert_levels,
		allowed_event_types,notify_on_new,notify_on_threshold_crossing,notify_on_tsunami_change,notify_on_alert_increase,
		maximum_event_age,created_at,updated_at) VALUES(gen_random_uuid(),$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
		CASE WHEN $8::float8 IS NULL THEN NULL ELSE ST_SetSRID(ST_MakePoint($9,$8),4326)::geography END,$11,$12,$13,$14,$15,$16,$17,$18::text::interval,$19,$19)
		RETURNING id,created_at,updated_at`, s.Name, s.Status, s.Channel, s.WebhookURL, s.EncryptedWebhookSecret, s.MinimumMagnitude,
		s.MaximumMagnitude, s.CenterLatitude, s.CenterLongitude, s.RadiusKM, s.TsunamiOnly, s.AllowedAlertLevels, s.AllowedEventTypes,
		s.NotifyOnNew, s.NotifyOnThresholdCrossing, s.NotifyOnTsunamiChange, s.NotifyOnAlertIncrease, s.MaximumEventAge.String(), now).
		Scan(&s.ID, &s.CreatedAt, &s.UpdatedAt)
	return s, err
}

func (r *Repository) ListSubscriptions(ctx context.Context) ([]notification.Subscription, error) {
	rows, err := r.Pool.Query(ctx, subscriptionSelect+` ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []notification.Subscription
	for rows.Next() {
		s, err := scanSubscription(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
func (r *Repository) GetSubscription(ctx context.Context, id uuid.UUID) (notification.Subscription, error) {
	s, err := scanSubscription(r.Pool.QueryRow(ctx, subscriptionSelect+` WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return s, ErrNotFound
	}
	return s, err
}

const subscriptionSelect = `SELECT id,name,status,channel,webhook_url,encrypted_webhook_secret,minimum_magnitude,
	maximum_magnitude,center_latitude,center_longitude,radius_km,tsunami_only,allowed_alert_levels,allowed_event_types,
	notify_on_new,notify_on_threshold_crossing,notify_on_tsunami_change,notify_on_alert_increase,
	EXTRACT(EPOCH FROM maximum_event_age)::bigint,created_at,updated_at FROM notification_subscriptions`

type scanner interface{ Scan(...any) error }

func scanSubscription(row scanner) (notification.Subscription, error) {
	var s notification.Subscription
	var seconds int64
	err := row.Scan(&s.ID, &s.Name, &s.Status, &s.Channel, &s.WebhookURL, &s.EncryptedWebhookSecret,
		&s.MinimumMagnitude, &s.MaximumMagnitude, &s.CenterLatitude, &s.CenterLongitude, &s.RadiusKM, &s.TsunamiOnly, &s.AllowedAlertLevels,
		&s.AllowedEventTypes, &s.NotifyOnNew, &s.NotifyOnThresholdCrossing, &s.NotifyOnTsunamiChange, &s.NotifyOnAlertIncrease,
		&seconds, &s.CreatedAt, &s.UpdatedAt)
	s.MaximumEventAge = time.Duration(seconds) * time.Second
	return s, err
}
func (r *Repository) DisableSubscription(ctx context.Context, id uuid.UUID, now time.Time) error {
	tag, err := r.Pool.Exec(ctx, `UPDATE notification_subscriptions SET status='disabled',updated_at=$2 WHERE id=$1`, id, now)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

func (r *Repository) UpdateSubscription(ctx context.Context, s notification.Subscription, now time.Time) (notification.Subscription, error) {
	tag, err := r.Pool.Exec(ctx, `UPDATE notification_subscriptions SET name=$2,status=$3,webhook_url=$4,
		minimum_magnitude=$5,maximum_magnitude=$6,center_latitude=$7,center_longitude=$8,radius_km=$9,
		area=CASE WHEN $7::float8 IS NULL THEN NULL ELSE ST_SetSRID(ST_MakePoint($8,$7),4326)::geography END,
		tsunami_only=$10,allowed_alert_levels=$11,allowed_event_types=$12,notify_on_new=$13,
		notify_on_threshold_crossing=$14,notify_on_tsunami_change=$15,notify_on_alert_increase=$16,
		maximum_event_age=$17::text::interval,updated_at=$18 WHERE id=$1`,
		s.ID, s.Name, s.Status, s.WebhookURL, s.MinimumMagnitude, s.MaximumMagnitude, s.CenterLatitude,
		s.CenterLongitude, s.RadiusKM, s.TsunamiOnly, s.AllowedAlertLevels, s.AllowedEventTypes, s.NotifyOnNew,
		s.NotifyOnThresholdCrossing, s.NotifyOnTsunamiChange, s.NotifyOnAlertIncrease, s.MaximumEventAge.String(), now)
	if err != nil {
		return s, err
	}
	if tag.RowsAffected() == 0 {
		return s, ErrNotFound
	}
	return r.GetSubscription(ctx, s.ID)
}

type Delivery struct {
	ID, SubscriptionID, EarthquakeID uuid.UUID
	EarthquakeVersion                int64
	TriggerType, Status              string
	AttemptCount                     int
	NextAttemptAt                    time.Time
	WebhookURL                       string
	EncryptedSecret                  []byte
	Payload                          []byte
}

func (r *Repository) Claim(ctx context.Context, worker string, batch int, lockTimeout time.Duration, timeNow time.Time) ([]Delivery, error) {
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `WITH candidates AS (
		SELECT d.id FROM notification_deliveries d
		WHERE ((d.status IN ('pending','retry') AND d.next_attempt_at <= $1) OR
		       (d.status='processing' AND d.locked_at < $1-$2::text::interval))
		ORDER BY d.next_attempt_at FOR UPDATE SKIP LOCKED LIMIT $3)
		UPDATE notification_deliveries d SET status='processing',locked_at=$1,locked_by=$4,updated_at=$1
		FROM candidates c,notification_subscriptions s WHERE d.id=c.id AND s.id=d.subscription_id
		RETURNING d.id,d.subscription_id,d.earthquake_id,d.earthquake_version,d.trigger_type,d.status,d.attempt_count,
		d.next_attempt_at,s.webhook_url,s.encrypted_webhook_secret,d.payload`, timeNow, lockTimeout.String(), batch, worker)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Delivery
	for rows.Next() {
		var d Delivery
		if err := rows.Scan(&d.ID, &d.SubscriptionID, &d.EarthquakeID, &d.EarthquakeVersion, &d.TriggerType,
			&d.Status, &d.AttemptCount, &d.NextAttemptAt, &d.WebhookURL, &d.EncryptedSecret, &d.Payload); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}
func (r *Repository) CompleteDelivery(ctx context.Context, id uuid.UUID, status string, attempt int, next time.Time, response *int, lastError *string, now time.Time) error {
	_, err := r.Pool.Exec(ctx, `UPDATE notification_deliveries SET status=$2,attempt_count=$3,next_attempt_at=$4,
		sent_at=CASE WHEN $2='sent' THEN $7 ELSE sent_at END,response_status=$5,last_error=$6,locked_at=NULL,locked_by=NULL,
		updated_at=$7 WHERE id=$1`, id, status, attempt, next, response, lastError, now)
	return err
}
func (r *Repository) RetryDelivery(ctx context.Context, id uuid.UUID, now time.Time) error {
	tag, err := r.Pool.Exec(ctx, `UPDATE notification_deliveries SET status='retry',next_attempt_at=$2,locked_at=NULL,
		locked_by=NULL,last_error=NULL,updated_at=$2 WHERE id=$1 AND status IN ('dead','retry','pending')`, id, now)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}
