package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type TelegramAlertMessage struct {
	ID, SubscriptionID, EarthquakeID uuid.UUID
	TelegramChatID                   int64
	TelegramMessageID                *int64
	DesiredEarthquakeVersion         int64
	DeliveredEarthquakeVersion       *int64
	DesiredPayload                   json.RawMessage
	Lifecycle, Status                string
	AttemptCount                     int
	NextAttemptAt                    time.Time
}

func (r *Repository) UpsertTelegramAlertMessage(ctx context.Context, subscriptionID, earthquakeID uuid.UUID,
	earthquakeVersion int64, lifecycle string, payload json.RawMessage, now time.Time) (TelegramAlertMessage, error) {
	return upsertTelegramAlertMessage(ctx, r.Pool, subscriptionID, earthquakeID, earthquakeVersion, lifecycle, payload, now)
}

type telegramAlertQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func upsertTelegramAlertMessage(ctx context.Context, query telegramAlertQuerier, subscriptionID, earthquakeID uuid.UUID,
	earthquakeVersion int64, lifecycle string, payload json.RawMessage, now time.Time) (TelegramAlertMessage, error) {
	var alert TelegramAlertMessage
	err := query.QueryRow(ctx, `INSERT INTO telegram_alert_messages(
		id,subscription_id,earthquake_id,telegram_chat_id,desired_earthquake_version,desired_payload,lifecycle,
		status,attempt_count,next_attempt_at,created_at,updated_at)
		SELECT gen_random_uuid(),s.id,$2,s.telegram_chat_id,$3,$4,$5,'pending_send',0,$6,$6,$6
		FROM notification_subscriptions s
		WHERE s.id=$1 AND s.channel='telegram' AND s.telegram_chat_id IS NOT NULL
		ON CONFLICT(subscription_id,earthquake_id) DO UPDATE SET
			desired_earthquake_version=GREATEST(telegram_alert_messages.desired_earthquake_version,EXCLUDED.desired_earthquake_version),
			desired_payload=CASE WHEN EXCLUDED.desired_earthquake_version>telegram_alert_messages.desired_earthquake_version
				THEN EXCLUDED.desired_payload ELSE telegram_alert_messages.desired_payload END,
			lifecycle=CASE WHEN EXCLUDED.desired_earthquake_version>telegram_alert_messages.desired_earthquake_version
				THEN EXCLUDED.lifecycle ELSE telegram_alert_messages.lifecycle END,
			status=CASE WHEN EXCLUDED.desired_earthquake_version<=telegram_alert_messages.desired_earthquake_version
				THEN telegram_alert_messages.status
				WHEN telegram_alert_messages.telegram_message_id IS NULL THEN 'pending_send' ELSE 'pending_edit' END,
			next_attempt_at=CASE WHEN EXCLUDED.desired_earthquake_version>telegram_alert_messages.desired_earthquake_version
				THEN LEAST(telegram_alert_messages.next_attempt_at,EXCLUDED.next_attempt_at)
				ELSE telegram_alert_messages.next_attempt_at END,
			updated_at=EXCLUDED.updated_at
		RETURNING id,subscription_id,earthquake_id,telegram_chat_id,telegram_message_id,desired_earthquake_version,
		delivered_earthquake_version,desired_payload,lifecycle,status,attempt_count,next_attempt_at`,
		subscriptionID, earthquakeID, earthquakeVersion, payload, lifecycle, now).Scan(telegramAlertScan(&alert)...)
	if errors.Is(err, pgx.ErrNoRows) {
		return alert, ErrNotFound
	}
	return alert, err
}

func (r *Repository) ClaimTelegramAlertMessages(ctx context.Context, worker string, batch int,
	lockTimeout time.Duration, now time.Time) ([]TelegramAlertMessage, error) {
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `WITH candidates AS (
		SELECT a.id FROM telegram_alert_messages a
		JOIN notification_subscriptions s ON s.id=a.subscription_id
		WHERE ((a.status IN ('pending_send','pending_edit','retry') AND a.next_attempt_at <= $1)
			OR (a.status='processing' AND a.locked_at < $1-$2::text::interval))
		ORDER BY CASE WHEN s.subscription_kind='user' THEN 0 ELSE 1 END,a.next_attempt_at,a.id
		FOR UPDATE OF a SKIP LOCKED LIMIT $3)
		UPDATE telegram_alert_messages a SET status='processing',locked_at=$1,locked_by=$4,updated_at=$1
		FROM candidates c WHERE a.id=c.id
		RETURNING a.id,a.subscription_id,a.earthquake_id,a.telegram_chat_id,a.telegram_message_id,
		a.desired_earthquake_version,a.delivered_earthquake_version,a.desired_payload,a.lifecycle,a.status,
		a.attempt_count,a.next_attempt_at`, now, lockTimeout.String(), batch, worker)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var alerts []TelegramAlertMessage
	for rows.Next() {
		var alert TelegramAlertMessage
		if err := rows.Scan(telegramAlertScan(&alert)...); err != nil {
			return nil, err
		}
		alerts = append(alerts, alert)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return alerts, nil
}

func (r *Repository) CompleteTelegramAlertMessage(ctx context.Context, id uuid.UUID, deliveredVersion int64,
	telegramMessageID int64, attempt int, now time.Time) error {
	tag, err := r.Pool.Exec(ctx, `UPDATE telegram_alert_messages SET
		telegram_message_id=CASE WHEN telegram_message_id IS NULL THEN $3 ELSE telegram_message_id END,
		delivered_earthquake_version=GREATEST(COALESCE(delivered_earthquake_version,0),$2),
		status=CASE WHEN desired_earthquake_version>$2 THEN 'pending_edit' ELSE 'active' END,
		attempt_count=$4,next_attempt_at=$5,locked_at=NULL,locked_by=NULL,last_error=NULL,updated_at=$5
		WHERE id=$1`, id, deliveredVersion, telegramMessageID, attempt, now)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

func (r *Repository) FailTelegramAlertMessage(ctx context.Context, id uuid.UUID, attempt int, next time.Time,
	dead bool, lastError string, now time.Time) error {
	status := "retry"
	if dead {
		status = "dead"
	}
	if len(lastError) > 1000 {
		lastError = lastError[:1000]
	}
	tag, err := r.Pool.Exec(ctx, `UPDATE telegram_alert_messages SET status=$2,attempt_count=$3,next_attempt_at=$4,
		locked_at=NULL,locked_by=NULL,last_error=$5,updated_at=$6 WHERE id=$1`, id, status, attempt, next, lastError, now)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

func telegramAlertScan(alert *TelegramAlertMessage) []any {
	return []any{&alert.ID, &alert.SubscriptionID, &alert.EarthquakeID, &alert.TelegramChatID,
		&alert.TelegramMessageID, &alert.DesiredEarthquakeVersion, &alert.DeliveredEarthquakeVersion,
		&alert.DesiredPayload, &alert.Lifecycle, &alert.Status, &alert.AttemptCount, &alert.NextAttemptAt}
}
