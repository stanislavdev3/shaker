package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/example/earthquake-service/internal/domain/earthquake"
	"github.com/example/earthquake-service/internal/eventstream"
)

type MessagePosition struct {
	Topic     string
	Partition int
	Offset    int64
}

type OutboxMessage struct {
	ID           uuid.UUID
	Topic        string
	Key          string
	Schema       string
	Payload      json.RawMessage
	Headers      json.RawMessage
	AttemptCount int
}

func (r *Repository) ApplyProviderMessage(ctx context.Context, position MessagePosition,
	message eventstream.ProviderObservationV1, receivedAt time.Time,
) (RunStats, bool, error) {
	if position.Topic != eventstream.ProviderObservationsTopic || position.Partition < 0 || position.Offset < 0 {
		return RunStats{}, false, errors.New("invalid provider message position")
	}
	if message.MessageID == uuid.Nil || message.ProducedAt.IsZero() || message.Mode == "" {
		return RunStats{}, false, errors.New("provider observation envelope is incomplete")
	}
	event, err := message.Event()
	if err != nil {
		return RunStats{}, false, err
	}
	tx, err := r.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return RunStats{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `INSERT INTO core_message_inbox(
		message_id,topic,partition,message_offset,schema_name,provider,external_id,received_at,processed_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$8) ON CONFLICT DO NOTHING`, message.MessageID, position.Topic,
		position.Partition, position.Offset, message.Schema, event.Provider, event.ExternalID, receivedAt)
	if err != nil {
		return RunStats{}, false, err
	}
	if tag.RowsAffected() == 0 {
		duplicate, duplicateErr := duplicateProviderMessage(ctx, tx, position, message.MessageID)
		if duplicateErr != nil {
			return RunStats{}, false, duplicateErr
		}
		if !duplicate {
			return RunStats{}, false, errors.New("kafka position conflicts with another provider message")
		}
		if err := tx.Commit(ctx); err != nil {
			return RunStats{}, false, err
		}
		return RunStats{}, false, nil
	}
	stats, err := applyBatch(ctx, tx, []earthquake.Event{event}, message.Mode, message.BaselineComplete, false, true, receivedAt)
	if err != nil {
		return stats, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return stats, false, err
	}
	return stats, true, nil
}

func (r *Repository) ApplyIncidentMessage(ctx context.Context, position MessagePosition,
	message eventstream.IncidentChangedV1, receivedAt time.Time,
) (bool, error) {
	if position.Topic != eventstream.IncidentChangesTopic || position.Partition < 0 || position.Offset < 0 {
		return false, errors.New("invalid incident message position")
	}
	if message.MessageID == uuid.Nil || message.ProducedAt.IsZero() {
		return false, errors.New("incident change envelope is incomplete")
	}
	change, err := message.Change()
	if err != nil {
		return false, err
	}
	tx, err := r.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `INSERT INTO notification_message_inbox(
		message_id,topic,partition,message_offset,schema_name,earthquake_id,earthquake_version,received_at,processed_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$8) ON CONFLICT DO NOTHING`, message.MessageID, position.Topic,
		position.Partition, position.Offset, message.Schema, change.Current.ID, change.Current.Version, receivedAt)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		duplicate, duplicateErr := duplicateNotificationMessage(ctx, tx, position, message.MessageID)
		if duplicateErr != nil {
			return false, duplicateErr
		}
		if !duplicate {
			return false, errors.New("kafka position conflicts with another incident message")
		}
		if err := tx.Commit(ctx); err != nil {
			return false, err
		}
		return false, nil
	}
	if message.Operation == earthquake.Updated {
		if err := refreshTelegramAlertMessages(ctx, tx, change.Current, receivedAt); err != nil {
			return false, err
		}
	}
	if message.NotificationsEligible {
		if err := createDeliveries(ctx, tx, change, message.IngestionMode, message.BaselineComplete, receivedAt); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func duplicateNotificationMessage(ctx context.Context, tx pgx.Tx, position MessagePosition,
	messageID uuid.UUID,
) (bool, error) {
	var storedID uuid.UUID
	err := tx.QueryRow(ctx, `SELECT message_id FROM notification_message_inbox
		WHERE topic=$1 AND partition=$2 AND message_offset=$3`, position.Topic, position.Partition, position.Offset).Scan(&storedID)
	if err == nil {
		return storedID == messageID, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return false, err
	}
	err = tx.QueryRow(ctx, `SELECT message_id FROM notification_message_inbox WHERE message_id=$1`, messageID).Scan(&storedID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func duplicateProviderMessage(ctx context.Context, tx pgx.Tx, position MessagePosition, messageID uuid.UUID) (bool, error) {
	var storedID uuid.UUID
	err := tx.QueryRow(ctx, `SELECT message_id FROM core_message_inbox
		WHERE topic=$1 AND partition=$2 AND message_offset=$3`, position.Topic, position.Partition, position.Offset).Scan(&storedID)
	if err == nil {
		return storedID == messageID, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return false, err
	}
	err = tx.QueryRow(ctx, `SELECT message_id FROM core_message_inbox WHERE message_id=$1`, messageID).Scan(&storedID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func enqueueIncidentChange(ctx context.Context, tx pgx.Tx, change earthquake.Change, mode string,
	baselineComplete bool, now time.Time,
) error {
	message := eventstream.NewIncidentChanged(change, mode, baselineComplete, now)
	payload, err := eventstream.Marshal(message)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO core_outbox_messages(
		id,topic,message_key,schema_name,payload,next_attempt_at,created_at,updated_at)
		VALUES($1,$2,$3,$4,$5,$6,$6,$6) ON CONFLICT(id) DO NOTHING`, message.MessageID,
		eventstream.IncidentChangesTopic, change.Current.ID.String(), message.Schema, payload, now)
	return err
}

func (r *Repository) ClaimCoreOutbox(ctx context.Context, workerID string, batch int, lockTimeout time.Duration,
	now time.Time,
) ([]OutboxMessage, error) {
	if workerID == "" || batch <= 0 {
		return nil, errors.New("invalid outbox claim")
	}
	rows, err := r.Pool.Query(ctx, `WITH candidates AS (
		SELECT id FROM core_outbox_messages
		WHERE published_at IS NULL AND next_attempt_at<=$1 AND (locked_at IS NULL OR locked_at<$2)
		ORDER BY next_attempt_at,created_at LIMIT $3 FOR UPDATE SKIP LOCKED
	)
	UPDATE core_outbox_messages m SET locked_at=$1,locked_by=$4,updated_at=$1
	FROM candidates c WHERE m.id=c.id
	RETURNING m.id,m.topic,m.message_key,m.schema_name,m.payload,m.headers,m.attempt_count`,
		now, now.Add(-lockTimeout), batch, workerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	messages := make([]OutboxMessage, 0, batch)
	for rows.Next() {
		var message OutboxMessage
		if err := rows.Scan(&message.ID, &message.Topic, &message.Key, &message.Schema, &message.Payload,
			&message.Headers, &message.AttemptCount); err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}
	return messages, rows.Err()
}

func (r *Repository) CompleteCoreOutbox(ctx context.Context, id uuid.UUID, workerID string, publishedAt time.Time) error {
	tag, err := r.Pool.Exec(ctx, `UPDATE core_outbox_messages SET published_at=$3,locked_at=NULL,locked_by=NULL,
		last_error=NULL,attempt_count=attempt_count+1,updated_at=$3 WHERE id=$1 AND locked_by=$2 AND published_at IS NULL`,
		id, workerID, publishedAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("complete core outbox %s: %w", id, ErrNotFound)
	}
	return nil
}

func (r *Repository) FailCoreOutbox(ctx context.Context, id uuid.UUID, workerID, message string,
	nextAttemptAt, now time.Time,
) error {
	if len(message) > 2000 {
		message = message[:2000]
	}
	tag, err := r.Pool.Exec(ctx, `UPDATE core_outbox_messages SET locked_at=NULL,locked_by=NULL,last_error=$3,
		attempt_count=attempt_count+1,next_attempt_at=$4,updated_at=$5
		WHERE id=$1 AND locked_by=$2 AND published_at IS NULL`, id, workerID, message, nextAttemptAt, now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("fail core outbox %s: %w", id, ErrNotFound)
	}
	return nil
}
