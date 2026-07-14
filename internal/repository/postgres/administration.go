package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/example/earthquake-service/internal/administration"
	"github.com/example/earthquake-service/internal/domain/earthquake"
	"github.com/example/earthquake-service/internal/domain/notification"
)

func (r *Repository) BootstrapOwners(ctx context.Context, emails []string, now time.Time) error {
	if len(emails) == 0 {
		return nil
	}
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, email := range emails {
		_, err = tx.Exec(ctx, `INSERT INTO admin_role_bindings(id,email,role,created_at,updated_at,created_by)
			VALUES(gen_random_uuid(),$1,'owner',$2,$2,'bootstrap') ON CONFLICT(email) DO NOTHING`, email, now)
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (r *Repository) RoleForEmail(ctx context.Context, email string) (administration.Role, error) {
	var role administration.Role
	err := r.Pool.QueryRow(ctx, `SELECT role FROM admin_role_bindings WHERE email=$1 AND active`, email).Scan(&role)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", administration.ErrNotFound
	}
	return role, err
}

func (r *Repository) ListAdminIncidents(ctx context.Context, filter administration.IncidentFilter) ([]earthquake.Event, error) {
	args := make([]any, 0, 6)
	where := []string{"TRUE"}
	add := func(format string, value any) {
		args = append(args, value)
		where = append(where, fmt.Sprintf(format, len(args)))
	}
	if filter.Provider != "" {
		add("preferred_source=$%d", filter.Provider)
	}
	if filter.Lifecycle != "" {
		add("lifecycle=$%d", filter.Lifecycle)
	}
	if filter.Status != "" {
		add("status=$%d", filter.Status)
	}
	if filter.MinMagnitude != nil {
		add("magnitude >= $%d", *filter.MinMagnitude)
	}
	if filter.BeforeAt != nil && filter.BeforeID != nil {
		args = append(args, *filter.BeforeAt, *filter.BeforeID)
		where = append(where, fmt.Sprintf("(occurred_at,id) < ($%d,$%d)", len(args)-1, len(args)))
	}
	args = append(args, filter.Limit)
	query := adminIncidentSelect + ` WHERE ` + strings.Join(where, " AND ") +
		fmt.Sprintf(` ORDER BY occurred_at DESC,id DESC LIMIT $%d`, len(args))
	rows, err := r.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]earthquake.Event, 0, filter.Limit)
	for rows.Next() {
		var event earthquake.Event
		var provenance []byte
		if err := rows.Scan(adminIncidentScan(&event, &provenance)...); err != nil {
			return nil, err
		}
		items = append(items, event)
	}
	return items, rows.Err()
}

func (r *Repository) AdminIncident(ctx context.Context, id uuid.UUID) (administration.IncidentDetail, error) {
	var detail administration.IncidentDetail
	err := r.Pool.QueryRow(ctx, adminIncidentSelect+` WHERE id=$1`, id).
		Scan(adminIncidentScan(&detail.Incident, &detail.Provenance)...)
	if errors.Is(err, pgx.ErrNoRows) {
		return detail, administration.ErrNotFound
	}
	if err != nil {
		return detail, err
	}
	if detail.Sources, err = r.adminSources(ctx, id); err != nil {
		return detail, err
	}
	if detail.Observations, err = r.adminObservations(ctx, id); err != nil {
		return detail, err
	}
	if detail.Associations, err = r.adminAssociations(ctx, id); err != nil {
		return detail, err
	}
	if detail.Evaluations, err = r.adminEvaluations(ctx, id); err != nil {
		return detail, err
	}
	detail.Revisions, err = r.adminRevisions(ctx, id)
	return detail, err
}

const adminIncidentSelect = `SELECT id,preferred_source,preferred_external_id,occurred_at,source_updated_at,latitude,
	longitude,depth_km,magnitude,magnitude_type,place,title,status,event_type,alert_level,tsunami,significance,
	felt_reports,cdi,mmi,station_count,azimuthal_gap,minimum_distance,rms,source_url,detail_url,version,first_seen_at,
	last_seen_at,created_at,updated_at,lifecycle,canonical_provenance FROM earthquakes`

func adminIncidentScan(event *earthquake.Event, provenance *[]byte) []any {
	return []any{&event.ID, &event.Provider, &event.ExternalID, &event.OccurredAt, &event.SourceUpdatedAt,
		&event.Latitude, &event.Longitude, &event.DepthKM, &event.Magnitude, &event.MagnitudeType, &event.Place,
		&event.Title, &event.Status, &event.EventType, &event.AlertLevel, &event.Tsunami, &event.Significance,
		&event.FeltReports, &event.CDI, &event.MMI, &event.StationCount, &event.AzimuthalGap, &event.MinimumDistance,
		&event.RMS, &event.SourceURL, &event.DetailURL, &event.Version, &event.FirstSeenAt, &event.LastSeenAt,
		&event.CreatedAt, &event.UpdatedAt, &event.Lifecycle, provenance}
}

func (r *Repository) adminSources(ctx context.Context, id uuid.UUID) ([]administration.SourceRecord, error) {
	rows, err := r.Pool.Query(ctx, `SELECT id,provider,external_id,latest_observation_channel,solution_class,
		source_url,detail_url,version,source_updated_at,first_seen_at,last_seen_at FROM earthquake_source_records
		WHERE earthquake_id=$1 ORDER BY provider,external_id LIMIT 50`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []administration.SourceRecord
	for rows.Next() {
		var item administration.SourceRecord
		if err := rows.Scan(&item.ID, &item.Provider, &item.ExternalID, &item.LatestObservationChannel,
			&item.SolutionClass, &item.SourceURL, &item.DetailURL, &item.Version, &item.SourceUpdatedAt,
			&item.FirstSeenAt, &item.LastSeenAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r *Repository) adminObservations(ctx context.Context, id uuid.UUID) ([]administration.Observation, error) {
	rows, err := r.Pool.Query(ctx, `SELECT o.id,o.source_record_id,o.source_version,o.channel,o.solution_class,
		o.source_updated_at,o.received_at FROM provider_observations o JOIN earthquake_source_records s
		ON s.id=o.source_record_id WHERE s.earthquake_id=$1 ORDER BY o.received_at DESC LIMIT 200`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []administration.Observation
	for rows.Next() {
		var item administration.Observation
		if err := rows.Scan(&item.ID, &item.SourceRecordID, &item.SourceVersion, &item.Channel,
			&item.SolutionClass, &item.SourceUpdatedAt, &item.ReceivedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r *Repository) adminAssociations(ctx context.Context, id uuid.UUID) ([]administration.Association, error) {
	rows, err := r.Pool.Query(ctx, `SELECT id,source_record_id,method,confidence,algorithm_version,evidence,
		active,associated_at,ended_at FROM earthquake_source_associations WHERE earthquake_id=$1
		ORDER BY associated_at DESC LIMIT 100`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []administration.Association
	for rows.Next() {
		var item administration.Association
		if err := rows.Scan(&item.ID, &item.SourceRecordID, &item.Method, &item.Confidence,
			&item.AlgorithmVersion, &item.Evidence, &item.Active, &item.AssociatedAt, &item.EndedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r *Repository) adminEvaluations(ctx context.Context, id uuid.UUID) ([]administration.IntensityEvaluation, error) {
	rows, err := r.Pool.Query(ctx, `SELECT id,subscription_id,earthquake_version,model_name,model_version,decision,
		mean_mmi,sigma_mmi,lower_mmi,upper_mmi,threshold_mmi,epicentral_distance_km,
		hypocentral_distance_km,magnitude,depth_km,created_at FROM notification_intensity_evaluations
		WHERE earthquake_id=$1 ORDER BY created_at DESC LIMIT 200`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []administration.IntensityEvaluation
	for rows.Next() {
		var item administration.IntensityEvaluation
		if err := rows.Scan(&item.ID, &item.SubscriptionID, &item.EarthquakeVersion, &item.ModelName,
			&item.ModelVersion, &item.Decision, &item.MeanMMI, &item.SigmaMMI, &item.LowerMMI,
			&item.UpperMMI, &item.ThresholdMMI, &item.EpicentralDistanceKM, &item.HypocentralDistanceKM,
			&item.Magnitude, &item.DepthKM, &item.CreatedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r *Repository) adminRevisions(ctx context.Context, id uuid.UUID) ([]administration.Revision, error) {
	rows, err := r.Pool.Query(ctx, `SELECT id,version,source_updated_at,changed_fields,created_at
		FROM earthquake_revisions WHERE earthquake_id=$1 ORDER BY version DESC LIMIT 100`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []administration.Revision
	for rows.Next() {
		var item administration.Revision
		if err := rows.Scan(&item.ID, &item.Version, &item.SourceUpdatedAt, &item.ChangedFields,
			&item.CreatedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r *Repository) ListAdminSubscriptions(ctx context.Context, filter administration.PageFilter) ([]notification.Subscription, error) {
	query, args := paginatedQuery(adminSubscriptionSelect, filter)
	rows, err := r.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []notification.Subscription
	for rows.Next() {
		item, err := scanSubscription(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r *Repository) AdminSubscription(ctx context.Context, id uuid.UUID) (notification.Subscription, error) {
	item, err := scanSubscription(r.Pool.QueryRow(ctx, adminSubscriptionSelect+` WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return item, administration.ErrNotFound
	}
	return item, err
}

const adminSubscriptionSelect = `SELECT id,name,status,channel,webhook_url,NULL::bytea AS encrypted_webhook_secret,
	minimum_magnitude,maximum_magnitude,center_latitude,center_longitude,radius_km,tsunami_only,
	allowed_alert_levels,allowed_event_types,notify_on_new,notify_on_threshold_crossing,notify_on_tsunami_change,
	notify_on_alert_increase,EXTRACT(EPOCH FROM maximum_event_age)::bigint,telegram_chat_id,created_at,updated_at,
	subscription_kind,telegram_chat_username,notification_language,minimum_intensity FROM notification_subscriptions`

const adminNotificationSelect = `SELECT id,subscription_id,earthquake_id,kind,delivery_class,status,earthquake_version,
	delivered_version,trigger_type,last_error,payload,attempt_count,next_attempt_at,sent_at,created_at,updated_at FROM (
	SELECT d.id,d.subscription_id,d.earthquake_id,'webhook_delivery'::text AS kind,'webhook'::text AS delivery_class,
		d.status,d.earthquake_version,NULL::bigint AS delivered_version,d.trigger_type,d.last_error,
		convert_to(left(d.payload::text,8192),'UTF8') AS payload,d.attempt_count,d.next_attempt_at,d.sent_at,
		d.created_at,d.updated_at
	FROM notification_deliveries d
	JOIN notification_subscriptions s ON s.id=d.subscription_id
	UNION ALL
	SELECT a.id,a.subscription_id,a.earthquake_id,'telegram_alert'::text AS kind,
		CASE WHEN s.subscription_kind='global_channel' THEN 'telegram_channel' ELSE 'telegram_private' END AS delivery_class,
		a.status,a.desired_earthquake_version,a.delivered_earthquake_version,NULL::text AS trigger_type,a.last_error,
		convert_to(left(a.desired_payload::text,8192),'UTF8') AS payload,a.attempt_count,a.next_attempt_at,
		NULL::timestamptz AS sent_at,a.created_at,a.updated_at
	FROM telegram_alert_messages a
	JOIN notification_subscriptions s ON s.id=a.subscription_id) notifications`

func (r *Repository) ListAdminNotifications(ctx context.Context, filter administration.NotificationFilter) ([]administration.NotificationItem, error) {
	query, args := adminNotificationQuery(filter)
	rows, err := r.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []administration.NotificationItem
	for rows.Next() {
		var item administration.NotificationItem
		if err := rows.Scan(adminNotificationScan(&item)...); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func adminNotificationQuery(filter administration.NotificationFilter) (string, []any) {
	where := make([]string, 0, 2)
	args := make([]any, 0, 4)
	if filter.DeliveryClass != "" {
		args = append(args, filter.DeliveryClass)
		where = append(where, fmt.Sprintf("delivery_class=$%d", len(args)))
	}
	if filter.BeforeAt != nil && filter.BeforeID != nil {
		args = append(args, *filter.BeforeAt, *filter.BeforeID)
		where = append(where, fmt.Sprintf("(created_at,id)<($%d,$%d)", len(args)-1, len(args)))
	}
	query := adminNotificationSelect
	if len(where) > 0 {
		query += ` WHERE ` + strings.Join(where, " AND ")
	}
	args = append(args, filter.Limit)
	query += fmt.Sprintf(" ORDER BY created_at DESC,id DESC LIMIT $%d", len(args))
	return query, args
}

func (r *Repository) AdminNotification(ctx context.Context, id uuid.UUID) (administration.NotificationItem, error) {
	var item administration.NotificationItem
	err := r.Pool.QueryRow(ctx, adminNotificationSelect+` WHERE id=$1`, id).Scan(adminNotificationScan(&item)...)
	if errors.Is(err, pgx.ErrNoRows) {
		return item, administration.ErrNotFound
	}
	return item, err
}

func adminNotificationScan(item *administration.NotificationItem) []any {
	return []any{&item.ID, &item.SubscriptionID, &item.EarthquakeID, &item.Kind, &item.DeliveryClass, &item.Status,
		&item.EarthquakeVersion, &item.DeliveredVersion, &item.TriggerType, &item.LastError, &item.Payload, &item.AttemptCount,
		&item.NextAttemptAt, &item.SentAt, &item.CreatedAt, &item.UpdatedAt}
}

func (r *Repository) ListAdminAudit(ctx context.Context, filter administration.PageFilter) ([]administration.AuditEntry, error) {
	query, args := paginatedQuery(`SELECT id,actor_subject,actor_email,actor_role,action,resource_type,
		resource_id,reason,request_id,source_ip,user_agent,created_at FROM admin_audit_log
		`, filter)
	rows, err := r.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []administration.AuditEntry
	for rows.Next() {
		var item administration.AuditEntry
		if err := rows.Scan(&item.ID, &item.ActorSubject, &item.ActorEmail, &item.Role, &item.Action,
			&item.ResourceType, &item.ResourceID, &item.Reason, &item.RequestID, &item.SourceIP,
			&item.UserAgent, &item.CreatedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func paginatedQuery(base string, filter administration.PageFilter) (string, []any) {
	if filter.BeforeAt != nil && filter.BeforeID != nil {
		return base + ` WHERE (created_at,id)<($1,$2) ORDER BY created_at DESC,id DESC LIMIT $3`,
			[]any{*filter.BeforeAt, *filter.BeforeID, filter.Limit}
	}
	return base + ` ORDER BY created_at DESC,id DESC LIMIT $1`, []any{filter.Limit}
}
