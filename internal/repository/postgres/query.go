package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/example/earthquake-service/internal/domain/earthquake"
)

type ListFilter struct {
	From, To                                       *time.Time
	MinMagnitude, MaxMagnitude, MinDepth, MaxDepth *float64
	Latitude, Longitude, RadiusKM                  *float64
	BBox                                           *[4]float64
	Tsunami                                        *bool
	AlertLevel, Status, EventType, Source          *string
	Limit                                          int
	Sort                                           string
	CursorOccurredAt                               *time.Time
	CursorMagnitude                                *float64
	CursorID                                       *uuid.UUID
}

func (r *Repository) List(ctx context.Context, f ListFilter) ([]earthquake.Event, error) {
	args := []any{}
	where := []string{"TRUE"}
	add := func(clause string, v any) {
		args = append(args, v)
		where = append(where, fmt.Sprintf(clause, len(args)))
	}
	if f.From != nil {
		add("occurred_at >= $%d", *f.From)
	}
	if f.To != nil {
		add("occurred_at <= $%d", *f.To)
	}
	if f.MinMagnitude != nil {
		add("magnitude >= $%d", *f.MinMagnitude)
	}
	if f.MaxMagnitude != nil {
		add("magnitude <= $%d", *f.MaxMagnitude)
	}
	if f.MinDepth != nil {
		add("depth_km >= $%d", *f.MinDepth)
	}
	if f.MaxDepth != nil {
		add("depth_km <= $%d", *f.MaxDepth)
	}
	if f.Tsunami != nil {
		add("tsunami = $%d", *f.Tsunami)
	}
	if f.AlertLevel != nil {
		add("alert_level = $%d", *f.AlertLevel)
	}
	if f.Status != nil {
		add("status = $%d", *f.Status)
	}
	if f.EventType != nil {
		add("event_type = $%d", *f.EventType)
	}
	if f.Source != nil {
		add("preferred_source = $%d", *f.Source)
	}
	distance := "NULL::float8"
	if f.RadiusKM != nil {
		args = append(args, *f.Longitude, *f.Latitude, *f.RadiusKM*1000)
		distance = fmt.Sprintf("ST_Distance(location,ST_SetSRID(ST_MakePoint($%d,$%d),4326)::geography)/1000", len(args)-2, len(args)-1)
		where = append(where, fmt.Sprintf("ST_DWithin(location,ST_SetSRID(ST_MakePoint($%d,$%d),4326)::geography,$%d)", len(args)-2, len(args)-1, len(args)))
	}
	if f.BBox != nil {
		args = append(args, f.BBox[0], f.BBox[1], f.BBox[2], f.BBox[3])
		n := len(args)
		where = append(where, fmt.Sprintf("location::geometry && ST_MakeEnvelope($%d,$%d,$%d,$%d,4326)", n-3, n-2, n-1, n))
	}
	order := "occurred_at DESC,id DESC"
	if f.Sort == "occurred_at_asc" {
		order = "occurred_at ASC,id ASC"
	}
	if f.Sort == "magnitude_desc" {
		order = "magnitude DESC NULLS LAST,id DESC"
	}
	if f.CursorID != nil {
		switch f.Sort {
		case "occurred_at_asc":
			args = append(args, *f.CursorOccurredAt, *f.CursorID)
			where = append(where, fmt.Sprintf("(occurred_at,id)>($%d,$%d)", len(args)-1, len(args)))
		case "magnitude_desc":
			args = append(args, *f.CursorMagnitude, *f.CursorID)
			where = append(where, fmt.Sprintf("(magnitude,id)<($%d,$%d)", len(args)-1, len(args)))
		default:
			args = append(args, *f.CursorOccurredAt, *f.CursorID)
			where = append(where, fmt.Sprintf("(occurred_at,id)<($%d,$%d)", len(args)-1, len(args)))
		}
	}
	args = append(args, f.Limit)
	query := `SELECT id,preferred_source,preferred_external_id,occurred_at,source_updated_at,latitude,longitude,depth_km,
		magnitude,magnitude_type,place,title,status,event_type,alert_level,tsunami,significance,felt_reports,cdi,mmi,
		station_count,azimuthal_gap,minimum_distance,rms,source_url,detail_url,version,first_seen_at,last_seen_at,
		created_at,updated_at,` + distance + ` FROM earthquakes WHERE ` + strings.Join(where, " AND ") + ` ORDER BY ` + order + fmt.Sprintf(" LIMIT $%d", len(args))
	rows, err := r.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []earthquake.Event
	for rows.Next() {
		var e earthquake.Event
		if err := rows.Scan(&e.ID, &e.Provider, &e.ExternalID, &e.OccurredAt, &e.SourceUpdatedAt,
			&e.Latitude, &e.Longitude, &e.DepthKM, &e.Magnitude, &e.MagnitudeType, &e.Place, &e.Title, &e.Status, &e.EventType,
			&e.AlertLevel, &e.Tsunami, &e.Significance, &e.FeltReports, &e.CDI, &e.MMI, &e.StationCount, &e.AzimuthalGap,
			&e.MinimumDistance, &e.RMS, &e.SourceURL, &e.DetailURL, &e.Version, &e.FirstSeenAt, &e.LastSeenAt, &e.CreatedAt,
			&e.UpdatedAt, &e.DistanceKM); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (r *Repository) Get(ctx context.Context, id uuid.UUID) (earthquake.Event, error) {
	var e earthquake.Event
	err := r.Pool.QueryRow(ctx, `SELECT id,preferred_source,preferred_external_id,occurred_at,source_updated_at,latitude,
		longitude,depth_km,magnitude,magnitude_type,place,title,status,event_type,alert_level,tsunami,significance,
		felt_reports,cdi,mmi,station_count,azimuthal_gap,minimum_distance,rms,source_url,detail_url,version,first_seen_at,
		last_seen_at,created_at,updated_at FROM earthquakes WHERE id=$1`, id).Scan(&e.ID, &e.Provider, &e.ExternalID, &e.OccurredAt,
		&e.SourceUpdatedAt, &e.Latitude, &e.Longitude, &e.DepthKM, &e.Magnitude, &e.MagnitudeType, &e.Place, &e.Title, &e.Status,
		&e.EventType, &e.AlertLevel, &e.Tsunami, &e.Significance, &e.FeltReports, &e.CDI, &e.MMI, &e.StationCount, &e.AzimuthalGap,
		&e.MinimumDistance, &e.RMS, &e.SourceURL, &e.DetailURL, &e.Version, &e.FirstSeenAt, &e.LastSeenAt, &e.CreatedAt, &e.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return e, ErrNotFound
	}
	return e, err
}

type Revision struct {
	ID              uuid.UUID `json:"id"`
	Version         int64     `json:"version"`
	SourceUpdatedAt time.Time `json:"source_updated_at"`
	ChangedFields   any       `json:"changed_fields"`
	Raw             any       `json:"raw_payload,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
}

func (r *Repository) Revisions(ctx context.Context, id uuid.UUID, raw bool) ([]Revision, error) {
	rows, err := r.Pool.Query(ctx, `SELECT id,version,source_updated_at,changed_fields,CASE WHEN $2 THEN raw_payload ELSE NULL END,created_at FROM earthquake_revisions WHERE earthquake_id=$1 ORDER BY version DESC`, id, raw)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Revision
	for rows.Next() {
		var x Revision
		if err := rows.Scan(&x.ID, &x.Version, &x.SourceUpdatedAt, &x.ChangedFields, &x.Raw, &x.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, rows.Err()
}
