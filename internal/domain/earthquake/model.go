package earthquake

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/google/uuid"
)

var ErrInvalidCoordinates = errors.New("invalid coordinates")

type Event struct {
	ID              uuid.UUID       `json:"id"`
	Provider        string          `json:"source"`
	ExternalID      string          `json:"external_id,omitempty"`
	OccurredAt      time.Time       `json:"occurred_at"`
	SourceUpdatedAt time.Time       `json:"source_updated_at"`
	Latitude        float64         `json:"latitude"`
	Longitude       float64         `json:"longitude"`
	DepthKM         *float64        `json:"depth_km"`
	Magnitude       *float64        `json:"magnitude"`
	MagnitudeType   *string         `json:"magnitude_type"`
	Place           *string         `json:"place"`
	Title           *string         `json:"title"`
	Status          *string         `json:"status"`
	EventType       *string         `json:"event_type"`
	AlertLevel      *string         `json:"alert_level"`
	Tsunami         *bool           `json:"tsunami"`
	Significance    *int            `json:"significance"`
	FeltReports     *int            `json:"felt_reports"`
	CDI             *float64        `json:"cdi"`
	MMI             *float64        `json:"mmi"`
	StationCount    *int            `json:"station_count"`
	AzimuthalGap    *float64        `json:"azimuthal_gap"`
	MinimumDistance *float64        `json:"minimum_distance"`
	RMS             *float64        `json:"rms"`
	SourceURL       *string         `json:"source_url"`
	DetailURL       *string         `json:"detail_url"`
	Version         int64           `json:"version"`
	FirstSeenAt     time.Time       `json:"first_seen_at"`
	LastSeenAt      time.Time       `json:"last_seen_at"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
	RawPayload      json.RawMessage `json:"-"`
	DistanceKM      *float64        `json:"distance_km,omitempty"`
}

func (e Event) Validate() error {
	if e.Provider == "" || e.ExternalID == "" || e.OccurredAt.IsZero() || e.SourceUpdatedAt.IsZero() {
		return errors.New("missing required event identity or timestamps")
	}
	if math.IsNaN(e.Latitude) || math.IsNaN(e.Longitude) || e.Latitude < -90 || e.Latitude > 90 || e.Longitude < -180 || e.Longitude > 180 {
		return ErrInvalidCoordinates
	}
	if (e.DepthKM != nil && math.IsNaN(*e.DepthKM)) || (e.Magnitude != nil && math.IsNaN(*e.Magnitude)) {
		return errors.New("NaN numeric value")
	}
	if !json.Valid(e.RawPayload) {
		return errors.New("invalid raw payload")
	}
	return nil
}

// PayloadHash hashes the raw JSON bytes. Provider adapters must populate RawPayload
// using a deterministic representation (USGS uses the exact feature bytes).
func (e Event) PayloadHash() [32]byte { return sha256.Sum256(e.RawPayload) }

// CanonicalPayloadHash normalizes JSON object key order and insignificant
// whitespace before hashing while preserving JSON numbers.
func (e Event) CanonicalPayloadHash() ([32]byte, error) {
	var zero [32]byte
	dec := json.NewDecoder(bytes.NewReader(e.RawPayload))
	dec.UseNumber()
	var value any
	if err := dec.Decode(&value); err != nil {
		return zero, err
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return zero, err
	}
	return sha256.Sum256(canonical), nil
}

type Change struct {
	Kind           ChangeKind
	Previous       *Event
	Current        Event
	ChangedFields  map[string]any
	SourceRecordID uuid.UUID
}

type ChangeKind string

const (
	Inserted  ChangeKind = "inserted"
	Updated   ChangeKind = "updated"
	Unchanged ChangeKind = "unchanged"
	Stale     ChangeKind = "stale"
)
