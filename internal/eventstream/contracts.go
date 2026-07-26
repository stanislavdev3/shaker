package eventstream

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/example/earthquake-service/internal/domain/earthquake"
)

const (
	ProviderObservationsTopic = "provider.observations.v1"
	IncidentChangesTopic      = "incident.changes.v1"
	ProviderObservationSchema = "provider.observation.v1"
	IncidentChangedSchema     = "incident.changed.v1"
)

var ErrUnsupportedSchema = errors.New("unsupported event schema")

type ProviderObservationV1 struct {
	Schema           string                       `json:"schema"`
	MessageID        uuid.UUID                    `json:"message_id"`
	ProducedAt       time.Time                    `json:"produced_at"`
	Mode             string                       `json:"mode"`
	BaselineComplete bool                         `json:"baseline_complete"`
	Observation      ProviderObservationPayloadV1 `json:"observation"`
}

type ProviderObservationPayloadV1 struct {
	Provider           string                   `json:"provider"`
	ExternalID         string                   `json:"external_id"`
	ObservationChannel string                   `json:"observation_channel"`
	SolutionClass      earthquake.SolutionClass `json:"solution_class"`
	OccurredAt         time.Time                `json:"occurred_at"`
	SourceUpdatedAt    time.Time                `json:"source_updated_at"`
	Latitude           float64                  `json:"latitude"`
	Longitude          float64                  `json:"longitude"`
	DepthKM            *float64                 `json:"depth_km"`
	Magnitude          *float64                 `json:"magnitude"`
	MagnitudeType      *string                  `json:"magnitude_type"`
	Place              *string                  `json:"place"`
	Title              *string                  `json:"title"`
	Status             *string                  `json:"status"`
	EventType          *string                  `json:"event_type"`
	AlertLevel         *string                  `json:"alert_level"`
	Tsunami            *bool                    `json:"tsunami"`
	Significance       *int                     `json:"significance"`
	FeltReports        *int                     `json:"felt_reports"`
	CDI                *float64                 `json:"cdi"`
	MMI                *float64                 `json:"mmi"`
	StationCount       *int                     `json:"station_count"`
	AzimuthalGap       *float64                 `json:"azimuthal_gap"`
	MinimumDistance    *float64                 `json:"minimum_distance"`
	RMS                *float64                 `json:"rms"`
	SourceURL          *string                  `json:"source_url"`
	DetailURL          *string                  `json:"detail_url"`
	RawPayload         json.RawMessage          `json:"raw_payload"`
}

type IncidentChangedV1 struct {
	Schema                string                `json:"schema"`
	MessageID             uuid.UUID             `json:"message_id"`
	ProducedAt            time.Time             `json:"produced_at"`
	Operation             earthquake.ChangeKind `json:"operation"`
	IngestionMode         string                `json:"ingestion_mode"`
	BaselineComplete      bool                  `json:"baseline_complete"`
	NotificationsEligible bool                  `json:"notifications_eligible"`
	Previous              *CanonicalIncidentV1  `json:"previous,omitempty"`
	Incident              CanonicalIncidentV1   `json:"incident"`
	ChangedFields         map[string]any        `json:"changed_fields,omitempty"`
}

type CanonicalIncidentV1 struct {
	ID                uuid.UUID            `json:"id"`
	Version           int64                `json:"version"`
	Lifecycle         earthquake.Lifecycle `json:"lifecycle"`
	PreferredProvider string               `json:"preferred_provider"`
	PreferredID       string               `json:"preferred_external_id"`
	OccurredAt        time.Time            `json:"occurred_at"`
	SourceUpdatedAt   time.Time            `json:"source_updated_at"`
	Latitude          float64              `json:"latitude"`
	Longitude         float64              `json:"longitude"`
	DepthKM           *float64             `json:"depth_km"`
	Magnitude         *float64             `json:"magnitude"`
	MagnitudeType     *string              `json:"magnitude_type"`
	Place             *string              `json:"place"`
	Title             *string              `json:"title"`
	Status            *string              `json:"status"`
	EventType         *string              `json:"event_type"`
	AlertLevel        *string              `json:"alert_level"`
	Tsunami           *bool                `json:"tsunami"`
	Significance      *int                 `json:"significance"`
	FeltReports       *int                 `json:"felt_reports"`
	CDI               *float64             `json:"cdi"`
	MMI               *float64             `json:"mmi"`
	StationCount      *int                 `json:"station_count"`
	AzimuthalGap      *float64             `json:"azimuthal_gap"`
	MinimumDistance   *float64             `json:"minimum_distance"`
	RMS               *float64             `json:"rms"`
	SourceURL         *string              `json:"source_url"`
	DetailURL         *string              `json:"detail_url"`
	FirstSeenAt       time.Time            `json:"first_seen_at"`
	LastSeenAt        time.Time            `json:"last_seen_at"`
	UpdatedAt         time.Time            `json:"updated_at"`
}

func NewProviderObservation(event earthquake.Event, mode string, baselineComplete bool, producedAt time.Time) (ProviderObservationV1, error) {
	if err := event.Validate(); err != nil {
		return ProviderObservationV1{}, err
	}
	if !validIngestionMode(mode) {
		return ProviderObservationV1{}, fmt.Errorf("invalid ingestion mode %q", mode)
	}
	hash, err := event.CanonicalPayloadHash()
	if err != nil {
		return ProviderObservationV1{}, err
	}
	identity := fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%s", event.Provider, event.ExternalID,
		event.EffectiveObservationChannel(), event.SourceUpdatedAt.UTC().Format(time.RFC3339Nano), hex.EncodeToString(hash[:]))
	return ProviderObservationV1{
		Schema: ProviderObservationSchema, MessageID: deterministicID(identity), ProducedAt: producedAt.UTC(),
		Mode: mode, BaselineComplete: baselineComplete,
		Observation: ProviderObservationPayloadV1{
			Provider: event.Provider, ExternalID: event.ExternalID, ObservationChannel: event.EffectiveObservationChannel(),
			SolutionClass: event.EffectiveSolutionClass(), OccurredAt: event.OccurredAt.UTC(), SourceUpdatedAt: event.SourceUpdatedAt.UTC(),
			Latitude: event.Latitude, Longitude: event.Longitude, DepthKM: event.DepthKM, Magnitude: event.Magnitude,
			MagnitudeType: event.MagnitudeType, Place: event.Place, Title: event.Title, Status: event.Status,
			EventType: event.EventType, AlertLevel: event.AlertLevel, Tsunami: event.Tsunami, Significance: event.Significance,
			FeltReports: event.FeltReports, CDI: event.CDI, MMI: event.MMI, StationCount: event.StationCount,
			AzimuthalGap: event.AzimuthalGap, MinimumDistance: event.MinimumDistance, RMS: event.RMS,
			SourceURL: event.SourceURL, DetailURL: event.DetailURL, RawPayload: append(json.RawMessage(nil), event.RawPayload...),
		},
	}, nil
}

func (message ProviderObservationV1) Event() (earthquake.Event, error) {
	if message.Schema != ProviderObservationSchema {
		return earthquake.Event{}, fmt.Errorf("%w: %q", ErrUnsupportedSchema, message.Schema)
	}
	payload := message.Observation
	event := earthquake.Event{
		Provider: payload.Provider, ExternalID: payload.ExternalID, ObservationChannel: payload.ObservationChannel,
		SolutionClass: payload.SolutionClass, OccurredAt: payload.OccurredAt, SourceUpdatedAt: payload.SourceUpdatedAt,
		Latitude: payload.Latitude, Longitude: payload.Longitude, DepthKM: payload.DepthKM, Magnitude: payload.Magnitude,
		MagnitudeType: payload.MagnitudeType, Place: payload.Place, Title: payload.Title, Status: payload.Status,
		EventType: payload.EventType, AlertLevel: payload.AlertLevel, Tsunami: payload.Tsunami, Significance: payload.Significance,
		FeltReports: payload.FeltReports, CDI: payload.CDI, MMI: payload.MMI, StationCount: payload.StationCount,
		AzimuthalGap: payload.AzimuthalGap, MinimumDistance: payload.MinimumDistance, RMS: payload.RMS,
		SourceURL: payload.SourceURL, DetailURL: payload.DetailURL, RawPayload: append(json.RawMessage(nil), payload.RawPayload...),
	}
	return event, event.Validate()
}

func NewIncidentChanged(change earthquake.Change, mode string, baselineComplete bool, producedAt time.Time) IncidentChangedV1 {
	event := change.Current
	identity := fmt.Sprintf("%s\x00%d", event.ID, event.Version)
	var previous *CanonicalIncidentV1
	if change.Previous != nil {
		snapshot := canonicalIncident(*change.Previous)
		previous = &snapshot
	}
	return IncidentChangedV1{
		Schema: IncidentChangedSchema, MessageID: deterministicID(identity), ProducedAt: producedAt.UTC(),
		Operation: change.Kind, IngestionMode: mode, BaselineComplete: baselineComplete,
		NotificationsEligible: mode == "realtime" && baselineComplete, Previous: previous,
		ChangedFields: change.ChangedFields, Incident: canonicalIncident(event),
	}
}

func (message IncidentChangedV1) Change() (earthquake.Change, error) {
	if message.Schema != IncidentChangedSchema {
		return earthquake.Change{}, fmt.Errorf("%w: %q", ErrUnsupportedSchema, message.Schema)
	}
	if message.Operation != earthquake.Inserted && message.Operation != earthquake.Updated {
		return earthquake.Change{}, fmt.Errorf("invalid incident operation %q", message.Operation)
	}
	if !validIngestionMode(message.IngestionMode) {
		return earthquake.Change{}, fmt.Errorf("invalid ingestion mode %q", message.IngestionMode)
	}
	if message.NotificationsEligible != (message.IngestionMode == "realtime" && message.BaselineComplete) {
		return earthquake.Change{}, errors.New("notification eligibility does not match ingestion state")
	}
	if message.Incident.ID == uuid.Nil || message.Incident.Version <= 0 {
		return earthquake.Change{}, errors.New("canonical incident identity is incomplete")
	}
	current := message.Incident.event()
	var previous *earthquake.Event
	if message.Previous != nil {
		value := message.Previous.event()
		previous = &value
	}
	return earthquake.Change{Kind: message.Operation, Previous: previous, Current: current,
		ChangedFields: message.ChangedFields}, nil
}

func canonicalIncident(event earthquake.Event) CanonicalIncidentV1 {
	return CanonicalIncidentV1{
		ID: event.ID, Version: event.Version, Lifecycle: event.Lifecycle, PreferredProvider: event.Provider,
		PreferredID: event.ExternalID, OccurredAt: event.OccurredAt, SourceUpdatedAt: event.SourceUpdatedAt,
		Latitude: event.Latitude, Longitude: event.Longitude, DepthKM: event.DepthKM, Magnitude: event.Magnitude,
		MagnitudeType: event.MagnitudeType, Place: event.Place, Title: event.Title, Status: event.Status,
		EventType: event.EventType, AlertLevel: event.AlertLevel, Tsunami: event.Tsunami, Significance: event.Significance,
		FeltReports: event.FeltReports, CDI: event.CDI, MMI: event.MMI, StationCount: event.StationCount,
		AzimuthalGap: event.AzimuthalGap, MinimumDistance: event.MinimumDistance, RMS: event.RMS,
		SourceURL: event.SourceURL, DetailURL: event.DetailURL, FirstSeenAt: event.FirstSeenAt,
		LastSeenAt: event.LastSeenAt, UpdatedAt: event.UpdatedAt,
	}
}

func (incident CanonicalIncidentV1) event() earthquake.Event {
	return earthquake.Event{
		ID: incident.ID, Version: incident.Version, Lifecycle: incident.Lifecycle,
		Provider: incident.PreferredProvider, ExternalID: incident.PreferredID,
		OccurredAt: incident.OccurredAt, SourceUpdatedAt: incident.SourceUpdatedAt,
		Latitude: incident.Latitude, Longitude: incident.Longitude, DepthKM: incident.DepthKM,
		Magnitude: incident.Magnitude, MagnitudeType: incident.MagnitudeType, Place: incident.Place,
		Title: incident.Title, Status: incident.Status, EventType: incident.EventType,
		AlertLevel: incident.AlertLevel, Tsunami: incident.Tsunami, Significance: incident.Significance,
		FeltReports: incident.FeltReports, CDI: incident.CDI, MMI: incident.MMI,
		StationCount: incident.StationCount, AzimuthalGap: incident.AzimuthalGap,
		MinimumDistance: incident.MinimumDistance, RMS: incident.RMS, SourceURL: incident.SourceURL,
		DetailURL: incident.DetailURL, FirstSeenAt: incident.FirstSeenAt, LastSeenAt: incident.LastSeenAt,
		UpdatedAt: incident.UpdatedAt,
	}
}

func Marshal(value any) ([]byte, error) {
	return json.Marshal(value)
}

func UnmarshalProviderObservation(data []byte) (ProviderObservationV1, error) {
	var message ProviderObservationV1
	if err := json.Unmarshal(data, &message); err != nil {
		return ProviderObservationV1{}, err
	}
	if message.Schema != ProviderObservationSchema {
		return ProviderObservationV1{}, fmt.Errorf("%w: %q", ErrUnsupportedSchema, message.Schema)
	}
	if message.MessageID == uuid.Nil || message.ProducedAt.IsZero() || message.Mode == "" {
		return ProviderObservationV1{}, errors.New("provider observation envelope is incomplete")
	}
	if !validIngestionMode(message.Mode) {
		return ProviderObservationV1{}, fmt.Errorf("invalid ingestion mode %q", message.Mode)
	}
	if _, err := message.Event(); err != nil {
		return ProviderObservationV1{}, err
	}
	return message, nil
}

func UnmarshalIncidentChanged(data []byte) (IncidentChangedV1, error) {
	var message IncidentChangedV1
	if err := json.Unmarshal(data, &message); err != nil {
		return IncidentChangedV1{}, err
	}
	if message.Schema != IncidentChangedSchema {
		return IncidentChangedV1{}, fmt.Errorf("%w: %q", ErrUnsupportedSchema, message.Schema)
	}
	if message.MessageID == uuid.Nil || message.ProducedAt.IsZero() {
		return IncidentChangedV1{}, errors.New("incident change envelope is incomplete")
	}
	if _, err := message.Change(); err != nil {
		return IncidentChangedV1{}, err
	}
	return message, nil
}

func deterministicID(identity string) uuid.UUID {
	return uuid.NewHash(sha256.New(), uuid.NameSpaceOID, []byte(identity), 5)
}

func validIngestionMode(mode string) bool {
	switch mode {
	case "baseline", "realtime", "backfill", "recovery":
		return true
	default:
		return false
	}
}
