package emsc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/example/earthquake-service/internal/domain/earthquake"
)

const (
	ProviderName     = "emsc"
	FDSNChannel      = "emsc_fdsn"
	WebSocketChannel = "emsc_standing_order"
)

type featureCollection struct {
	Type     string            `json:"type"`
	Features []json.RawMessage `json:"features"`
}

type feature struct {
	Type       string          `json:"type"`
	Geometry   geometry        `json:"geometry"`
	Properties json.RawMessage `json:"properties"`
}

type geometry struct {
	Type        string        `json:"type"`
	Coordinates []json.Number `json:"coordinates"`
}

type properties struct {
	UnID          string   `json:"unid"`
	SourceID      string   `json:"source_id"`
	SourceCatalog string   `json:"source_catalog"`
	Time          string   `json:"time"`
	LastUpdate    string   `json:"lastupdate"`
	Latitude      *float64 `json:"lat"`
	Longitude     *float64 `json:"lon"`
	Depth         *float64 `json:"depth"`
	Magnitude     *float64 `json:"mag"`
	MagnitudeType *string  `json:"magtype"`
	EventType     *string  `json:"evtype"`
	Author        *string  `json:"auth"`
	Region        *string  `json:"flynn_region"`
}

type standingOrderMessage struct {
	Action string          `json:"action"`
	Data   json.RawMessage `json:"data"`
}

func ParseFDSN(data []byte) ([]earthquake.Event, int, error) {
	var collection featureCollection
	if err := decodeJSON(data, &collection); err != nil {
		return nil, 0, fmt.Errorf("decode EMSC FDSN JSON: %w", err)
	}
	rawFeatures := collection.Features
	if collection.Type == "Feature" {
		rawFeatures = []json.RawMessage{append([]byte(nil), data...)}
	} else if collection.Type != "FeatureCollection" || rawFeatures == nil {
		return nil, 0, errors.New("invalid EMSC GeoJSON document")
	}
	events := make([]earthquake.Event, 0, len(rawFeatures))
	invalid := 0
	for _, raw := range rawFeatures {
		event, err := parseFeature(raw, FDSNChannel, earthquake.ConfirmedSolution)
		if err != nil {
			invalid++
			continue
		}
		events = append(events, event)
	}
	return events, invalid, nil
}

func ParseStandingOrder(data []byte) (earthquake.Event, error) {
	var message standingOrderMessage
	if err := decodeJSON(data, &message); err != nil {
		return earthquake.Event{}, fmt.Errorf("decode EMSC standing-order message: %w", err)
	}
	solution := earthquake.PreliminarySolution
	switch strings.ToLower(message.Action) {
	case "insert", "create", "update":
	case "delete", "remove":
		solution = earthquake.RetractedSolution
	default:
		return earthquake.Event{}, fmt.Errorf("unsupported EMSC action %q", message.Action)
	}
	if len(message.Data) == 0 || bytes.Equal(message.Data, []byte("null")) {
		return earthquake.Event{}, errors.New("EMSC standing-order message has no data")
	}
	event, err := parseFeature(message.Data, WebSocketChannel, solution)
	if err != nil {
		return earthquake.Event{}, err
	}
	event.RawPayload = append([]byte(nil), data...)
	return event, event.Validate()
}

func parseFeature(raw json.RawMessage, channel string, solution earthquake.SolutionClass) (earthquake.Event, error) {
	var value feature
	if err := decodeJSON(raw, &value); err != nil {
		return earthquake.Event{}, err
	}
	if value.Type != "Feature" || value.Geometry.Type != "Point" {
		return earthquake.Event{}, errors.New("malformed EMSC feature")
	}
	var props properties
	if err := decodeJSON(value.Properties, &props); err != nil {
		return earthquake.Event{}, err
	}
	if props.UnID == "" || props.Time == "" || props.LastUpdate == "" {
		return earthquake.Event{}, errors.New("EMSC feature is missing identity or timestamps")
	}
	occurredAt, err := time.Parse(time.RFC3339Nano, props.Time)
	if err != nil {
		return earthquake.Event{}, fmt.Errorf("parse EMSC origin time: %w", err)
	}
	updatedAt, err := time.Parse(time.RFC3339Nano, props.LastUpdate)
	if err != nil {
		return earthquake.Event{}, fmt.Errorf("parse EMSC update time: %w", err)
	}
	latitude, longitude, err := coordinates(value.Geometry, props)
	if err != nil {
		return earthquake.Event{}, err
	}
	eventType := normalizeEventType(props.EventType)
	status := solutionStatus(solution)
	detailURL := "https://www.seismicportal.eu/fdsnws/event/1/query?format=json&eventid=" + props.UnID
	event := earthquake.Event{
		Provider: ProviderName, ExternalID: props.UnID, OccurredAt: occurredAt.UTC(), SourceUpdatedAt: updatedAt.UTC(),
		Latitude: latitude, Longitude: longitude, DepthKM: props.Depth, Magnitude: props.Magnitude,
		MagnitudeType: props.MagnitudeType, Place: props.Region, Title: props.Region, Status: &status,
		EventType: eventType, DetailURL: &detailURL, RawPayload: append([]byte(nil), raw...),
		ObservationChannel: channel, SolutionClass: solution,
	}
	return event, event.Validate()
}

func coordinates(g geometry, props properties) (float64, float64, error) {
	if props.Latitude != nil && props.Longitude != nil {
		return *props.Latitude, *props.Longitude, nil
	}
	if len(g.Coordinates) < 2 {
		return 0, 0, errors.New("EMSC feature is missing coordinates")
	}
	longitude, err := g.Coordinates[0].Float64()
	if err != nil {
		return 0, 0, err
	}
	latitude, err := g.Coordinates[1].Float64()
	if err != nil {
		return 0, 0, err
	}
	return latitude, longitude, nil
}

func normalizeEventType(value *string) *string {
	if value == nil {
		return nil
	}
	normalized := strings.ToLower(strings.TrimSpace(*value))
	switch normalized {
	case "ke", "earthquake":
		normalized = "earthquake"
	case "qb", "quarry blast":
		normalized = "quarry blast"
	}
	return &normalized
}

func solutionStatus(solution earthquake.SolutionClass) string {
	switch solution {
	case earthquake.PreliminarySolution:
		return "preliminary"
	case earthquake.RetractedSolution:
		return "deleted"
	default:
		return "confirmed"
	}
}

func decodeJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("multiple JSON values")
	}
	return nil
}
