package notification

import (
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/example/earthquake-service/internal/domain/earthquake"
)

type Trigger string

const (
	NewEvent                  Trigger = "new_event"
	MagnitudeThresholdCrossed Trigger = "magnitude_threshold_crossed"
	TsunamiActivated          Trigger = "tsunami_activated"
	AlertLevelIncreased       Trigger = "alert_level_increased"
)

type Subscription struct {
	ID                        uuid.UUID
	Name                      string
	Status                    string
	Channel                   string
	WebhookURL                string
	EncryptedWebhookSecret    []byte
	MinimumMagnitude          *float64
	MaximumMagnitude          *float64
	CenterLatitude            *float64
	CenterLongitude           *float64
	RadiusKM                  *float64
	TsunamiOnly               bool
	AllowedAlertLevels        []string
	AllowedEventTypes         []string
	NotifyOnNew               bool
	NotifyOnThresholdCrossing bool
	NotifyOnTsunamiChange     bool
	NotifyOnAlertIncrease     bool
	MaximumEventAge           time.Duration
	CreatedAt                 time.Time
	UpdatedAt                 time.Time
}

func (s Subscription) Validate(maxRadius float64, production bool) error {
	if s.Name == "" || s.Channel != "webhook" {
		return ErrInvalidSubscription
	}
	geo := s.CenterLatitude != nil || s.CenterLongitude != nil || s.RadiusKM != nil
	if geo && (s.CenterLatitude == nil || s.CenterLongitude == nil || s.RadiusKM == nil || *s.RadiusKM <= 0 || *s.RadiusKM > maxRadius) {
		return ErrInvalidGeography
	}
	if s.MinimumMagnitude != nil && s.MaximumMagnitude != nil && *s.MinimumMagnitude > *s.MaximumMagnitude {
		return ErrInvalidMagnitudeRange
	}
	u, err := url.Parse(s.WebhookURL)
	allowedScheme := u.Scheme == "https" || (!production && u.Scheme == "http")
	if err != nil || u.Host == "" || u.User != nil || !allowedScheme {
		return ErrInvalidWebhookURL
	}
	return nil
}

type validationError string

func (e validationError) Error() string { return string(e) }

const (
	ErrInvalidSubscription   validationError = "invalid subscription"
	ErrInvalidGeography      validationError = "invalid geographic filter"
	ErrInvalidMagnitudeRange validationError = "minimum magnitude exceeds maximum magnitude"
	ErrInvalidWebhookURL     validationError = "invalid webhook URL"
)

func AlertSeverity(v *string) int {
	if v == nil {
		return 0
	}
	switch strings.ToLower(*v) {
	case "green":
		return 1
	case "yellow":
		return 2
	case "orange":
		return 3
	case "red":
		return 4
	default:
		return 0
	}
}

func Triggers(s Subscription, old *earthquake.Event, current earthquake.Event, mode string, now time.Time, baselineComplete bool) []Trigger {
	if s.Status != "active" || !matches(s, current) {
		return nil
	}
	var result []Trigger
	if old == nil && mode == "realtime" && baselineComplete && s.NotifyOnNew && now.Sub(current.OccurredAt) <= s.MaximumEventAge {
		result = append(result, NewEvent)
	}
	if old == nil {
		return result
	}
	if s.NotifyOnThresholdCrossing && s.MinimumMagnitude != nil && current.Magnitude != nil &&
		(old.Magnitude == nil || *old.Magnitude < *s.MinimumMagnitude) && *current.Magnitude >= *s.MinimumMagnitude {
		result = append(result, MagnitudeThresholdCrossed)
	}
	if s.NotifyOnTsunamiChange && current.Tsunami != nil && *current.Tsunami && (old.Tsunami == nil || !*old.Tsunami) {
		result = append(result, TsunamiActivated)
	}
	if s.NotifyOnAlertIncrease && AlertSeverity(current.AlertLevel) > AlertSeverity(old.AlertLevel) {
		result = append(result, AlertLevelIncreased)
	}
	return result
}

func matches(s Subscription, e earthquake.Event) bool {
	if s.MinimumMagnitude != nil && (e.Magnitude == nil || *e.Magnitude < *s.MinimumMagnitude) {
		return false
	}
	if s.MaximumMagnitude != nil && (e.Magnitude == nil || *e.Magnitude > *s.MaximumMagnitude) {
		return false
	}
	if s.TsunamiOnly && (e.Tsunami == nil || !*e.Tsunami) {
		return false
	}
	if len(s.AllowedAlertLevels) > 0 && !contains(s.AllowedAlertLevels, e.AlertLevel) {
		return false
	}
	if len(s.AllowedEventTypes) > 0 && !contains(s.AllowedEventTypes, e.EventType) {
		return false
	}
	return true
}

func contains(values []string, value *string) bool {
	if value == nil {
		return false
	}
	for _, v := range values {
		if v == *value {
			return true
		}
	}
	return false
}
