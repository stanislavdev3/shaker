package administration

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/example/earthquake-service/internal/domain/earthquake"
	"github.com/example/earthquake-service/internal/domain/notification"
)

var ErrNotFound = errors.New("administration resource not found")

type Role string

const (
	Viewer   Role = "viewer"
	Operator Role = "operator"
	Owner    Role = "owner"
)

func (r Role) Valid() bool { return r == Viewer || r == Operator || r == Owner }

type Identity struct {
	Subject string
	Email   string
	Role    Role
}

type IncidentFilter struct {
	Provider     string
	Lifecycle    string
	Status       string
	MinMagnitude *float64
	BeforeAt     *time.Time
	BeforeID     *uuid.UUID
	Limit        int
}

type PageFilter struct {
	BeforeAt *time.Time
	BeforeID *uuid.UUID
	Limit    int
}

type NotificationFilter struct {
	PageFilter
	DeliveryClass string
}

type SourceRecord struct {
	ID                       uuid.UUID
	Provider, ExternalID     string
	LatestObservationChannel string
	SolutionClass            string
	SourceURL, DetailURL     *string
	Version                  int64
	SourceUpdatedAt          time.Time
	FirstSeenAt, LastSeenAt  time.Time
}

type Observation struct {
	ID                     uuid.UUID
	SourceRecordID         uuid.UUID
	SourceVersion          int64
	Channel, SolutionClass string
	SourceUpdatedAt        time.Time
	ReceivedAt             time.Time
}

type Association struct {
	ID, SourceRecordID uuid.UUID
	Method             string
	Confidence         *float64
	AlgorithmVersion   *string
	Evidence           []byte
	Active             bool
	AssociatedAt       time.Time
	EndedAt            *time.Time
}

type IntensityEvaluation struct {
	ID, SubscriptionID                                              uuid.UUID
	EarthquakeVersion                                               int64
	ModelName, ModelVersion, Decision, DecisionPolicyVersion        string
	MeanMMI, SigmaMMI, LowerMMI, UpperMMI, ThresholdMMI             float64
	DecisionBoundaryMMI                                             float64
	EpicentralDistanceKM, HypocentralDistanceKM, Magnitude, DepthKM float64
	CreatedAt                                                       time.Time
}

type NotificationMatchingAudit struct {
	ID, EarthquakeID                                                   uuid.UUID
	EarthquakeVersion                                                  int64
	Mode, ModelVersion, DecisionPolicyVersion                          string
	BaselineComplete                                                   bool
	CandidateRadiusKM, CandidateMinimumMMI                             float64
	SelectedSubscriptionCount, IntensityCandidateCount                 int
	IntensityEvaluationCount, NotifyDecisionCount, BelowThresholdCount int
	EstimateErrorCount, TriggerCount                                   int
	CreatedAt                                                          time.Time
}

type Revision struct {
	ID              uuid.UUID
	Version         int64
	SourceUpdatedAt time.Time
	ChangedFields   []byte
	CreatedAt       time.Time
}

type IncidentDetail struct {
	Incident       earthquake.Event
	Provenance     []byte
	Sources        []SourceRecord
	Observations   []Observation
	Associations   []Association
	Evaluations    []IntensityEvaluation
	MatchingAudits []NotificationMatchingAudit
	Revisions      []Revision
}

type NotificationItem struct {
	ID, SubscriptionID, EarthquakeID uuid.UUID
	Kind, DeliveryClass, Status      string
	EarthquakeVersion                int64
	DeliveredVersion                 *int64
	TriggerType, LastError           *string
	Payload                          []byte
	AttemptCount                     int
	NextAttemptAt                    time.Time
	SentAt                           *time.Time
	CreatedAt, UpdatedAt             time.Time
}

type AuditEntry struct {
	ID                               uuid.UUID
	ActorSubject, ActorEmail         string
	Role                             Role
	Action, ResourceType, ResourceID string
	Reason                           *string
	RequestID, SourceIP, UserAgent   *string
	CreatedAt                        time.Time
}

type Repository interface {
	BootstrapOwners(context.Context, []string, time.Time) error
	RoleForEmail(context.Context, string) (Role, error)
	ListAdminIncidents(context.Context, IncidentFilter) ([]earthquake.Event, error)
	AdminIncident(context.Context, uuid.UUID) (IncidentDetail, error)
	ListAdminSubscriptions(context.Context, PageFilter) ([]notification.Subscription, error)
	AdminSubscription(context.Context, uuid.UUID) (notification.Subscription, error)
	ListAdminNotifications(context.Context, NotificationFilter) ([]NotificationItem, error)
	AdminNotification(context.Context, uuid.UUID) (NotificationItem, error)
	ListAdminAudit(context.Context, PageFilter) ([]AuditEntry, error)
}

type Service struct {
	repo Repository
	now  func() time.Time
}

func New(repo Repository, now func() time.Time) *Service {
	return &Service{repo: repo, now: now}
}

func (s *Service) BootstrapOwners(ctx context.Context, emails []string) error {
	normalized := make([]string, 0, len(emails))
	for _, email := range emails {
		if email = normalizeEmail(email); email != "" {
			normalized = append(normalized, email)
		}
	}
	return s.repo.BootstrapOwners(ctx, normalized, s.now())
}

func (s *Service) Authorize(ctx context.Context, subject, email string) (Identity, error) {
	email = normalizeEmail(email)
	if subject == "" || email == "" {
		return Identity{}, ErrNotFound
	}
	role, err := s.repo.RoleForEmail(ctx, email)
	if errors.Is(err, ErrNotFound) || (err == nil && !role.Valid()) {
		return Identity{}, ErrNotFound
	}
	if err != nil {
		return Identity{}, err
	}
	return Identity{Subject: subject, Email: email, Role: role}, nil
}

func (s *Service) Incidents(ctx context.Context, filter IncidentFilter) ([]earthquake.Event, error) {
	if filter.Limit < 1 || filter.Limit > 100 {
		filter.Limit = 50
	}
	return s.repo.ListAdminIncidents(ctx, filter)
}

func (s *Service) Incident(ctx context.Context, id uuid.UUID) (IncidentDetail, error) {
	return s.repo.AdminIncident(ctx, id)
}

func (s *Service) Subscriptions(ctx context.Context, filter PageFilter) ([]notification.Subscription, error) {
	return s.repo.ListAdminSubscriptions(ctx, normalizePage(filter))
}

func (s *Service) Subscription(ctx context.Context, id uuid.UUID) (notification.Subscription, error) {
	return s.repo.AdminSubscription(ctx, id)
}

func (s *Service) Notifications(ctx context.Context, filter NotificationFilter) ([]NotificationItem, error) {
	filter.PageFilter = normalizePage(filter.PageFilter)
	return s.repo.ListAdminNotifications(ctx, filter)
}

func (s *Service) Notification(ctx context.Context, id uuid.UUID) (NotificationItem, error) {
	return s.repo.AdminNotification(ctx, id)
}

func (s *Service) Audit(ctx context.Context, filter PageFilter) ([]AuditEntry, error) {
	return s.repo.ListAdminAudit(ctx, normalizePage(filter))
}

func normalizeEmail(value string) string { return strings.ToLower(strings.TrimSpace(value)) }

func normalizePage(filter PageFilter) PageFilter {
	if filter.Limit < 1 || filter.Limit > 100 {
		filter.Limit = 50
	}
	return filter
}
