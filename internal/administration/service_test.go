package administration

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/example/earthquake-service/internal/domain/earthquake"
	"github.com/example/earthquake-service/internal/domain/notification"
)

type repositoryStub struct {
	bootstrapped []string
	role         Role
}

func (r *repositoryStub) BootstrapOwners(_ context.Context, emails []string, _ time.Time) error {
	r.bootstrapped = emails
	return nil
}
func (r *repositoryStub) RoleForEmail(_ context.Context, _ string) (Role, error) { return r.role, nil }
func (*repositoryStub) ListAdminIncidents(context.Context, IncidentFilter) ([]earthquake.Event, error) {
	return nil, nil
}
func (*repositoryStub) AdminIncident(context.Context, uuid.UUID) (IncidentDetail, error) {
	return IncidentDetail{}, nil
}
func (*repositoryStub) ListAdminSubscriptions(context.Context, PageFilter) ([]notification.Subscription, error) {
	return nil, nil
}
func (*repositoryStub) AdminSubscription(context.Context, uuid.UUID) (notification.Subscription, error) {
	return notification.Subscription{}, nil
}
func (*repositoryStub) ListAdminNotifications(context.Context, NotificationFilter) ([]NotificationItem, error) {
	return nil, nil
}
func (*repositoryStub) AdminNotification(context.Context, uuid.UUID) (NotificationItem, error) {
	return NotificationItem{}, nil
}
func (*repositoryStub) ListAdminAudit(context.Context, PageFilter) ([]AuditEntry, error) {
	return nil, nil
}

func TestServiceNormalizesBootstrapOwnersAndIdentity(t *testing.T) {
	repository := &repositoryStub{role: Owner}
	service := New(repository, func() time.Time { return time.Unix(1, 0) })
	if err := service.BootstrapOwners(context.Background(), []string{" Admin@Example.COM ", ""}); err != nil {
		t.Fatal(err)
	}
	if len(repository.bootstrapped) != 1 || repository.bootstrapped[0] != "admin@example.com" {
		t.Fatalf("owners=%v", repository.bootstrapped)
	}
	identity, err := service.Authorize(context.Background(), "subject", " ADMIN@example.com ")
	if err != nil {
		t.Fatal(err)
	}
	if identity.Email != "admin@example.com" || identity.Role != Owner {
		t.Fatalf("identity=%+v", identity)
	}
}

func TestServiceRejectsUnknownRole(t *testing.T) {
	service := New(&repositoryStub{role: "invalid"}, time.Now)
	if _, err := service.Authorize(context.Background(), "subject", "admin@example.com"); err == nil {
		t.Fatal("expected authorization failure")
	}
}
