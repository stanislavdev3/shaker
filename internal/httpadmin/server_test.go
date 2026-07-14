package httpadmin

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/example/earthquake-service/internal/administration"
	"github.com/example/earthquake-service/internal/domain/earthquake"
	"github.com/example/earthquake-service/internal/domain/notification"
)

type adminRepositoryStub struct {
	role               administration.Role
	subscription       notification.Subscription
	notificationFilter administration.NotificationFilter
}

func (*adminRepositoryStub) BootstrapOwners(context.Context, []string, time.Time) error { return nil }
func (r *adminRepositoryStub) RoleForEmail(context.Context, string) (administration.Role, error) {
	return r.role, nil
}
func (*adminRepositoryStub) ListAdminIncidents(context.Context, administration.IncidentFilter) ([]earthquake.Event, error) {
	place := "<script>alert(1)</script>"
	return []earthquake.Event{{ID: uuid.New(), OccurredAt: time.Unix(1, 0), Place: &place}}, nil
}
func (*adminRepositoryStub) AdminIncident(context.Context, uuid.UUID) (administration.IncidentDetail, error) {
	return administration.IncidentDetail{}, administration.ErrNotFound
}
func (*adminRepositoryStub) ListAdminSubscriptions(context.Context, administration.PageFilter) ([]notification.Subscription, error) {
	return nil, nil
}
func (r *adminRepositoryStub) AdminSubscription(context.Context, uuid.UUID) (notification.Subscription, error) {
	return r.subscription, nil
}
func (r *adminRepositoryStub) ListAdminNotifications(_ context.Context, filter administration.NotificationFilter) ([]administration.NotificationItem, error) {
	r.notificationFilter = filter
	return nil, nil
}
func (*adminRepositoryStub) AdminNotification(context.Context, uuid.UUID) (administration.NotificationItem, error) {
	return administration.NotificationItem{}, administration.ErrNotFound
}
func (*adminRepositoryStub) ListAdminAudit(context.Context, administration.PageFilter) ([]administration.AuditEntry, error) {
	return nil, nil
}

func TestAdminPagesRequireHostRoleAndEscapeHTML(t *testing.T) {
	repository := &adminRepositoryStub{role: administration.Owner}
	handler := newTestAdminHandler(t, repository)

	wrongHost := httptest.NewRequest(http.MethodGet, "http://wrong.example/incidents", nil)
	wrongHost.Host = "wrong.example"
	wrongResponse := httptest.NewRecorder()
	handler.ServeHTTP(wrongResponse, wrongHost)
	if wrongResponse.Code != http.StatusNotFound {
		t.Fatalf("wrong-host status=%d", wrongResponse.Code)
	}

	request := httptest.NewRequest(http.MethodGet, "http://admin.example/incidents", nil)
	request.Host = "admin.example"
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "<script>") || !strings.Contains(response.Body.String(), "&lt;script&gt;") {
		t.Fatalf("HTML was not escaped: %s", response.Body.String())
	}
	for _, expected := range []string{`navbar-vertical`, `class="nav-item active"`, `class="mobile-nav"`, `Shaker`, `Operations console`, `class="table-meta"`} {
		if !strings.Contains(response.Body.String(), expected) {
			t.Fatalf("admin shell is missing %q", expected)
		}
	}
	if response.Header().Get("Content-Security-Policy") == "" || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("security headers are missing")
	}
}

func TestEmbeddedAdminStylesheetIsServed(t *testing.T) {
	handler := newTestAdminHandler(t, &adminRepositoryStub{role: administration.Owner})
	request := httptest.NewRequest(http.MethodGet, "http://admin.example/assets/style.css", nil)
	request.Host = "admin.example"
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if !strings.HasPrefix(response.Header().Get("Content-Type"), "text/css") {
		t.Fatalf("content type=%q", response.Header().Get("Content-Type"))
	}
	for _, expected := range []string{"grid-template-columns: 272px", "position: fixed", "grid-column: 2", ".mobile-nav", ".incident-hero", ".status-success", "@media"} {
		if !strings.Contains(response.Body.String(), expected) {
			t.Fatalf("stylesheet is missing %q", expected)
		}
	}
}

func TestNotificationsUseValidatedDeliveryTabs(t *testing.T) {
	repository := &adminRepositoryStub{role: administration.Owner}
	handler := newTestAdminHandler(t, repository)
	request := httptest.NewRequest(http.MethodGet, "http://admin.example/notifications?type=telegram_channel", nil)
	request.Host = "admin.example"
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if repository.notificationFilter.DeliveryClass != "telegram_channel" {
		t.Fatalf("delivery class=%q", repository.notificationFilter.DeliveryClass)
	}
	for _, expected := range []string{"Telegram channel", "Telegram private", "Webhooks", `class="tab active"`} {
		if !strings.Contains(response.Body.String(), expected) {
			t.Fatalf("notifications page is missing %q", expected)
		}
	}

	request = httptest.NewRequest(http.MethodGet, "http://admin.example/notifications?type=unknown", nil)
	request.Host = "admin.example"
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("invalid type status=%d", response.Code)
	}
}

func TestEmbeddedTablerStylesheetIsServed(t *testing.T) {
	handler := newTestAdminHandler(t, &adminRepositoryStub{role: administration.Owner})
	request := httptest.NewRequest(http.MethodGet, "http://admin.example/assets/tabler.min.css", nil)
	request.Host = "admin.example"
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), "Tabler v1.4.0") {
		t.Fatal("embedded Tabler asset is missing its version banner")
	}
}

func TestEmbeddedStylesheetsAreServedThroughAdminMount(t *testing.T) {
	adminHandler := newTestAdminHandler(t, &adminRepositoryStub{role: administration.Owner})
	root := chi.NewRouter()
	root.Mount("/admin", adminHandler)

	for _, path := range []string{"/admin/assets/tabler.min.css", "/admin/assets/style.css"} {
		request := httptest.NewRequest(http.MethodGet, "http://admin.example"+path, nil)
		request.Host = "admin.example"
		response := httptest.NewRecorder()
		root.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("path=%s status=%d body=%s", path, response.Code, response.Body.String())
		}
		if !strings.HasPrefix(response.Header().Get("Content-Type"), "text/css") {
			t.Fatalf("path=%s content type=%q", path, response.Header().Get("Content-Type"))
		}
	}
}

func TestEnabledAdminRejectsMissingCloudflareToken(t *testing.T) {
	repository := &adminRepositoryStub{role: administration.Owner}
	service := administration.New(repository, time.Now)
	handler, err := New(service, slog.New(slog.NewTextHandler(io.Discard, nil)), Config{
		Host: "admin.example", TeamDomain: "team.cloudflareaccess.com", Audience: "audience",
		CSRFKey: []byte("0123456789abcdef0123456789abcdef"), Now: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "https://admin.example/incidents", nil)
	request.Host = "admin.example"
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "incident") {
		t.Fatal("unauthenticated response disclosed page data")
	}
}

func TestListCursorIsSignedAndScoped(t *testing.T) {
	server := &Server{csrfKey: []byte("0123456789abcdef0123456789abcdef")}
	now := time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)
	id := uuid.New()
	cursor := server.encodeCursor("incidents", now, id)
	decodedTime, decodedID, err := server.decodeCursor(cursor, "incidents")
	if err != nil || !decodedTime.Equal(now) || decodedID != id {
		t.Fatalf("decoded=%s %s err=%v", decodedTime, decodedID, err)
	}
	if _, _, err := server.decodeCursor(cursor, "subscriptions"); err == nil {
		t.Fatal("expected cursor scope rejection")
	}
	if _, _, err := server.decodeCursor(cursor+"x", "incidents"); err == nil {
		t.Fatal("expected cursor signature rejection")
	}
}

func TestSubscriptionPageNeverRendersSecretsOrExactPersonalData(t *testing.T) {
	chatID := int64(123456789)
	latitude, longitude := 42.8746, 74.5698
	username := "private_username"
	repository := &adminRepositoryStub{role: administration.Owner, subscription: notification.Subscription{
		ID: uuid.New(), Name: "telegram:123456789", Channel: "telegram", TelegramChatID: &chatID, TelegramChatUsername: &username,
		CenterLatitude: &latitude, CenterLongitude: &longitude, EncryptedWebhookSecret: []byte("secret-material"),
	}}
	handler := newTestAdminHandler(t, repository)
	request := httptest.NewRequest(http.MethodGet, "http://admin.example/subscriptions/"+repository.subscription.ID.String(), nil)
	request.Host = "admin.example"
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	body := response.Body.String()
	for _, forbidden := range []string{"secret-material", "123456789", "private_username", "42.8746", "74.5698"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("page disclosed %q", forbidden)
		}
	}
}

func TestCSRFMiddlewareRejectsMutationWithoutOriginAndToken(t *testing.T) {
	server := &Server{host: "admin.example", csrfKey: []byte("0123456789abcdef0123456789abcdef"), now: time.Now}
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	handler := server.requireCSRF(next)
	request := httptest.NewRequest(http.MethodPost, "https://admin.example/action", nil)
	identity := administration.Identity{Subject: "user-1", Email: "admin@example.com", Role: administration.Owner}
	request = request.WithContext(context.WithValue(request.Context(), identityContextKey{}, identity))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("status=%d", response.Code)
	}

	request = httptest.NewRequest(http.MethodPost, "https://admin.example/action", nil)
	request = request.WithContext(context.WithValue(request.Context(), identityContextKey{}, identity))
	request.Header.Set("Origin", "https://admin.example")
	request.Header.Set("X-CSRF-Token", server.csrfToken(identity.Subject))
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("valid CSRF status=%d", response.Code)
	}
}

func newTestAdminHandler(t *testing.T, repository *adminRepositoryStub) http.Handler {
	t.Helper()
	service := administration.New(repository, time.Now)
	handler, err := New(service, slog.New(slog.NewTextHandler(io.Discard, nil)), Config{
		Host: "admin.example", DevelopmentEmail: "admin@example.com", CSRFKey: []byte("0123456789abcdef0123456789abcdef"), Now: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return handler
}
