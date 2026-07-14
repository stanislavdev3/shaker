package httpapi

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/example/earthquake-service/internal/clock"
	"github.com/example/earthquake-service/internal/observability"
	"github.com/example/earthquake-service/internal/realtime"
)

func TestRootDoesNotServeFrontend(t *testing.T) {
	handler := New(
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		clock.Real{},
		observability.NewMetrics(prometheus.NewRegistry()),
		"admin-key",
		[]byte("cursor-key"),
		nil,
		false,
		2000,
		realtime.NewHub(),
		nil,
	)
	for _, path := range []string{"/", "/admin/", "/admin/incidents"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusNotFound {
			t.Fatalf("path=%s status=%d, want %d", path, response.Code, http.StatusNotFound)
		}
	}
}

func TestPublicAPINamespaceIsRegistered(t *testing.T) {
	handler := New(
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		clock.Real{},
		observability.NewMetrics(prometheus.NewRegistry()),
		"admin-key",
		[]byte("cursor-key"),
		nil,
		false,
		2000,
		realtime.NewHub(),
		nil,
	)
	request := httptest.NewRequest(http.MethodGet, "/api/earthquakes/not-a-uuid", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want route handler status %d", response.Code, http.StatusBadRequest)
	}
}

func TestVersionedNamespacesAreNotRegistered(t *testing.T) {
	handler := New(
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		clock.Real{},
		observability.NewMetrics(prometheus.NewRegistry()),
		"admin-key",
		[]byte("cursor-key"),
		nil,
		false,
		2000,
		realtime.NewHub(),
		nil,
	)
	for _, path := range []string{"/v1/earthquakes", "/v1/admin/notification-subscriptions", "/api/v1/earthquakes"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusNotFound {
			t.Fatalf("path=%s status=%d, want %d", path, response.Code, http.StatusNotFound)
		}
	}
}

func TestAdministrativeAPINamespaceIsRegistered(t *testing.T) {
	handler := New(
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		clock.Real{},
		observability.NewMetrics(prometheus.NewRegistry()),
		"admin-key",
		[]byte("cursor-key"),
		nil,
		false,
		2000,
		realtime.NewHub(),
		nil,
	)
	request := httptest.NewRequest(http.MethodGet, "/admin/api/earthquakes/not-a-uuid/revisions", nil)
	request.Header.Set("Authorization", "Bearer admin-key")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want route handler status %d", response.Code, http.StatusBadRequest)
	}
}
