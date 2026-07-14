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
	)
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusNotFound {
		t.Fatalf("status=%d, want %d", response.Code, http.StatusNotFound)
	}
}
