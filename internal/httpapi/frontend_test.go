package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Client-side routes like /event/{id} must fall back to index.html instead of
// 404ing, so deep links and refreshes resolve.
func TestFrontendSPAFallback(t *testing.T) {
	handler := frontendHandler()
	for _, path := range []string{"/", "/event/00000000-0000-0000-0000-000000000000"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: got status %d, want 200", path, rec.Code)
		}
		if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
			t.Fatalf("%s: got content-type %q, want text/html", path, ct)
		}
	}
}
