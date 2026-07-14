package usgs

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/example/earthquake-service/internal/domain/earthquake"
	"github.com/example/earthquake-service/internal/provider"
)

func TestParseFeedKeepsNullableFieldsAndRejectsIndividualFeature(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "testdata", "usgs", "valid_feed.json"))
	if err != nil {
		t.Fatal(err)
	}
	events, invalid, err := ParseFeed(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || invalid != 1 {
		t.Fatalf("events=%d invalid=%d", len(events), invalid)
	}
	e := events[0]
	if e.Magnitude == nil || *e.Magnitude != 5.4 || e.FeltReports != nil || e.AlertLevel != nil {
		t.Fatalf("nullable fields decoded incorrectly: %+v", e)
	}
	if e.DepthKM == nil || *e.DepthKM != 18.3 || e.Tsunami == nil || *e.Tsunami {
		t.Fatalf("geometry/tsunami decoded incorrectly: %+v", e)
	}
	if e.SolutionClass != earthquake.ReviewedSolution {
		t.Fatalf("solution class=%q, want reviewed", e.SolutionClass)
	}
}

func TestRealtimeConditionalRequestAnd304(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") != `"feed-v1"` || r.Header.Get("User-Agent") != "test-agent" {
			t.Errorf("missing conditional or user agent headers")
		}
		w.Header().Set("ETag", `"feed-v1"`)
		w.WriteHeader(http.StatusNotModified)
	}))
	defer server.Close()
	client := New(server.URL, server.URL, "test-agent", time.Second, 1024)
	events, metadata, err := client.FetchRealtime(context.Background(), provider.CacheValidators{ETag: `"feed-v1"`})
	if err != nil || !metadata.NotModified || len(events) != 0 {
		t.Fatalf("events=%d metadata=%+v err=%v", len(events), metadata, err)
	}
}

func TestProviderRetries429AndBoundsBody(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts++
		if attempts == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{"type":"FeatureCollection","features":[]}`))
	}))
	defer server.Close()
	client := New(server.URL, server.URL, "test-agent", time.Second, 1024)
	if _, _, err := client.FetchRealtime(context.Background(), provider.CacheValidators{}); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d", attempts)
	}
	oversized := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(make([]byte, 65))
	}))
	defer oversized.Close()
	client = New(oversized.URL, oversized.URL, "test-agent", time.Second, 64)
	if _, _, err := client.FetchRealtime(context.Background(), provider.CacheValidators{}); err == nil {
		t.Fatal("expected oversized response error")
	}
}
func TestParseFeedRejectsInvalidTopLevel(t *testing.T) {
	if _, _, err := ParseFeed([]byte(`{"type":"Feature","features":[]}`)); err == nil {
		t.Fatal("expected error")
	}
	if _, _, err := ParseFeed([]byte(`not json`)); err == nil {
		t.Fatal("expected error")
	}
}
