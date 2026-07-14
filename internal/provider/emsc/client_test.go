package emsc

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/example/earthquake-service/internal/provider"
)

type fixedClock struct{ value time.Time }

func (c fixedClock) Now() time.Time { return c.value }

func TestFDSNFetchRealtime(t *testing.T) {
	now := time.Date(2026, 7, 14, 8, 0, 0, 0, time.UTC)
	data := fixture(t, "fdsn_feed.json")
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if got := request.URL.Query().Get("updatedafter"); got != "2026-07-14T06:00:00Z" {
			t.Errorf("updatedafter=%q", got)
		}
		if request.URL.Query().Get("format") != "json" || request.Header.Get("User-Agent") != "test-agent" {
			t.Errorf("unexpected request: %s", request.URL.String())
		}
		_, _ = writer.Write(data)
	}))
	defer server.Close()

	client := NewFDSN(server.URL, "test-agent", fixedClock{now}, time.Second, 2*time.Hour, 1<<20)
	events, metadata, err := client.FetchRealtime(context.Background(), provider.CacheValidators{})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || metadata.InvalidCount != 1 {
		t.Fatalf("events=%d metadata=%#v", len(events), metadata)
	}
}

func TestFDSNFetchRealtimeNoContent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client := NewFDSN(server.URL, "test-agent", fixedClock{time.Now()}, time.Second, time.Hour, 1024)
	events, _, err := client.FetchRealtime(context.Background(), provider.CacheValidators{})
	if err != nil || len(events) != 0 {
		t.Fatalf("events=%d err=%v", len(events), err)
	}
}

func TestFDSNResponseIsBounded(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(`{"type":"FeatureCollection","features":[]}`))
	}))
	defer server.Close()
	client := NewFDSN(server.URL, "test-agent", fixedClock{time.Now()}, time.Second, time.Hour, 8)
	_, _, err := client.FetchRealtime(context.Background(), provider.CacheValidators{})
	if err == nil {
		t.Fatal("expected bounded response error")
	}
}
