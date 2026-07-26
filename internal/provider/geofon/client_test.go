package geofon

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/example/earthquake-service/internal/provider"
)

type fixedClock struct{ value time.Time }

func (c fixedClock) Now() time.Time { return c.value }

func TestParseGEOFONText(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "testdata", "geofon", "events.txt"))
	if err != nil {
		t.Fatal(err)
	}
	events, invalid, err := Parse(data)
	if err != nil || invalid != 1 || len(events) != 1 {
		t.Fatalf("events=%+v invalid=%d err=%v", events, invalid, err)
	}
	event := events[0]
	if event.Provider != ProviderName || event.ExternalID != "gfz2026nzqk" || event.Magnitude == nil || *event.Magnitude != 5.37 ||
		event.DepthKM == nil || *event.DepthKM != 10 || event.DetailURL == nil {
		t.Fatalf("event=%+v", event)
	}
}

func TestClientBuildsBoundedRealtimeQuery(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("format") != "text" || r.URL.Query().Get("starttime") == "" || r.URL.Query().Get("endtime") == "" ||
			r.Header.Get("User-Agent") != "shaker/test" {
			t.Fatalf("query=%v headers=%v", r.URL.Query(), r.Header)
		}
		_, _ = w.Write([]byte("#EventID|Time|Latitude|Longitude|Depth/km|MagType|Magnitude|EventLocationName|EventType\n"))
	}))
	defer server.Close()
	client := New(server.URL, "shaker/test", fixedClock{value: time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)},
		time.Second, time.Hour, 1024)
	if _, _, err := client.FetchRealtime(context.Background(), provider.CacheValidators{}); err != nil {
		t.Fatal(err)
	}
}
