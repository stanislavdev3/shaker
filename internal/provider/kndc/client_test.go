package kndc

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

func TestParseKNDCBulletin(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "testdata", "kndc", "alarm.json"))
	if err != nil {
		t.Fatal(err)
	}
	events, invalid, err := Parse(data)
	if err != nil || invalid != 1 || len(events) != 1 {
		t.Fatalf("events=%+v invalid=%d err=%v", events, invalid, err)
	}
	event := events[0]
	if event.Provider != ProviderName || event.ExternalID != "6395" || event.Magnitude == nil || *event.Magnitude != 4.7 ||
		event.MagnitudeType == nil || *event.MagnitudeType != "mb" ||
		event.OccurredAt.Format(time.RFC3339Nano) != "2026-07-19T20:41:05.78Z" {
		t.Fatalf("event=%+v", event)
	}
	if event.SourceUpdatedAt.Format(time.RFC3339) != "2026-07-20T00:06:58Z" {
		t.Fatalf("updated=%s", event.SourceUpdatedAt)
	}
}

func TestParseKNDCBulletinDoesNotUseShiftedEpoch(t *testing.T) {
	data := []byte(`[{"id":"6396","epochtime":"1784973466","evdate":"2026-07-25","evtime":"15:57:46","evmsec":"85","lat":"42.7936","lon":"74.6332","depth":0,"mb":3.7,"gregion":"KYRGYZSTAN","lddate":"2026-07-25 22:32:32"}]`)
	events, invalid, err := Parse(data)
	if err != nil || invalid != 0 || len(events) != 1 {
		t.Fatalf("events=%+v invalid=%d err=%v", events, invalid, err)
	}
	if got := events[0].OccurredAt.Format(time.RFC3339Nano); got != "2026-07-25T15:57:46.85Z" {
		t.Fatalf("occurred_at=%s", got)
	}
}

func TestClientUsesDescendingBoundedPage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/getOriginList.php" || r.URL.Query().Get("desc") != "yes" ||
			r.URL.Query().Get("limit") != "100" || r.Header.Get("User-Agent") != "shaker/test" {
			t.Fatalf("path=%s query=%v headers=%v", r.URL.Path, r.URL.Query(), r.Header)
		}
		_, _ = w.Write([]byte("[]"))
	}))
	defer server.Close()
	client := New(server.URL, "shaker/test", time.Second, 1024)
	if _, _, err := client.FetchRealtime(context.Background(), provider.CacheValidators{}); err != nil {
		t.Fatal(err)
	}
}
