//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/example/earthquake-service/internal/domain/earthquake"
	"github.com/example/earthquake-service/internal/domain/notification"
)

func integrationRepository(t *testing.T) *Repository {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	repo, err := Open(context.Background(), url, 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(repo.Pool.Close)
	_, err = repo.Pool.Exec(context.Background(), `TRUNCATE notification_deliveries,notification_subscriptions,earthquake_revisions,earthquake_source_records,earthquakes,ingestion_runs,provider_state CASCADE`)
	if err != nil {
		t.Fatal(err)
	}
	return repo
}
func testEvent(id string, updated time.Time, mag float64) earthquake.Event {
	raw, _ := json.Marshal(map[string]any{"id": id, "updated": updated.UnixMilli(), "mag": mag})
	return earthquake.Event{Provider: "usgs", ExternalID: id, OccurredAt: updated.Add(-time.Minute), SourceUpdatedAt: updated,
		Latitude: 40.1, Longitude: 74.2, Magnitude: &mag, RawPayload: raw}
}
func TestIdempotentAndStaleUpsert(t *testing.T) {
	r := integrationRepository(t)
	ctx := context.Background()
	now := time.Now().UTC()
	event := testEvent("same", now, 5.1)
	first, err := r.ApplyBatch(ctx, []earthquake.Event{event}, "backfill", true, now)
	if err != nil {
		t.Fatal(err)
	}
	if first.Inserted != 1 {
		t.Fatalf("%+v", first)
	}
	same, err := r.ApplyBatch(ctx, []earthquake.Event{event}, "backfill", true, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if same.Unchanged != 1 {
		t.Fatalf("%+v", same)
	}
	stale := testEvent("same", now.Add(-time.Hour), 9.9)
	if _, err := r.ApplyBatch(ctx, []earthquake.Event{stale}, "backfill", true, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	var magnitude float64
	var version int64
	if err := r.Pool.QueryRow(ctx, `SELECT magnitude,version FROM earthquakes`).Scan(&magnitude, &version); err != nil {
		t.Fatal(err)
	}
	if magnitude != 5.1 || version != 1 {
		t.Fatalf("magnitude=%v version=%d", magnitude, version)
	}
	updated := testEvent("same", now.Add(time.Minute), 5.5)
	stats, err := r.ApplyBatch(ctx, []earthquake.Event{updated}, "backfill", true, now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if stats.Updated != 1 {
		t.Fatalf("%+v", stats)
	}
	var revisions int
	if err := r.Pool.QueryRow(ctx, `SELECT count(*) FROM earthquake_revisions`).Scan(&revisions); err != nil {
		t.Fatal(err)
	}
	if revisions != 1 {
		t.Fatalf("revisions=%d", revisions)
	}
}
func TestConcurrentUpsertSingleRecord(t *testing.T) {
	r := integrationRepository(t)
	ctx := context.Background()
	now := time.Now().UTC()
	event := testEvent("concurrent", now, 4.2)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := r.ApplyBatch(ctx, []earthquake.Event{event}, "backfill", true, now)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err := r.Pool.QueryRow(ctx, `SELECT count(*) FROM earthquakes WHERE preferred_external_id='concurrent'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("count=%d", count)
	}
}
func TestSpatialSearchAndStableCursorTuple(t *testing.T) {
	r := integrationRepository(t)
	ctx := context.Background()
	now := time.Now().UTC()
	for i, id := range []string{"a", "b", "c"} {
		e := testEvent(id, now, float64(i+1))
		e.Longitude += float64(i)
		if _, err := r.ApplyBatch(ctx, []earthquake.Event{e}, "backfill", true, now); err != nil {
			t.Fatal(err)
		}
	}
	radius := 150.0
	lat, lon := 40.1, 74.2
	events, err := r.List(ctx, ListFilter{Latitude: &lat, Longitude: &lon, RadiusKM: &radius, Limit: 10, Sort: "occurred_at_desc"})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("events=%d", len(events))
	}
	if events[0].DistanceKM == nil {
		t.Fatal("missing distance")
	}
}

func TestTransactionalDeliveryAndConcurrentClaim(t *testing.T) {
	r := integrationRepository(t)
	ctx := context.Background()
	now := time.Now().UTC()
	listener, err := r.Pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Release()
	if _, err := listener.Exec(ctx, `LISTEN notification_delivery_changes`); err != nil {
		t.Fatal(err)
	}
	sub, err := r.CreateSubscription(ctx, notification.Subscription{
		Name: "test", Status: "active", Channel: "webhook", WebhookURL: "https://receiver.example/hook",
		EncryptedWebhookSecret: []byte("encrypted"), MinimumMagnitude: floatPtr(4),
		NotifyOnNew: true, MaximumEventAge: 2 * time.Hour,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.ApplyBatch(ctx, []earthquake.Event{testEvent("notify", now, 5)}, "realtime", true, now); err != nil {
		t.Fatal(err)
	}
	notificationContext, cancelNotification := context.WithTimeout(ctx, time.Second)
	defer cancelNotification()
	notice, err := listener.Conn().WaitForNotification(notificationContext)
	if err != nil {
		t.Fatal(err)
	}
	if notice.Channel != "notification_delivery_changes" {
		t.Fatalf("channel=%s", notice.Channel)
	}
	var count int
	if err := r.Pool.QueryRow(ctx, `SELECT count(*) FROM notification_deliveries WHERE subscription_id=$1`, sub.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("deliveries=%d", count)
	}
	type result struct {
		jobs []Delivery
		err  error
	}
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for _, worker := range []string{"one", "two"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			jobs, err := r.Claim(ctx, worker, 10, 5*time.Minute, now)
			results <- result{jobs, err}
		}()
	}
	wg.Wait()
	close(results)
	claimed := 0
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		claimed += len(result.jobs)
	}
	if claimed != 1 {
		t.Fatalf("claimed=%d", claimed)
	}
}

func TestEarthquakeChangePublishesAfterCommit(t *testing.T) {
	r := integrationRepository(t)
	ctx := context.Background()
	listener, err := r.Pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Release()
	if _, err := listener.Exec(ctx, `LISTEN earthquake_changes`); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err := r.ApplyBatch(ctx, []earthquake.Event{testEvent("realtime-signal", now, 4.8)}, "backfill", true, now); err != nil {
		t.Fatal(err)
	}
	waitContext, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	notice, err := listener.Conn().WaitForNotification(waitContext)
	if err != nil {
		t.Fatal(err)
	}
	if notice.Channel != "earthquake_changes" {
		t.Fatalf("channel=%s", notice.Channel)
	}
	var payload struct {
		EarthquakeID string `json:"earthquake_id"`
		Version      int64  `json:"version"`
	}
	if err := json.Unmarshal([]byte(notice.Payload), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.EarthquakeID == "" || payload.Version != 1 {
		t.Fatalf("payload=%+v", payload)
	}
}

func TestAbandonedDeliveryRecovery(t *testing.T) {
	r := integrationRepository(t)
	ctx := context.Background()
	now := time.Now().UTC()
	sub, err := r.CreateSubscription(ctx, notification.Subscription{
		Name: "test", Status: "active", Channel: "webhook", WebhookURL: "https://receiver.example/hook",
		EncryptedWebhookSecret: []byte("encrypted"), NotifyOnNew: true, MaximumEventAge: 2 * time.Hour,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.ApplyBatch(ctx, []earthquake.Event{testEvent("abandoned", now, 5)}, "realtime", true, now); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Pool.Exec(ctx, `UPDATE notification_deliveries SET status='processing',locked_at=$1,locked_by='dead-worker' WHERE subscription_id=$2`, now.Add(-10*time.Minute), sub.ID); err != nil {
		t.Fatal(err)
	}
	jobs, err := r.Claim(ctx, "recovery", 10, 5*time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 {
		t.Fatalf("jobs=%d", len(jobs))
	}
}

func floatPtr(v float64) *float64 { return &v }
