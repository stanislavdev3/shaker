//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/example/earthquake-service/internal/administration"
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
	_, err = repo.Pool.Exec(context.Background(), `TRUNCATE admin_audit_log,admin_role_bindings,
		notification_intensity_evaluations,telegram_alert_messages,notification_deliveries,notification_subscriptions,
		provider_observations,earthquake_source_associations,earthquake_revisions,earthquake_source_records,earthquakes,
		ingestion_runs,provider_state CASCADE`)
	if err != nil {
		t.Fatal(err)
	}
	return repo
}

func TestAdministrationRolesAndAppendOnlyAudit(t *testing.T) {
	r := integrationRepository(t)
	ctx := context.Background()
	now := time.Now().UTC()
	if err := r.BootstrapOwners(ctx, []string{"admin@example.com"}, now); err != nil {
		t.Fatal(err)
	}
	role, err := r.RoleForEmail(ctx, "admin@example.com")
	if err != nil {
		t.Fatal(err)
	}
	if role != administration.Owner {
		t.Fatalf("role=%q", role)
	}
	if err := r.BootstrapOwners(ctx, []string{"admin@example.com"}, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := r.Pool.QueryRow(ctx, `SELECT count(*) FROM admin_role_bindings WHERE email=$1`, "admin@example.com").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("bindings=%d", count)
	}
	var auditID uuid.UUID
	err = r.Pool.QueryRow(ctx, `INSERT INTO admin_audit_log(id,actor_subject,actor_email,actor_role,action,
		resource_type,resource_id,created_at) VALUES(gen_random_uuid(),$1,$2,$3,$4,$5,$6,$7) RETURNING id`,
		"subject", "admin@example.com", "owner", "test", "subscription", "resource-1", now).Scan(&auditID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Pool.Exec(ctx, `UPDATE admin_audit_log SET action=$2 WHERE id=$1`, auditID, "changed"); err == nil {
		t.Fatal("expected append-only audit update rejection")
	}
	if _, err := r.Pool.Exec(ctx, `DELETE FROM admin_audit_log WHERE id=$1`, auditID); err == nil {
		t.Fatal("expected append-only audit delete rejection")
	}
}

func TestAdministrationReadModels(t *testing.T) {
	r := integrationRepository(t)
	ctx := context.Background()
	now := time.Now().UTC()
	subscription, err := r.CreateSubscription(ctx, notification.Subscription{
		Name: "admin-read-model", Status: "active", Channel: "webhook",
		WebhookURL: "https://receiver.example/hook", EncryptedWebhookSecret: []byte("must-not-be-read"),
		NotifyOnNew: true, MaximumEventAge: 2 * time.Hour,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.ApplyBatch(ctx, []earthquake.Event{testEvent("admin-read-model", now, 5.2)}, "realtime", true, now); err != nil {
		t.Fatal(err)
	}
	incidents, err := r.ListAdminIncidents(ctx, administration.IncidentFilter{Limit: 50})
	if err != nil {
		t.Fatal(err)
	}
	if len(incidents) != 1 {
		t.Fatalf("incidents=%d", len(incidents))
	}
	detail, err := r.AdminIncident(ctx, incidents[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(detail.Sources) != 1 || len(detail.Observations) != 1 || len(detail.Associations) != 1 {
		t.Fatalf("unexpected incident detail: %+v", detail)
	}
	subscriptions, err := r.ListAdminSubscriptions(ctx, administration.PageFilter{Limit: 50})
	if err != nil {
		t.Fatal(err)
	}
	if len(subscriptions) != 1 || subscriptions[0].ID != subscription.ID {
		t.Fatalf("subscriptions=%+v", subscriptions)
	}
	if len(subscriptions[0].EncryptedWebhookSecret) != 0 {
		t.Fatal("admin subscription query returned encrypted webhook secret")
	}
	notifications, err := r.ListAdminNotifications(ctx, administration.NotificationFilter{
		PageFilter: administration.PageFilter{Limit: 50}, DeliveryClass: "webhook",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(notifications) != 1 || notifications[0].SubscriptionID != subscription.ID ||
		notifications[0].DeliveryClass != "webhook" || len(notifications[0].Payload) == 0 {
		t.Fatalf("notifications=%+v", notifications)
	}
	if _, err := r.AdminNotification(ctx, notifications[0].ID); err != nil {
		t.Fatal(err)
	}
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

func TestStaleCatalogueObservationConfirmsWithoutReplacingNewerParameters(t *testing.T) {
	r := integrationRepository(t)
	ctx := context.Background()
	now := time.Now().UTC()
	preliminary := testEvent("emsc-confirmation", now, 5.1)
	preliminary.Provider = "emsc"
	preliminary.ObservationChannel = "emsc_websocket"
	preliminary.SolutionClass = earthquake.PreliminarySolution
	if _, err := r.ApplyBatch(ctx, []earthquake.Event{preliminary}, "backfill", true, now); err != nil {
		t.Fatal(err)
	}

	confirmed := testEvent("emsc-confirmation", now.Add(-time.Minute), 4.7)
	confirmed.Provider = "emsc"
	confirmed.ObservationChannel = "emsc_fdsn"
	confirmed.SolutionClass = earthquake.ConfirmedSolution
	stats, err := r.ApplyBatch(ctx, []earthquake.Event{confirmed}, "backfill", true, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if stats.Updated != 1 {
		t.Fatalf("stats=%+v", stats)
	}
	var magnitude float64
	var lifecycle string
	var version int64
	if err := r.Pool.QueryRow(ctx, `SELECT magnitude,lifecycle,version FROM earthquakes WHERE preferred_external_id=$1`,
		preliminary.ExternalID).Scan(&magnitude, &lifecycle, &version); err != nil {
		t.Fatal(err)
	}
	if magnitude != 5.1 || lifecycle != "confirmed" || version != 2 {
		t.Fatalf("magnitude=%v lifecycle=%q version=%d", magnitude, lifecycle, version)
	}
	var observations int
	if err := r.Pool.QueryRow(ctx, `SELECT count(*) FROM provider_observations`).Scan(&observations); err != nil {
		t.Fatal(err)
	}
	if observations != 2 {
		t.Fatalf("observations=%d", observations)
	}
}

func TestNewerPreliminaryObservationDoesNotReplaceConfirmedCatalogueParameters(t *testing.T) {
	r := integrationRepository(t)
	ctx := context.Background()
	now := time.Now().UTC()
	confirmed := testEvent("emsc-channel-priority", now, 5.1)
	confirmed.Provider = "emsc"
	confirmed.ObservationChannel = "emsc_fdsn"
	confirmed.SolutionClass = earthquake.ConfirmedSolution
	if _, err := r.ApplyBatch(ctx, []earthquake.Event{confirmed}, "backfill", true, now); err != nil {
		t.Fatal(err)
	}

	preliminary := testEvent("emsc-channel-priority", now.Add(time.Minute), 6.8)
	preliminary.Provider = "emsc"
	preliminary.ObservationChannel = "emsc_standing_order"
	preliminary.SolutionClass = earthquake.PreliminarySolution
	stats, err := r.ApplyBatch(ctx, []earthquake.Event{preliminary}, "realtime", true, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if stats.Unchanged != 1 {
		t.Fatalf("stats=%+v", stats)
	}
	var magnitude float64
	var lifecycle, channel string
	if err := r.Pool.QueryRow(ctx, `SELECT e.magnitude,e.lifecycle,s.latest_observation_channel
		FROM earthquakes e JOIN earthquake_source_records s ON s.earthquake_id=e.id
		WHERE e.preferred_external_id=$1`, confirmed.ExternalID).Scan(&magnitude, &lifecycle, &channel); err != nil {
		t.Fatal(err)
	}
	if magnitude != 5.1 || lifecycle != "confirmed" || channel != "emsc_fdsn" {
		t.Fatalf("magnitude=%v lifecycle=%q channel=%q", magnitude, lifecycle, channel)
	}
	var observations int
	if err := r.Pool.QueryRow(ctx, `SELECT count(*) FROM provider_observations`).Scan(&observations); err != nil {
		t.Fatal(err)
	}
	if observations != 2 {
		t.Fatalf("observations=%d", observations)
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

func TestTelegramSubscriptionCreatesDistanceAwareDelivery(t *testing.T) {
	r := integrationRepository(t)
	ctx := context.Background()
	now := time.Now().UTC()
	const chatID int64 = 42

	if _, err := r.UpsertTelegramLocation(ctx, chatID, 40.1, 74.2, 1000, now); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ActivateTelegramSubscription(ctx, chatID, 4.5, now); err != nil {
		t.Fatal(err)
	}
	event := testEvent("telegram-notify", now, 5.2)
	sourceURL := "https://earthquake.usgs.gov/earthquakes/eventpage/telegram-notify"
	event.SourceURL = &sourceURL
	if _, err := r.ApplyBatch(ctx, []earthquake.Event{event}, "realtime", true, now); err != nil {
		t.Fatal(err)
	}
	jobs, err := r.ClaimTelegramAlertMessages(ctx, "telegram-worker", 10, 5*time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 {
		t.Fatalf("jobs=%d", len(jobs))
	}
	if jobs[0].TelegramChatID != chatID {
		t.Fatalf("unexpected delivery: %+v", jobs[0])
	}
	var payload struct {
		Sources    map[string]string `json:"sources"`
		Earthquake struct {
			DistanceKM *float64 `json:"distance_km"`
		} `json:"earthquake"`
	}
	if err := json.Unmarshal(jobs[0].DesiredPayload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Earthquake.DistanceKM == nil || *payload.Earthquake.DistanceKM > 0.01 {
		t.Fatalf("distance=%v", payload.Earthquake.DistanceKM)
	}
	if payload.Sources["usgs"] != sourceURL {
		t.Fatalf("sources=%v", payload.Sources)
	}
}

func TestTelegramIntensitySubscriptionAuditsDecisionAndLocalizesPayload(t *testing.T) {
	r := integrationRepository(t)
	ctx := context.Background()
	now := time.Now().UTC()
	const notifiedChatID, quietChatID int64 = 43, 44

	for _, configured := range []struct {
		chatID    int64
		threshold float64
		longitude float64
	}{{notifiedChatID, 6, 74.2}, {quietChatID, 6, 75.2}} {
		if _, err := r.UpsertTelegramLocation(ctx, configured.chatID, 40.1, configured.longitude, 0, now); err != nil {
			t.Fatal(err)
		}
		if _, err := r.SetTelegramLanguage(ctx, configured.chatID, "ru", now); err != nil {
			t.Fatal(err)
		}
		if _, err := r.ActivateTelegramIntensity(ctx, configured.chatID, configured.threshold, now); err != nil {
			t.Fatal(err)
		}
	}

	event := testEvent("telegram-intensity", now, 5.0)
	depth := 10.0
	event.DepthKM = &depth
	if _, err := r.ApplyBatch(ctx, []earthquake.Event{event}, "realtime", true, now); err != nil {
		t.Fatal(err)
	}
	alerts, err := r.ClaimTelegramAlertMessages(ctx, "telegram-worker", 10, 5*time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(alerts) != 1 || alerts[0].TelegramChatID != notifiedChatID {
		t.Fatalf("alerts=%+v", alerts)
	}
	var payload struct {
		Language *string `json:"language"`
		Shaking  *struct {
			MeanMMI  float64 `json:"mean_mmi"`
			UpperMMI float64 `json:"upper_mmi"`
		} `json:"shaking"`
	}
	if err := json.Unmarshal(alerts[0].DesiredPayload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Language == nil || *payload.Language != "ru" || payload.Shaking == nil || payload.Shaking.MeanMMI <= 0 || payload.Shaking.UpperMMI < 6 {
		t.Fatalf("payload=%+v", payload)
	}
	var notifyDecisions, belowDecisions int
	if err := r.Pool.QueryRow(ctx, `SELECT
		count(*) FILTER (WHERE decision='notify'),count(*) FILTER (WHERE decision='below_threshold')
		FROM notification_intensity_evaluations`).Scan(&notifyDecisions, &belowDecisions); err != nil {
		t.Fatal(err)
	}
	if notifyDecisions != 1 || belowDecisions != 1 {
		t.Fatalf("notify=%d below=%d", notifyDecisions, belowDecisions)
	}
}

func TestGlobalTelegramChannelReceivesUnfilteredWorldwideIncident(t *testing.T) {
	r := integrationRepository(t)
	ctx := context.Background()
	now := time.Now().UTC()
	const chatID int64 = -10042

	subscription, err := r.UpsertGlobalTelegramChannel(ctx, chatID, "@eqmonitor", now)
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := r.UpsertGlobalTelegramChannel(ctx, chatID, "@eqmonitor", now.Add(time.Second))
	if err != nil || repeated.ID != subscription.ID {
		t.Fatalf("repeated=%+v err=%v", repeated, err)
	}
	event := testEvent("global-channel", now, 0.4)
	event.Latitude = -55
	event.Longitude = -130
	eventType := "earthquake"
	event.EventType = &eventType
	event.Provider = "emsc"
	event.ObservationChannel = "emsc_websocket"
	event.SolutionClass = earthquake.PreliminarySolution
	if _, err := r.ApplyBatch(ctx, []earthquake.Event{event}, "realtime", true, now); err != nil {
		t.Fatal(err)
	}
	alerts, err := r.ClaimTelegramAlertMessages(ctx, "global-channel-worker", 10, 5*time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(alerts) != 1 || alerts[0].SubscriptionID != subscription.ID || alerts[0].TelegramChatID != chatID {
		t.Fatalf("alerts=%+v", alerts)
	}
	var payload struct {
		Lifecycle  string `json:"lifecycle"`
		Earthquake struct {
			Magnitude  *float64 `json:"magnitude"`
			DistanceKM *float64 `json:"distance_km"`
		} `json:"earthquake"`
	}
	if err := json.Unmarshal(alerts[0].DesiredPayload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Lifecycle != "preliminary" || payload.Earthquake.Magnitude == nil || *payload.Earthquake.Magnitude != 0.4 || payload.Earthquake.DistanceKM != nil {
		t.Fatalf("payload=%+v", payload)
	}
	if err := r.CompleteTelegramAlertMessage(ctx, alerts[0].ID, alerts[0].DesiredEarthquakeVersion, 77, 1, now); err != nil {
		t.Fatal(err)
	}
	confirmed := testEvent("global-channel", now.Add(-time.Minute), 0.3)
	confirmed.Provider = "emsc"
	confirmed.EventType = &eventType
	confirmed.ObservationChannel = "emsc_fdsn"
	confirmed.SolutionClass = earthquake.ConfirmedSolution
	if _, err := r.ApplyBatch(ctx, []earthquake.Event{confirmed}, "recovery", true, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	updated, err := r.ClaimTelegramAlertMessages(ctx, "global-channel-worker", 10, 5*time.Minute, now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(updated) != 1 || updated[0].TelegramMessageID == nil || *updated[0].TelegramMessageID != 77 || updated[0].Lifecycle != "confirmed" {
		t.Fatalf("updated=%+v", updated)
	}
}

func TestIncidentObservationAndTelegramProjection(t *testing.T) {
	r := integrationRepository(t)
	ctx := context.Background()
	now := time.Now().UTC()
	const chatID int64 = 84

	subscription, err := r.UpsertTelegramLocation(ctx, chatID, 40.1, 74.2, 1000, now)
	if err != nil {
		t.Fatal(err)
	}
	event := testEvent("incident-projection", now, 5.1)
	event.ObservationChannel = "usgs_realtime"
	event.SolutionClass = earthquake.ReviewedSolution
	if _, err := r.ApplyBatch(ctx, []earthquake.Event{event}, "backfill", true, now); err != nil {
		t.Fatal(err)
	}

	var earthquakeID uuid.UUID
	var lifecycle string
	if err := r.Pool.QueryRow(ctx, `SELECT id,lifecycle FROM earthquakes WHERE preferred_external_id=$1`, event.ExternalID).
		Scan(&earthquakeID, &lifecycle); err != nil {
		t.Fatal(err)
	}
	if lifecycle != "reviewed" {
		t.Fatalf("lifecycle=%q", lifecycle)
	}
	var observations, associations int
	if err := r.Pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM provider_observations),
		(SELECT count(*) FROM earthquake_source_associations WHERE active)`).Scan(&observations, &associations); err != nil {
		t.Fatal(err)
	}
	if observations != 1 || associations != 1 {
		t.Fatalf("observations=%d associations=%d", observations, associations)
	}

	alert, err := r.UpsertTelegramAlertMessage(ctx, subscription.ID, earthquakeID, 1, lifecycle, json.RawMessage(`{"text":"reviewed"}`), now)
	if err != nil {
		t.Fatal(err)
	}
	if alert.Status != "pending_send" || alert.TelegramChatID != chatID {
		t.Fatalf("alert=%+v", alert)
	}
	claimed, err := r.ClaimTelegramAlertMessages(ctx, "projection-worker", 10, 5*time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed=%d", len(claimed))
	}
	if err := r.CompleteTelegramAlertMessage(ctx, alert.ID, 1, 123, 1, now); err != nil {
		t.Fatal(err)
	}
	updated, err := r.UpsertTelegramAlertMessage(ctx, subscription.ID, earthquakeID, 2, "confirmed", json.RawMessage(`{"text":"confirmed"}`), now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if updated.Status != "pending_edit" || updated.TelegramMessageID == nil || *updated.TelegramMessageID != 123 {
		t.Fatalf("updated=%+v", updated)
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
