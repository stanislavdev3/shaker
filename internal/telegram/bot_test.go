package telegram

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	domainnotification "github.com/example/earthquake-service/internal/domain/notification"
)

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

type fakeStore struct {
	subscription domainnotification.Subscription
}

func (s *fakeStore) State(context.Context, string, string) (string, bool, error) {
	return "", false, nil
}
func (s *fakeStore) SetState(context.Context, string, string, string, time.Time) error { return nil }
func (s *fakeStore) AcquireTelegramPoller(context.Context) (func(), bool, error) {
	return func() {}, true, nil
}
func (s *fakeStore) UpsertTelegramLocation(_ context.Context, chatID int64, latitude, longitude, radius float64, now time.Time) (domainnotification.Subscription, error) {
	s.subscription = domainnotification.Subscription{
		Name: "telegram:42", Status: "paused", Channel: "telegram", TelegramChatID: &chatID,
		CenterLatitude: &latitude, CenterLongitude: &longitude, RadiusKM: &radius, UpdatedAt: now,
	}
	return s.subscription, nil
}
func (s *fakeStore) ActivateTelegramSubscription(_ context.Context, chatID int64, magnitude float64, now time.Time) (domainnotification.Subscription, error) {
	if s.subscription.TelegramChatID == nil || *s.subscription.TelegramChatID != chatID {
		return domainnotification.Subscription{}, domainnotification.ErrSubscriptionNotFound
	}
	s.subscription.Status = "active"
	s.subscription.MinimumMagnitude = &magnitude
	s.subscription.UpdatedAt = now
	return s.subscription, nil
}
func (s *fakeStore) GetTelegramSubscription(context.Context, int64) (domainnotification.Subscription, error) {
	if s.subscription.TelegramChatID == nil {
		return domainnotification.Subscription{}, domainnotification.ErrSubscriptionNotFound
	}
	return s.subscription, nil
}
func (s *fakeStore) DisableTelegramSubscription(context.Context, int64, time.Time) error {
	if s.subscription.TelegramChatID == nil {
		return domainnotification.ErrSubscriptionNotFound
	}
	s.subscription.Status = "disabled"
	return nil
}

type fakeAPI struct {
	messages                   []string
	answeredCallbackID         string
	locationChatID, locationID int64
	latitude, longitude        float64
}

func (a *fakeAPI) GetUpdates(context.Context, int64) ([]Update, error) {
	return nil, errors.New("unused")
}
func (a *fakeAPI) SendMessage(_ context.Context, _ int64, text string) error {
	a.messages = append(a.messages, text)
	return nil
}
func (a *fakeAPI) SendMessageRemovingKeyboard(_ context.Context, _ int64, text string) error {
	a.messages = append(a.messages, "remove:"+text)
	return nil
}
func (a *fakeAPI) RequestLocation(_ context.Context, _ int64, text string) error {
	a.messages = append(a.messages, text)
	return nil
}
func (a *fakeAPI) AnswerCallbackQuery(_ context.Context, callbackID, _ string) error {
	a.answeredCallbackID = callbackID
	return nil
}
func (a *fakeAPI) SendLocation(_ context.Context, chatID, replyID int64, latitude, longitude float64) error {
	a.locationChatID, a.locationID = chatID, replyID
	a.latitude, a.longitude = latitude, longitude
	return nil
}

func TestRegistrationFlow(t *testing.T) {
	store := &fakeStore{}
	api := &fakeAPI{}
	bot := NewBot(store, api, fixedClock{now: time.Date(2026, 7, 13, 10, 0, 0, 0, time.UTC)}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx := context.Background()
	chat := Chat{ID: 42}

	if err := bot.handle(ctx, Update{ID: 1, Message: &Message{Chat: chat, Text: "/start"}}); err != nil {
		t.Fatal(err)
	}
	if err := bot.handle(ctx, Update{ID: 2, Message: &Message{Chat: chat, Location: &Location{Latitude: 40.1, Longitude: 74.2}}}); err != nil {
		t.Fatal(err)
	}
	if err := bot.handle(ctx, Update{ID: 3, Message: &Message{Chat: chat, Text: "4.5"}}); err != nil {
		t.Fatal(err)
	}
	if store.subscription.Status != "active" || store.subscription.MinimumMagnitude == nil || *store.subscription.MinimumMagnitude != 4.5 {
		t.Fatalf("unexpected subscription: %+v", store.subscription)
	}
	if store.subscription.RadiusKM == nil || *store.subscription.RadiusKM != 1000 {
		t.Fatalf("radius = %v", store.subscription.RadiusKM)
	}
	if len(api.messages) != 3 {
		t.Fatalf("messages = %v", api.messages)
	}
	if len(api.messages[1]) < len("remove:") || api.messages[1][:len("remove:")] != "remove:" {
		t.Fatalf("location reply did not remove keyboard: %v", api.messages)
	}
}

func TestLocationCallback(t *testing.T) {
	api := &fakeAPI{}
	bot := NewBot(&fakeStore{}, api, fixedClock{now: time.Now()}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	update := Update{ID: 1, CallbackQuery: &CallbackQuery{
		ID: "callback-1", Data: "loc:42.800000:74.600000",
		Message: &Message{ID: 99, Chat: Chat{ID: 42, Type: "private"}},
	}}
	if err := bot.handle(context.Background(), update); err != nil {
		t.Fatal(err)
	}
	if api.answeredCallbackID != "callback-1" || api.locationChatID != 42 || api.locationID != 99 || api.latitude != 42.8 || api.longitude != 74.6 {
		t.Fatalf("unexpected callback result: %+v", api)
	}
}

func TestStartDoesNotRequestLocationWhenAlreadyConfigured(t *testing.T) {
	chatID := int64(42)
	magnitude := 4.5
	store := &fakeStore{subscription: domainnotification.Subscription{
		Status: "active", Channel: "telegram", TelegramChatID: &chatID, MinimumMagnitude: &magnitude,
	}}
	api := &fakeAPI{}
	bot := NewBot(store, api, fixedClock{now: time.Now()}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	if err := bot.handle(context.Background(), Update{ID: 1, Message: &Message{Chat: Chat{ID: chatID}, Text: "/start"}}); err != nil {
		t.Fatal(err)
	}
	if len(api.messages) != 1 || api.messages[0] == "Share your location to receive earthquake alerts within 1000 km." {
		t.Fatalf("unexpected messages: %v", api.messages)
	}
}
