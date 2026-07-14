package telegram

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
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
		CenterLatitude: &latitude, CenterLongitude: &longitude, UpdatedAt: now,
	}
	if radius > 0 {
		s.subscription.RadiusKM = &radius
	}
	return s.subscription, nil
}
func (s *fakeStore) SetTelegramLanguage(_ context.Context, chatID int64, language string, now time.Time) (domainnotification.Subscription, error) {
	if s.subscription.TelegramChatID == nil || *s.subscription.TelegramChatID != chatID {
		return domainnotification.Subscription{}, domainnotification.ErrSubscriptionNotFound
	}
	s.subscription.NotificationLanguage = &language
	s.subscription.UpdatedAt = now
	return s.subscription, nil
}
func (s *fakeStore) ActivateTelegramIntensity(_ context.Context, chatID int64, intensity float64, now time.Time) (domainnotification.Subscription, error) {
	if s.subscription.TelegramChatID == nil || *s.subscription.TelegramChatID != chatID || s.subscription.NotificationLanguage == nil {
		return domainnotification.Subscription{}, domainnotification.ErrSubscriptionNotFound
	}
	s.subscription.Status = "active"
	s.subscription.MinimumIntensity = &intensity
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
	choices                    []string
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
func (a *fakeAPI) RequestChoice(_ context.Context, _ int64, text string, choices []string) error {
	a.messages = append(a.messages, "choice:"+text)
	a.choices = append([]string(nil), choices...)
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
	if err := bot.handle(ctx, Update{ID: 3, Message: &Message{Chat: chat, Text: "Русский"}}); err != nil {
		t.Fatal(err)
	}
	if err := bot.handle(ctx, Update{ID: 4, Message: &Message{Chat: chat, Text: "IV"}}); err != nil {
		t.Fatal(err)
	}
	if store.subscription.Status != "active" || store.subscription.MinimumIntensity == nil || *store.subscription.MinimumIntensity != 4 {
		t.Fatalf("unexpected subscription: %+v", store.subscription)
	}
	if store.subscription.NotificationLanguage == nil || *store.subscription.NotificationLanguage != "ru" || store.subscription.RadiusKM != nil {
		t.Fatalf("language/radius = %v/%v", store.subscription.NotificationLanguage, store.subscription.RadiusKM)
	}
	if len(api.messages) != 4 {
		t.Fatalf("messages = %v", api.messages)
	}
	if !strings.HasPrefix(api.messages[1], "choice:") || !strings.HasPrefix(api.messages[3], "remove:") {
		t.Fatalf("unexpected keyboards: %v", api.messages)
	}
	if got := strings.Join(api.choices, ","); got != "II,III,IV,V,VI" {
		t.Fatalf("intensity choices=%q", got)
	}
}

func TestParseIntensityRejectsThresholdAboveStrong(t *testing.T) {
	for _, value := range []string{"VII", "7", "X"} {
		if _, ok := parseIntensity(value); ok {
			t.Fatalf("accepted intensity threshold %q", value)
		}
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
	language := "en"
	store := &fakeStore{subscription: domainnotification.Subscription{
		Status: "active", Channel: "telegram", TelegramChatID: &chatID, MinimumMagnitude: &magnitude,
		NotificationLanguage: &language,
	}}
	api := &fakeAPI{}
	bot := NewBot(store, api, fixedClock{now: time.Now()}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	if err := bot.handle(context.Background(), Update{ID: 1, Message: &Message{Chat: Chat{ID: chatID}, Text: "/start"}}); err != nil {
		t.Fatal(err)
	}
	if len(api.messages) != 1 || strings.Contains(api.messages[0], "Share your location") {
		t.Fatalf("unexpected messages: %v", api.messages)
	}
}
