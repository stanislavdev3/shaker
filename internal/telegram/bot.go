package telegram

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/example/earthquake-service/internal/clock"
	domainnotification "github.com/example/earthquake-service/internal/domain/notification"
)

const (
	registrationRadiusKM = 1000.0
	stateProvider        = "telegram"
	stateUpdatesOffset   = "updates_offset"
)

type BotStore interface {
	State(context.Context, string, string) (string, bool, error)
	SetState(context.Context, string, string, string, time.Time) error
	AcquireTelegramPoller(context.Context) (func(), bool, error)
	UpsertTelegramLocation(context.Context, int64, float64, float64, float64, time.Time) (domainnotification.Subscription, error)
	ActivateTelegramSubscription(context.Context, int64, float64, time.Time) (domainnotification.Subscription, error)
	GetTelegramSubscription(context.Context, int64) (domainnotification.Subscription, error)
	DisableTelegramSubscription(context.Context, int64, time.Time) error
}

type BotAPI interface {
	GetUpdates(context.Context, int64) ([]Update, error)
	SendMessage(context.Context, int64, string) error
	SendMessageRemovingKeyboard(context.Context, int64, string) error
	RequestLocation(context.Context, int64, string) error
	AnswerCallbackQuery(context.Context, string, string) error
	SendLocation(context.Context, int64, int64, float64, float64) error
}

type Bot struct {
	store BotStore
	api   BotAPI
	clock clock.Clock
	log   *slog.Logger
}

func NewBot(store BotStore, api BotAPI, c clock.Clock, log *slog.Logger) *Bot {
	return &Bot{store: store, api: api, clock: c, log: log}
}

func (b *Bot) Run(ctx context.Context) {
	for ctx.Err() == nil {
		release, acquired, err := b.store.AcquireTelegramPoller(ctx)
		if err != nil {
			b.log.Error("acquire Telegram poller lock", "error", err)
		} else if acquired {
			b.poll(ctx)
			release()
			return
		}
		if !wait(ctx, 5*time.Second) {
			return
		}
	}
}

func (b *Bot) poll(ctx context.Context) {
	offset := int64(0)
	if value, ok, err := b.store.State(ctx, stateProvider, stateUpdatesOffset); err != nil {
		b.log.Error("load Telegram updates offset", "error", err)
	} else if ok {
		if parsed, parseErr := strconv.ParseInt(value, 10, 64); parseErr == nil && parsed >= 0 {
			offset = parsed
		}
	}
	for ctx.Err() == nil {
		updates, err := b.api.GetUpdates(ctx, offset)
		if err != nil {
			if ctx.Err() == nil {
				b.log.Error("poll Telegram updates", "error", err)
				wait(ctx, 2*time.Second)
			}
			continue
		}
		for _, update := range updates {
			if update.ID < offset {
				continue
			}
			if err := b.handle(ctx, update); err != nil {
				b.log.Error("handle Telegram update", "update_id", update.ID, "error", err)
				break
			}
			offset = update.ID + 1
			if err := b.store.SetState(ctx, stateProvider, stateUpdatesOffset, strconv.FormatInt(offset, 10), b.clock.Now()); err != nil {
				b.log.Error("persist Telegram updates offset", "update_id", update.ID, "error", err)
				break
			}
		}
	}
}

func (b *Bot) handle(ctx context.Context, update Update) error {
	if update.CallbackQuery != nil {
		return b.handleCallbackQuery(ctx, *update.CallbackQuery)
	}
	if update.Message == nil || update.Message.Chat.ID == 0 {
		return nil
	}
	message := update.Message
	chatID := message.Chat.ID
	if message.Location != nil {
		if !validLocation(*message.Location) {
			return b.api.SendMessage(ctx, chatID, "The location is invalid. Please share it again.")
		}
		if _, err := b.store.UpsertTelegramLocation(ctx, chatID, message.Location.Latitude, message.Location.Longitude, registrationRadiusKM, b.clock.Now()); err != nil {
			return err
		}
		return b.api.SendMessageRemovingKeyboard(ctx, chatID, "Location saved. Send the minimum magnitude as a number, for example: 4.5")
	}

	text := strings.TrimSpace(message.Text)
	command := strings.ToLower(strings.SplitN(text, " ", 2)[0])
	if at := strings.IndexByte(command, '@'); at >= 0 {
		command = command[:at]
	}
	switch command {
	case "/start":
		subscription, err := b.store.GetTelegramSubscription(ctx, chatID)
		if errors.Is(err, domainnotification.ErrSubscriptionNotFound) {
			return b.api.RequestLocation(ctx, chatID, "Share your location to receive earthquake alerts within 1000 km.")
		}
		if err != nil {
			return err
		}
		if subscription.Status == "active" && subscription.MinimumMagnitude != nil {
			return b.api.SendMessage(ctx, chatID, fmt.Sprintf("Alerts are already active within 1000 km for magnitude %.1f and above. Use /location to change your location.", *subscription.MinimumMagnitude))
		}
		return b.api.SendMessage(ctx, chatID, "Your location is already saved. Send the minimum magnitude, or use /location to replace it.")
	case "/location":
		return b.api.RequestLocation(ctx, chatID, "Share your location to receive earthquake alerts within 1000 km.")
	case "/stop":
		err := b.store.DisableTelegramSubscription(ctx, chatID, b.clock.Now())
		if errors.Is(err, domainnotification.ErrSubscriptionNotFound) {
			return b.api.SendMessage(ctx, chatID, "There is no active alert subscription.")
		}
		if err != nil {
			return err
		}
		return b.api.SendMessage(ctx, chatID, "Earthquake alerts have been disabled. Send /start to enable them again.")
	case "/status":
		subscription, err := b.store.GetTelegramSubscription(ctx, chatID)
		if errors.Is(err, domainnotification.ErrSubscriptionNotFound) {
			return b.api.RequestLocation(ctx, chatID, "No location is configured. Share your location to get started.")
		}
		if err != nil {
			return err
		}
		if subscription.Status != "active" || subscription.MinimumMagnitude == nil {
			return b.api.SendMessage(ctx, chatID, "Your location is saved, but alerts are not active. Send the minimum magnitude.")
		}
		return b.api.SendMessage(ctx, chatID, fmt.Sprintf("Alerts are active within 1000 km for magnitude %.1f and above.", *subscription.MinimumMagnitude))
	}

	magnitude, err := strconv.ParseFloat(text, 64)
	if err != nil || math.IsNaN(magnitude) || math.IsInf(magnitude, 0) || magnitude < 0 || magnitude > 10 {
		return b.api.SendMessage(ctx, chatID, "Send a minimum magnitude between 0 and 10, or use /start to share a location.")
	}
	if _, err := b.store.ActivateTelegramSubscription(ctx, chatID, magnitude, b.clock.Now()); errors.Is(err, domainnotification.ErrSubscriptionNotFound) {
		return b.api.RequestLocation(ctx, chatID, "Share your location before setting the minimum magnitude.")
	} else if err != nil {
		return err
	}
	return b.api.SendMessage(ctx, chatID, fmt.Sprintf("Alerts enabled: magnitude %.1f and above within 1000 km. Use /status to check or /stop to disable.", magnitude))
}

func (b *Bot) handleCallbackQuery(ctx context.Context, query CallbackQuery) error {
	if query.Message == nil || query.Message.Chat.ID <= 0 || query.Message.ID <= 0 {
		return b.api.AnswerCallbackQuery(ctx, query.ID, "Location is available only in a private chat.")
	}
	parts := strings.Split(query.Data, ":")
	if len(parts) != 3 || parts[0] != "loc" {
		return b.api.AnswerCallbackQuery(ctx, query.ID, "Location is unavailable.")
	}
	latitude, latitudeErr := strconv.ParseFloat(parts[1], 64)
	longitude, longitudeErr := strconv.ParseFloat(parts[2], 64)
	if latitudeErr != nil || longitudeErr != nil || !validLocation(Location{Latitude: latitude, Longitude: longitude}) {
		return b.api.AnswerCallbackQuery(ctx, query.ID, "Location is unavailable.")
	}
	if err := b.api.AnswerCallbackQuery(ctx, query.ID, ""); err != nil {
		return err
	}
	return b.api.SendLocation(ctx, query.Message.Chat.ID, query.Message.ID, latitude, longitude)
}

func validLocation(location Location) bool {
	return !math.IsNaN(location.Latitude) && !math.IsNaN(location.Longitude) &&
		location.Latitude >= -90 && location.Latitude <= 90 && location.Longitude >= -180 && location.Longitude <= 180
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
