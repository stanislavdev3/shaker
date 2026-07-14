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
	stateProvider      = "telegram"
	stateUpdatesOffset = "updates_offset"
)

type BotStore interface {
	State(context.Context, string, string) (string, bool, error)
	SetState(context.Context, string, string, string, time.Time) error
	AcquireTelegramPoller(context.Context) (func(), bool, error)
	UpsertTelegramLocation(context.Context, int64, float64, float64, float64, time.Time) (domainnotification.Subscription, error)
	SetTelegramLanguage(context.Context, int64, string, time.Time) (domainnotification.Subscription, error)
	ActivateTelegramIntensity(context.Context, int64, float64, time.Time) (domainnotification.Subscription, error)
	GetTelegramSubscription(context.Context, int64) (domainnotification.Subscription, error)
	DisableTelegramSubscription(context.Context, int64, time.Time) error
}

type BotAPI interface {
	GetUpdates(context.Context, int64) ([]Update, error)
	SendMessage(context.Context, int64, string) error
	SendMessageRemovingKeyboard(context.Context, int64, string) error
	RequestLocation(context.Context, int64, string) error
	RequestChoice(context.Context, int64, string, []string) error
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
			return b.api.SendMessage(ctx, chatID, "Invalid location. Please share it again. / Некорректная локация. Отправьте её ещё раз.")
		}
		if _, err := b.store.UpsertTelegramLocation(ctx, chatID, message.Location.Latitude, message.Location.Longitude, 0, b.clock.Now()); err != nil {
			return err
		}
		return b.requestLanguage(ctx, chatID)
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
			return b.api.RequestLocation(ctx, chatID, "Share your location to receive local earthquake alerts. / Отправьте локацию для локальных оповещений.")
		}
		if err != nil {
			return err
		}
		if subscription.Status == "active" && subscription.MinimumIntensity != nil {
			return b.api.SendMessage(ctx, chatID, statusText(subscription))
		}
		if subscription.NotificationLanguage == nil {
			return b.requestLanguage(ctx, chatID)
		}
		if subscription.Status == "active" && subscription.MinimumMagnitude != nil {
			return b.api.SendMessage(ctx, chatID, legacyText(subscription))
		}
		return b.requestIntensity(ctx, chatID, languageOf(subscription))
	case "/location":
		language := b.subscriptionLanguage(ctx, chatID)
		return b.api.RequestLocation(ctx, chatID, localize(language,
			"Share your new location.", "Отправьте новую локацию."))
	case "/language":
		return b.requestLanguage(ctx, chatID)
	case "/intensity":
		subscription, err := b.store.GetTelegramSubscription(ctx, chatID)
		if errors.Is(err, domainnotification.ErrSubscriptionNotFound) {
			return b.api.RequestLocation(ctx, chatID, "Share your location first. / Сначала отправьте локацию.")
		}
		if err != nil {
			return err
		}
		return b.requestIntensity(ctx, chatID, languageOf(subscription))
	case "/stop":
		language := b.subscriptionLanguage(ctx, chatID)
		err := b.store.DisableTelegramSubscription(ctx, chatID, b.clock.Now())
		if errors.Is(err, domainnotification.ErrSubscriptionNotFound) {
			return b.api.SendMessage(ctx, chatID, localize(language,
				"There is no alert subscription.", "Подписка на оповещения не настроена."))
		}
		if err != nil {
			return err
		}
		return b.api.SendMessage(ctx, chatID, localize(language,
			"Earthquake alerts have been disabled. Send /start to enable them again.",
			"Оповещения отключены. Отправьте /start, чтобы включить их снова."))
	case "/status":
		subscription, err := b.store.GetTelegramSubscription(ctx, chatID)
		if errors.Is(err, domainnotification.ErrSubscriptionNotFound) {
			return b.api.RequestLocation(ctx, chatID, "No location is configured. / Локация не настроена.")
		}
		if err != nil {
			return err
		}
		if subscription.Status == "active" && subscription.MinimumIntensity != nil {
			return b.api.SendMessage(ctx, chatID, statusText(subscription))
		}
		if subscription.Status == "active" && subscription.MinimumMagnitude != nil {
			return b.api.SendMessage(ctx, chatID, legacyText(subscription))
		}
		return b.requestIntensity(ctx, chatID, languageOf(subscription))
	}

	if language, ok := parseLanguage(text); ok {
		subscription, err := b.store.SetTelegramLanguage(ctx, chatID, language, b.clock.Now())
		if errors.Is(err, domainnotification.ErrSubscriptionNotFound) {
			return b.api.RequestLocation(ctx, chatID, "Share your location first. / Сначала отправьте локацию.")
		}
		if err != nil {
			return err
		}
		if subscription.MinimumIntensity == nil && subscription.MinimumMagnitude == nil {
			return b.requestIntensity(ctx, chatID, language)
		}
		return b.api.SendMessageRemovingKeyboard(ctx, chatID, localize(language,
			"Notification language changed to English.", "Язык оповещений изменён на русский."))
	}
	intensity, ok := parseIntensity(text)
	if !ok {
		language := b.subscriptionLanguage(ctx, chatID)
		return b.api.SendMessage(ctx, chatID, localize(language,
			"Choose an intensity from II to VI, or use /start.",
			"Выберите интенсивность от II до VI или используйте /start."))
	}
	if _, err := b.store.ActivateTelegramIntensity(ctx, chatID, intensity, b.clock.Now()); errors.Is(err, domainnotification.ErrSubscriptionNotFound) {
		return b.api.RequestLocation(ctx, chatID, "Share your location and choose a language first. / Сначала отправьте локацию и выберите язык.")
	} else if err != nil {
		return err
	}
	language := b.subscriptionLanguage(ctx, chatID)
	return b.api.SendMessageRemovingKeyboard(ctx, chatID, localize(language,
		fmt.Sprintf("Alerts enabled for expected intensity %s and above. Use /status or /stop.", romanMMI(intensity)),
		fmt.Sprintf("Оповещения включены для ожидаемой интенсивности %s и выше. Команды: /status, /stop.", romanMMI(intensity))))
}

func (b *Bot) requestLanguage(ctx context.Context, chatID int64) error {
	return b.api.RequestChoice(ctx, chatID, "Choose notification language. / Выберите язык оповещений.", []string{"Русский", "English"})
}

func (b *Bot) requestIntensity(ctx context.Context, chatID int64, language string) error {
	return b.api.RequestChoice(ctx, chatID, localize(language,
		"Choose the minimum expected shaking intensity at your location.",
		"Выберите минимальную ожидаемую интенсивность толчков в вашей локации."),
		[]string{"II", "III", "IV", "V", "VI"})
}

func (b *Bot) subscriptionLanguage(ctx context.Context, chatID int64) string {
	subscription, err := b.store.GetTelegramSubscription(ctx, chatID)
	if err != nil {
		return "en"
	}
	return languageOf(subscription)
}

func languageOf(subscription domainnotification.Subscription) string {
	if subscription.NotificationLanguage != nil && *subscription.NotificationLanguage == "ru" {
		return "ru"
	}
	return "en"
}

func parseLanguage(text string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(text)) {
	case "ru", "rus", "русский", "рус":
		return "ru", true
	case "en", "eng", "english":
		return "en", true
	default:
		return "", false
	}
}

func parseIntensity(text string) (float64, bool) {
	values := map[string]float64{"II": 2, "III": 3, "IV": 4, "V": 5, "VI": 6}
	if value, ok := values[strings.ToUpper(strings.TrimSpace(text))]; ok {
		return value, true
	}
	value, err := strconv.ParseFloat(strings.TrimSpace(text), 64)
	return value, err == nil && !math.IsNaN(value) && !math.IsInf(value, 0) && value >= 2 && value <= 6
}

func statusText(subscription domainnotification.Subscription) string {
	language := languageOf(subscription)
	return localize(language,
		fmt.Sprintf("Alerts are active for expected intensity %s and above. Use /location, /language, /intensity, or /stop.", romanMMI(*subscription.MinimumIntensity)),
		fmt.Sprintf("Оповещения активны для ожидаемой интенсивности %s и выше. Команды: /location, /language, /intensity, /stop.", romanMMI(*subscription.MinimumIntensity)))
}

func legacyText(subscription domainnotification.Subscription) string {
	language := languageOf(subscription)
	return localize(language,
		fmt.Sprintf("Legacy alerts are active for magnitude %.1f and above within the previous radius. Use /intensity to switch to local shaking intensity.", *subscription.MinimumMagnitude),
		fmt.Sprintf("Старые оповещения активны для магнитуды %.1f и выше в прежнем радиусе. Используйте /intensity, чтобы перейти на интенсивность в вашей локации.", *subscription.MinimumMagnitude))
}

func localize(language, english, russian string) string {
	if language == "ru" {
		return russian
	}
	return english
}

func romanMMI(value float64) string {
	roman := []string{"I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X"}
	index := int(math.Round(value)) - 1
	if index < 0 || index >= len(roman) {
		return fmt.Sprintf("%.1f MMI", value)
	}
	return roman[index]
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
