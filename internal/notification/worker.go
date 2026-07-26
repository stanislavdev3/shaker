package notification

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/example/earthquake-service/internal/clock"
	"github.com/example/earthquake-service/internal/observability"
	"github.com/example/earthquake-service/internal/repository/postgres"
)

type Worker struct {
	repo                                   *postgres.Repository
	cipher                                 *Cipher
	clock                                  clock.Clock
	log                                    *slog.Logger
	id, userAgent                          string
	batch, maxAttempts                     int
	lockTimeout, pollInterval, httpTimeout time.Duration
	maxResponse                            int64
	allowPrivate                           bool
	telegram                               TelegramSender
	metrics                                *observability.Metrics
}

type TelegramSender interface {
	SendAlertMessage(context.Context, int64, string, *float64, *float64, string) (int64, error)
	EditAlertMessage(context.Context, int64, int64, string, *float64, *float64, string) error
}

func NewWorker(repo *postgres.Repository, cipher *Cipher, c clock.Clock, log *slog.Logger, id, userAgent string,
	batch, maxAttempts int, lockTimeout, poll, httpTimeout time.Duration, maxResponse int64, allowPrivate bool,
	telegram TelegramSender, metrics ...*observability.Metrics,
) *Worker {
	worker := &Worker{repo: repo, cipher: cipher, clock: c, log: log, id: id, userAgent: userAgent, batch: batch, maxAttempts: maxAttempts,
		lockTimeout: lockTimeout, pollInterval: poll, httpTimeout: httpTimeout, maxResponse: maxResponse, allowPrivate: allowPrivate, telegram: telegram}
	if len(metrics) > 0 {
		worker.metrics = metrics[0]
	}
	return worker
}
func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()
	for {
		w.process(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
func (w *Worker) process(ctx context.Context) {
	jobs, err := w.repo.Claim(ctx, w.id, w.batch, w.lockTimeout, w.clock.Now())
	if err != nil {
		w.log.Error("claim notification deliveries", "error", err)
		return
	}
	for _, job := range jobs {
		w.deliver(ctx, job)
	}
	w.processTelegramAlerts(ctx)
}

func (w *Worker) processTelegramAlerts(ctx context.Context) {
	if w.telegram == nil {
		return
	}
	alerts, err := w.repo.ClaimTelegramAlertMessages(ctx, w.id, w.batch, w.lockTimeout, w.clock.Now())
	if err != nil {
		w.log.Error("claim Telegram alert messages", "error", err)
		return
	}
	for _, alert := range alerts {
		w.deliverTelegramAlert(ctx, alert)
	}
}

func (w *Worker) deliverTelegramAlert(ctx context.Context, alert postgres.TelegramAlertMessage) {
	operation := "edit"
	if alert.TelegramMessageID == nil {
		operation = "send"
	}
	attempt := alert.AttemptCount + 1
	now := w.clock.Now()
	message, err := telegramMessage(alert.DesiredPayload)
	messageID := int64(0)
	if err == nil {
		if alert.TelegramMessageID == nil {
			messageID, err = w.telegram.SendAlertMessage(ctx, alert.TelegramChatID, message.text, message.latitude, message.longitude, message.locationButton)
			if err != nil && messageID > 0 {
				w.log.Warn("send Telegram alert location", "telegram_alert_id", alert.ID, "error", err)
				err = nil
			}
		} else {
			messageID = *alert.TelegramMessageID
			err = w.telegram.EditAlertMessage(ctx, alert.TelegramChatID, messageID, message.text, message.latitude, message.longitude, message.locationButton)
		}
	}
	if err == nil {
		if saveErr := w.repo.CompleteTelegramAlertMessage(context.WithoutCancel(ctx), alert.ID,
			alert.DesiredEarthquakeVersion, messageID, attempt, w.clock.Now()); saveErr != nil {
			w.log.Error("persist Telegram alert result", "telegram_alert_id", alert.ID, "error", saveErr)
			if w.metrics != nil {
				w.metrics.ObserveTelegramAlert(operation, "persist_error")
			}
			return
		}
		if w.metrics != nil {
			w.metrics.ObserveTelegramAlert(operation, "sent")
		}
		return
	}
	next := now.Add(retryDelay(attempt))
	if retryAfter := telegramRetryAfter(err); retryAfter > 0 {
		next = now.Add(retryAfter)
	}
	dead := attempt >= w.maxAttempts
	if w.metrics != nil {
		result := "retry"
		if dead {
			result = "dead"
		}
		w.metrics.ObserveTelegramAlert(operation, result)
	}
	if saveErr := w.repo.FailTelegramAlertMessage(context.WithoutCancel(ctx), alert.ID, attempt, next, dead, err.Error(), w.clock.Now()); saveErr != nil {
		w.log.Error("persist Telegram alert failure", "telegram_alert_id", alert.ID, "error", saveErr)
	}
}
func (w *Worker) deliver(ctx context.Context, d postgres.Delivery) {
	started := w.clock.Now()
	attempt := d.AttemptCount + 1
	now := w.clock.Now()
	statusCode, serverRetryDelay, err := w.send(ctx, d, now)
	status := "sent"
	next := now
	var errorText *string
	if err != nil {
		v := err.Error()
		if len(v) > 1000 {
			v = v[:1000]
		}
		errorText = &v
		status = "retry"
		next = now.Add(retryDelay(attempt))
		if serverRetryDelay > 0 {
			next = now.Add(serverRetryDelay)
		}
		if attempt >= w.maxAttempts {
			status = "dead"
		}
	}
	if saveErr := w.repo.CompleteDelivery(context.WithoutCancel(ctx), d.ID, status, attempt, next, statusCode, errorText, w.clock.Now()); saveErr != nil {
		w.log.Error("persist notification result", "delivery_id", d.ID, "error", saveErr)
		status = "persist_error"
	}
	if w.metrics != nil {
		w.metrics.ObserveNotificationDelivery(d.Channel, status, w.clock.Now().Sub(started))
	}
}

func (w *Worker) send(ctx context.Context, d postgres.Delivery, now time.Time) (*int, time.Duration, error) {
	if d.Channel == "telegram" {
		if w.telegram == nil || d.TelegramChatID == nil {
			return nil, 0, errors.New("telegram delivery is not configured")
		}
		message, err := telegramMessage(d.Payload)
		if err != nil {
			return nil, 0, err
		}
		messageID, sendErr := w.telegram.SendAlertMessage(ctx, *d.TelegramChatID, message.text, message.latitude, message.longitude, message.locationButton)
		if sendErr != nil && messageID > 0 {
			w.log.Warn("send Telegram delivery location", "delivery_id", d.ID, "error", sendErr)
			return nil, 0, nil
		}
		err = sendErr
		return nil, telegramRetryAfter(err), err
	}
	if d.Channel != "webhook" {
		return nil, 0, fmt.Errorf("unsupported notification channel %q", d.Channel)
	}
	secret, err := w.cipher.Decrypt(d.EncryptedSecret)
	if err != nil {
		return nil, 0, err
	}
	parsed, ips, err := ValidateURL(ctx, d.WebhookURL, w.allowPrivate)
	if err != nil {
		return nil, 0, err
	}
	timestamp := now.Unix()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, parsed.String(), bytes.NewReader(d.Payload))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", w.userAgent)
	req.Header.Set("X-Earthquake-Delivery-ID", d.ID.String())
	req.Header.Set("X-Earthquake-Timestamp", strconv.FormatInt(timestamp, 10))
	req.Header.Set("X-Earthquake-Signature", Signature(secret, d.Payload, timestamp))
	allowed := map[string]bool{}
	for _, ip := range ips {
		allowed[ip.String()] = true
	}
	dialer := net.Dialer{Timeout: 5 * time.Second}
	transport := &http.Transport{DialContext: func(c context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		resolved, err := net.DefaultResolver.LookupIP(c, "ip", host)
		if err != nil {
			return nil, err
		}
		for _, ip := range resolved {
			if allowed[ip.String()] {
				return dialer.DialContext(c, network, net.JoinHostPort(ip.String(), port))
			}
		}
		return nil, ErrUnsafeDestination
	}}
	client := http.Client{Transport: transport, Timeout: w.httpTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = response.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, w.maxResponse))
	code := response.StatusCode
	if code < 200 || code >= 300 {
		return &code, parseRetryAfter(response.Header.Get("Retry-After"), now), fmt.Errorf("webhook returned HTTP %d", code)
	}
	return &code, 0, nil
}

type formattedTelegramMessage struct {
	text                string
	latitude, longitude *float64
	locationButton      string
}

func telegramMessage(payload []byte) (formattedTelegramMessage, error) {
	var delivery struct {
		Type      string            `json:"type"`
		Lifecycle string            `json:"lifecycle"`
		Sources   map[string]string `json:"sources"`
		Language  *string           `json:"language"`
		Shaking   *struct {
			MeanMMI  float64 `json:"mean_mmi"`
			LowerMMI float64 `json:"lower_mmi"`
			UpperMMI float64 `json:"upper_mmi"`
		} `json:"shaking"`
		Earthquake struct {
			Source     string    `json:"source"`
			Magnitude  *float64  `json:"magnitude"`
			DepthKM    *float64  `json:"depth_km"`
			DistanceKM *float64  `json:"distance_km"`
			Latitude   *float64  `json:"latitude"`
			Longitude  *float64  `json:"longitude"`
			Place      *string   `json:"place"`
			OccurredAt time.Time `json:"occurred_at"`
			SourceURL  *string   `json:"source_url"`
			DetailURL  *string   `json:"detail_url"`
		} `json:"earthquake"`
	}
	if err := json.Unmarshal(payload, &delivery); err != nil {
		return formattedTelegramMessage{}, fmt.Errorf("decode Telegram delivery: %w", err)
	}
	language := "en"
	if delivery.Language != nil && *delivery.Language == "ru" {
		language = "ru"
	}
	title := map[string]string{
		"new_event":                   "Earthquake detected",
		"magnitude_threshold_crossed": "Earthquake magnitude increased",
		"intensity_threshold_crossed": "Expected shaking intensity increased",
		"tsunami_activated":           "Tsunami alert activated",
		"alert_level_increased":       "Earthquake alert level increased",
	}[delivery.Type]
	if title == "" {
		title = "Earthquake update"
	}
	if language == "ru" {
		title = map[string]string{
			"new_event":                   "Обнаружено землетрясение",
			"magnitude_threshold_crossed": "Магнитуда землетрясения увеличилась",
			"intensity_threshold_crossed": "Ожидаемая интенсивность толчков увеличилась",
			"tsunami_activated":           "Объявлена угроза цунами",
			"alert_level_increased":       "Уровень опасности землетрясения повышен",
		}[delivery.Type]
		if title == "" {
			title = "Обновление данных о землетрясении"
		}
	}
	switch delivery.Lifecycle {
	case "preliminary":
		title = localized(language, "🟡 Preliminary earthquake — details are being refined", "🟡 Предварительное землетрясение — данные уточняются")
	case "confirmed":
		title = localized(language, "✅ Confirmed earthquake", "✅ Подтверждённое землетрясение")
	case "reviewed":
		title = localized(language, "🔵 Reviewed earthquake", "🔵 Проверенное землетрясение")
	case "retracted":
		title = localized(language, "❌ Retracted event", "❌ Событие отозвано")
	}
	lines := []string{title}
	if delivery.Earthquake.Magnitude != nil {
		lines = append(lines, fmt.Sprintf("%s: <b>%.1f</b>", localized(language, "Magnitude", "Магнитуда"), *delivery.Earthquake.Magnitude))
	}
	if delivery.Shaking != nil {
		mean := romanIntensity(delivery.Shaking.MeanMMI)
		description := intensityDescription(language, delivery.Shaking.MeanMMI)
		lines = append(lines, fmt.Sprintf("%s: <b>%s — %s</b>",
			localized(language, "Expected at your location", "Ожидается у вас"), mean, description))
		lines = append(lines, fmt.Sprintf("%s: %s–%s",
			localized(language, "Likely range", "Вероятный диапазон"),
			romanIntensity(delivery.Shaking.LowerMMI), romanIntensity(delivery.Shaking.UpperMMI)))
	}
	if delivery.Earthquake.DistanceKM != nil {
		lines = append(lines, fmt.Sprintf("%s: %.0f %s", localized(language, "Distance", "Расстояние"), *delivery.Earthquake.DistanceKM, localized(language, "km", "км")))
	}
	if delivery.Earthquake.DepthKM != nil {
		lines = append(lines, fmt.Sprintf("%s: %.1f %s", localized(language, "Depth", "Глубина"), *delivery.Earthquake.DepthKM, localized(language, "km", "км")))
	}
	if delivery.Earthquake.Place != nil && *delivery.Earthquake.Place != "" {
		lines = append(lines, localized(language, "Location", "Место")+": "+html.EscapeString(*delivery.Earthquake.Place))
	}
	if !delivery.Earthquake.OccurredAt.IsZero() {
		occurredAt := delivery.Earthquake.OccurredAt.UTC()
		fallback := occurredAt.Format("2006-01-02 15:04 UTC")
		lines = append(lines, fmt.Sprintf("%s: <tg-time unix=\"%d\" format=\"wDt\">%s</tg-time>", localized(language, "Time", "Время"), occurredAt.Unix(), fallback))
	}
	detailsURL := delivery.Earthquake.SourceURL
	if detailsURL == nil || *detailsURL == "" {
		detailsURL = delivery.Earthquake.DetailURL
	}
	if detailsURL != nil && validTelegramURL(*detailsURL) && delivery.Sources[delivery.Earthquake.Source] == "" {
		if delivery.Sources == nil {
			delivery.Sources = map[string]string{}
		}
		delivery.Sources[delivery.Earthquake.Source] = *detailsURL
	}
	lines = append(lines, telegramSourceLabels(delivery.Sources, delivery.Earthquake.Source))
	message := formattedTelegramMessage{text: strings.Join(lines, "\n"), locationButton: localized(language, "🗺 Show location", "🗺 Показать место")}
	if validTelegramCoordinates(delivery.Earthquake.Latitude, delivery.Earthquake.Longitude) {
		message.latitude = delivery.Earthquake.Latitude
		message.longitude = delivery.Earthquake.Longitude
	}
	return message, nil
}

func telegramSourceLabels(sources map[string]string, preferred string) string {
	ordered := []struct{ key, label string }{
		{"kndc", "KNDC"}, {"usgs", "USGS"}, {"geofon", "GEOFON"}, {"emsc", "EMSC"},
	}
	labels := make([]string, 0, len(ordered))
	for _, source := range ordered {
		if sourceURL, ok := sources[source.key]; ok {
			labels = append(labels, telegramSourceLabel(source.label, sourceURL))
		}
	}
	if len(labels) == 0 {
		for _, source := range ordered {
			if source.key == preferred {
				return telegramSourceLabel(source.label, "")
			}
		}
		return html.EscapeString(strings.ToUpper(preferred))
	}
	return strings.Join(labels, " | ")
}

func localized(language, english, russian string) string {
	if language == "ru" {
		return russian
	}
	return english
}

func romanIntensity(value float64) string {
	values := []string{"I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X"}
	index := int(math.Round(value)) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(values) {
		index = len(values) - 1
	}
	return values[index]
}

func intensityDescription(language string, value float64) string {
	english := []string{"not felt", "very weak", "weak", "light", "moderate", "strong", "very strong", "severe", "violent", "extreme"}
	russian := []string{"не ощущаются", "очень слабые", "слабые", "лёгкие", "умеренные", "сильные", "очень сильные", "разрушительные", "опустошительные", "экстремальные"}
	index := int(math.Round(value)) - 1
	index = max(0, min(index, len(english)-1))
	if language == "ru" {
		return russian[index]
	}
	return english[index]
}

func telegramSourceLabel(label, link string) string {
	if !validTelegramURL(link) {
		return label
	}
	return "<a href=\"" + html.EscapeString(link) + "\">" + label + "</a>"
}

func validTelegramURL(value string) bool {
	parsed, err := url.Parse(value)
	return err == nil && parsed.Host != "" && parsed.User == nil && (parsed.Scheme == "http" || parsed.Scheme == "https")
}

func validTelegramCoordinates(latitude, longitude *float64) bool {
	return latitude != nil && longitude != nil && !math.IsNaN(*latitude) && !math.IsNaN(*longitude) &&
		*latitude >= -90 && *latitude <= 90 && *longitude >= -180 && *longitude <= 180
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil && when.After(now) {
		return when.Sub(now)
	}
	return 0
}

func telegramRetryAfter(err error) time.Duration {
	if err == nil {
		return 0
	}
	var retryable interface{ RetryAfter() time.Duration }
	if errors.As(err, &retryable) {
		return retryable.RetryAfter()
	}
	return 0
}
func retryDelay(attempt int) time.Duration {
	d := 5 * time.Second * time.Duration(1<<min(attempt-1, 9))
	if d > time.Hour {
		d = time.Hour
	}
	return d/2 + time.Duration(rand.Int64N(int64(d/2)+1))
}
