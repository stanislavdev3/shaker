package notification

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/example/earthquake-service/internal/clock"
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
}

type TelegramSender interface {
	SendAlertMessage(context.Context, int64, string) (int64, error)
	EditAlertMessage(context.Context, int64, int64, string) error
}

func NewWorker(repo *postgres.Repository, cipher *Cipher, c clock.Clock, log *slog.Logger, id, userAgent string, batch, maxAttempts int, lockTimeout, poll, httpTimeout time.Duration, maxResponse int64, allowPrivate bool, telegram TelegramSender) *Worker {
	return &Worker{repo: repo, cipher: cipher, clock: c, log: log, id: id, userAgent: userAgent, batch: batch, maxAttempts: maxAttempts,
		lockTimeout: lockTimeout, pollInterval: poll, httpTimeout: httpTimeout, maxResponse: maxResponse, allowPrivate: allowPrivate, telegram: telegram}
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
	attempt := alert.AttemptCount + 1
	now := w.clock.Now()
	message, err := telegramMessage(alert.DesiredPayload)
	messageID := int64(0)
	if err == nil {
		if alert.TelegramMessageID == nil {
			messageID, err = w.telegram.SendAlertMessage(ctx, alert.TelegramChatID, message)
		} else {
			messageID = *alert.TelegramMessageID
			err = w.telegram.EditAlertMessage(ctx, alert.TelegramChatID, messageID, message)
		}
	}
	if err == nil {
		if saveErr := w.repo.CompleteTelegramAlertMessage(context.WithoutCancel(ctx), alert.ID,
			alert.DesiredEarthquakeVersion, messageID, attempt, w.clock.Now()); saveErr != nil {
			w.log.Error("persist Telegram alert result", "telegram_alert_id", alert.ID, "error", saveErr)
		}
		return
	}
	next := now.Add(retryDelay(attempt))
	if retryAfter := telegramRetryAfter(err); retryAfter > 0 {
		next = now.Add(retryAfter)
	}
	dead := attempt >= w.maxAttempts
	if saveErr := w.repo.FailTelegramAlertMessage(context.WithoutCancel(ctx), alert.ID, attempt, next, dead, err.Error(), w.clock.Now()); saveErr != nil {
		w.log.Error("persist Telegram alert failure", "telegram_alert_id", alert.ID, "error", saveErr)
	}
}
func (w *Worker) deliver(ctx context.Context, d postgres.Delivery) {
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
		_, err = w.telegram.SendAlertMessage(ctx, *d.TelegramChatID, message)
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

func telegramMessage(payload []byte) (string, error) {
	var delivery struct {
		Type       string `json:"type"`
		Lifecycle  string `json:"lifecycle"`
		Earthquake struct {
			Magnitude  *float64  `json:"magnitude"`
			DepthKM    *float64  `json:"depth_km"`
			DistanceKM *float64  `json:"distance_km"`
			Place      *string   `json:"place"`
			OccurredAt time.Time `json:"occurred_at"`
			SourceURL  *string   `json:"source_url"`
		} `json:"earthquake"`
	}
	if err := json.Unmarshal(payload, &delivery); err != nil {
		return "", fmt.Errorf("decode Telegram delivery: %w", err)
	}
	title := map[string]string{
		"new_event":                   "Earthquake detected",
		"magnitude_threshold_crossed": "Earthquake magnitude increased",
		"tsunami_activated":           "Tsunami alert activated",
		"alert_level_increased":       "Earthquake alert level increased",
	}[delivery.Type]
	if title == "" {
		title = "Earthquake update"
	}
	switch delivery.Lifecycle {
	case "preliminary":
		title = "🟡 Preliminary earthquake — details are being refined"
	case "confirmed":
		title = "✅ Confirmed earthquake"
	case "reviewed":
		title = "🔵 Reviewed earthquake"
	case "retracted":
		title = "❌ Retracted event"
	}
	lines := []string{title}
	if delivery.Earthquake.Magnitude != nil {
		lines = append(lines, fmt.Sprintf("Magnitude: %.1f", *delivery.Earthquake.Magnitude))
	}
	if delivery.Earthquake.DistanceKM != nil {
		lines = append(lines, fmt.Sprintf("Distance: %.0f km", *delivery.Earthquake.DistanceKM))
	}
	if delivery.Earthquake.DepthKM != nil {
		lines = append(lines, fmt.Sprintf("Depth: %.1f km", *delivery.Earthquake.DepthKM))
	}
	if delivery.Earthquake.Place != nil && *delivery.Earthquake.Place != "" {
		lines = append(lines, "Location: "+*delivery.Earthquake.Place)
	}
	if !delivery.Earthquake.OccurredAt.IsZero() {
		lines = append(lines, "Time: "+delivery.Earthquake.OccurredAt.UTC().Format(time.RFC3339))
	}
	if delivery.Earthquake.SourceURL != nil && *delivery.Earthquake.SourceURL != "" {
		lines = append(lines, "Details: "+*delivery.Earthquake.SourceURL)
	}
	return strings.Join(lines, "\n"), nil
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
