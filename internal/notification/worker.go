package notification

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
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
}

func NewWorker(repo *postgres.Repository, cipher *Cipher, c clock.Clock, log *slog.Logger, id, userAgent string, batch, maxAttempts int, lockTimeout, poll, httpTimeout time.Duration, maxResponse int64, allowPrivate bool) *Worker {
	return &Worker{repo: repo, cipher: cipher, clock: c, log: log, id: id, userAgent: userAgent, batch: batch, maxAttempts: maxAttempts,
		lockTimeout: lockTimeout, pollInterval: poll, httpTimeout: httpTimeout, maxResponse: maxResponse, allowPrivate: allowPrivate}
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
}
func (w *Worker) deliver(ctx context.Context, d postgres.Delivery) {
	attempt := d.AttemptCount + 1
	now := w.clock.Now()
	secret, err := w.cipher.Decrypt(d.EncryptedSecret)
	var statusCode *int
	var serverRetryDelay time.Duration
	if err == nil {
		parsed, ips, e := ValidateURL(ctx, d.WebhookURL, w.allowPrivate)
		err = e
		if err == nil {
			timestamp := now.Unix()
			req, e := http.NewRequestWithContext(ctx, http.MethodPost, parsed.String(), bytes.NewReader(d.Payload))
			err = e
			if err == nil {
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
				tr := &http.Transport{DialContext: func(c context.Context, network, address string) (net.Conn, error) {
					host, port, e := net.SplitHostPort(address)
					if e != nil {
						return nil, e
					}
					ips, e := net.DefaultResolver.LookupIP(c, "ip", host)
					if e != nil {
						return nil, e
					}
					for _, ip := range ips {
						if allowed[ip.String()] {
							return dialer.DialContext(c, network, net.JoinHostPort(ip.String(), port))
						}
					}
					return nil, ErrUnsafeDestination
				}}
				client := http.Client{Transport: tr, Timeout: w.httpTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
				resp, e := client.Do(req)
				err = e
				if err == nil {
					code := resp.StatusCode
					statusCode = &code
					_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, w.maxResponse))
					_ = resp.Body.Close()
					if code < 200 || code >= 300 {
						serverRetryDelay = parseRetryAfter(resp.Header.Get("Retry-After"), now)
						err = fmt.Errorf("webhook returned HTTP %d", code)
					}
				}
			}
		}
	}
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

func parseRetryAfter(value string, now time.Time) time.Duration {
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil && when.After(now) {
		return when.Sub(now)
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
