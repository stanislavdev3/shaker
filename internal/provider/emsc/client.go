package emsc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/example/earthquake-service/internal/clock"
	"github.com/example/earthquake-service/internal/domain/earthquake"
	"github.com/example/earthquake-service/internal/provider"
)

const fdsnPageSize = 20000

type FDSNClient struct {
	baseURL, userAgent string
	httpClient         *http.Client
	clock              clock.Clock
	lookback           time.Duration
	maxBytes           int64
	maxAttempts        int
}

func NewFDSN(baseURL, userAgent string, c clock.Clock, timeout, lookback time.Duration, maxBytes int64) *FDSNClient {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	return &FDSNClient{
		baseURL: baseURL, userAgent: userAgent, httpClient: &http.Client{Transport: otelhttp.NewTransport(transport), Timeout: timeout},
		clock: c, lookback: lookback, maxBytes: maxBytes, maxAttempts: 3,
	}
}

func (c *FDSNClient) Name() string { return ProviderName }

func (c *FDSNClient) FetchRealtime(ctx context.Context, _ provider.CacheValidators) ([]earthquake.Event, provider.FetchMetadata, error) {
	query := url.Values{}
	query.Set("format", "json")
	query.Set("updatedafter", c.clock.Now().Add(-c.lookback).UTC().Format(time.RFC3339Nano))
	query.Set("orderby", "time-asc")
	query.Set("limit", strconv.Itoa(fdsnPageSize))
	body, status, err := c.get(ctx, query)
	if err != nil {
		return nil, provider.FetchMetadata{}, err
	}
	if status == http.StatusNoContent {
		return nil, provider.FetchMetadata{}, nil
	}
	events, invalid, err := ParseFDSN(body)
	return events, provider.FetchMetadata{InvalidCount: invalid}, err
}

func (c *FDSNClient) FetchHistorical(ctx context.Context, from, to time.Time, cursor *string) ([]earthquake.Event, *string, provider.FetchMetadata, error) {
	offset := 1
	var err error
	if cursor != nil {
		offset, err = strconv.Atoi(*cursor)
		if err != nil || offset < 1 {
			return nil, nil, provider.FetchMetadata{}, errors.New("invalid EMSC historical cursor")
		}
	}
	query := url.Values{}
	query.Set("format", "json")
	query.Set("starttime", from.UTC().Format(time.RFC3339Nano))
	query.Set("endtime", to.UTC().Format(time.RFC3339Nano))
	query.Set("orderby", "time-asc")
	query.Set("limit", strconv.Itoa(fdsnPageSize))
	query.Set("offset", strconv.Itoa(offset))
	body, status, err := c.get(ctx, query)
	if err != nil {
		return nil, nil, provider.FetchMetadata{}, err
	}
	if status == http.StatusNoContent {
		return nil, nil, provider.FetchMetadata{}, nil
	}
	events, invalid, err := ParseFDSN(body)
	if err != nil {
		return nil, nil, provider.FetchMetadata{}, err
	}
	var next *string
	if len(events)+invalid == fdsnPageSize {
		value := strconv.Itoa(offset + fdsnPageSize)
		next = &value
	}
	return events, next, provider.FetchMetadata{InvalidCount: invalid}, nil
}

func (c *FDSNClient) get(ctx context.Context, query url.Values) ([]byte, int, error) {
	endpoint, err := url.Parse(c.baseURL)
	if err != nil {
		return nil, 0, err
	}
	endpoint.RawQuery = query.Encode()
	for attempt := 0; attempt < c.maxAttempts; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
		if err != nil {
			return nil, 0, err
		}
		request.Header.Set("User-Agent", c.userAgent)
		request.Header.Set("Accept", "application/json")
		response, err := c.httpClient.Do(request)
		if err != nil {
			if ctx.Err() != nil {
				return nil, 0, ctx.Err()
			}
			if attempt+1 == c.maxAttempts {
				return nil, 0, err
			}
			if err := wait(ctx, retryDelay(attempt, "")); err != nil {
				return nil, 0, err
			}
			continue
		}
		if response.StatusCode == http.StatusNoContent {
			_ = response.Body.Close()
			return nil, response.StatusCode, nil
		}
		if response.StatusCode >= 200 && response.StatusCode < 300 {
			body, err := readBounded(response.Body, c.maxBytes)
			_ = response.Body.Close()
			return body, response.StatusCode, err
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		_ = response.Body.Close()
		retryable := response.StatusCode == http.StatusRequestTimeout || response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500
		if !retryable || attempt+1 == c.maxAttempts {
			return nil, response.StatusCode, fmt.Errorf("EMSC FDSN returned HTTP %d", response.StatusCode)
		}
		if err := wait(ctx, retryDelay(attempt, response.Header.Get("Retry-After"))); err != nil {
			return nil, 0, err
		}
	}
	panic("unreachable")
}

func readBounded(reader io.Reader, maxBytes int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(reader, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > maxBytes {
		return nil, errors.New("EMSC response exceeds configured maximum")
	}
	return body, nil
}

func retryDelay(attempt int, retryAfter string) time.Duration {
	if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	base := time.Second << attempt
	return base/2 + time.Duration(rand.Int64N(int64(base/2)+1))
}

func wait(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
