package geofon

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/example/earthquake-service/internal/clock"
	"github.com/example/earthquake-service/internal/domain/earthquake"
	"github.com/example/earthquake-service/internal/provider"
)

const (
	ProviderName = "geofon"
	Channel      = "geofon_fdsn"
	// GEOFON intermittently returns HTTP 500 for very large result limits,
	// even when the actual result set is small. Keep realtime requests bounded;
	// historical ingestion still paginates transparently.
	pageSize = 2000
)

type Client struct {
	baseURL, userAgent string
	httpClient         *http.Client
	clock              clock.Clock
	lookback           time.Duration
	maxBytes           int64
}

func New(baseURL, userAgent string, c clock.Clock, timeout, lookback time.Duration, maxBytes int64) *Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	return &Client{baseURL: baseURL, userAgent: userAgent, clock: c, lookback: lookback,
		maxBytes: maxBytes, httpClient: &http.Client{Transport: otelhttp.NewTransport(transport), Timeout: timeout}}
}

func (c *Client) Name() string { return ProviderName }

func (c *Client) FetchRealtime(ctx context.Context, _ provider.CacheValidators) ([]earthquake.Event, provider.FetchMetadata, error) {
	now := c.clock.Now().UTC()
	query := url.Values{"format": {"text"}, "orderby": {"time-asc"}, "limit": {strconv.Itoa(pageSize)},
		"starttime": {now.Add(-c.lookback).Format(time.RFC3339)}, "endtime": {now.Format(time.RFC3339)}}
	body, status, err := c.get(ctx, query)
	if err != nil || status == http.StatusNoContent {
		return nil, provider.FetchMetadata{}, err
	}
	events, invalid, err := Parse(body)
	return events, provider.FetchMetadata{InvalidCount: invalid}, err
}

func (c *Client) FetchHistorical(ctx context.Context, from, to time.Time, cursor *string) ([]earthquake.Event, *string, provider.FetchMetadata, error) {
	offset := 1
	var err error
	if cursor != nil {
		offset, err = strconv.Atoi(*cursor)
		if err != nil || offset < 1 {
			return nil, nil, provider.FetchMetadata{}, errors.New("invalid GEOFON historical cursor")
		}
	}
	query := url.Values{"format": {"text"}, "orderby": {"time-asc"}, "limit": {strconv.Itoa(pageSize)},
		"offset": {strconv.Itoa(offset)}, "starttime": {from.UTC().Format(time.RFC3339)},
		"endtime": {to.UTC().Format(time.RFC3339)}}
	body, status, err := c.get(ctx, query)
	if err != nil || status == http.StatusNoContent {
		return nil, nil, provider.FetchMetadata{}, err
	}
	events, invalid, err := Parse(body)
	if err != nil {
		return nil, nil, provider.FetchMetadata{}, err
	}
	var next *string
	if len(events)+invalid == pageSize {
		value := strconv.Itoa(offset + pageSize)
		next = &value
	}
	return events, next, provider.FetchMetadata{InvalidCount: invalid}, nil
}

func (c *Client) get(ctx context.Context, query url.Values) ([]byte, int, error) {
	endpoint, err := url.Parse(c.baseURL)
	if err != nil {
		return nil, 0, err
	}
	endpoint.RawQuery = query.Encode()
	for attempt := 0; attempt < 3; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
		if err != nil {
			return nil, 0, err
		}
		request.Header.Set("User-Agent", c.userAgent)
		request.Header.Set("Accept", "text/plain")
		response, err := c.httpClient.Do(request)
		if err == nil && response.StatusCode == http.StatusNoContent {
			_ = response.Body.Close()
			return nil, response.StatusCode, nil
		}
		if err == nil && response.StatusCode >= 200 && response.StatusCode < 300 {
			body, readErr := readBounded(response.Body, c.maxBytes)
			_ = response.Body.Close()
			return body, response.StatusCode, readErr
		}
		if err != nil && ctx.Err() != nil {
			return nil, 0, ctx.Err()
		}
		status := 0
		retryAfter := ""
		if response != nil {
			status = response.StatusCode
			retryAfter = response.Header.Get("Retry-After")
			_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
			_ = response.Body.Close()
		}
		retryable := err != nil || status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500
		if !retryable || attempt == 2 {
			if err != nil {
				return nil, status, err
			}
			return nil, status, fmt.Errorf("GEOFON FDSN returned HTTP %d", status)
		}
		if err := wait(ctx, retryDelay(attempt, retryAfter)); err != nil {
			return nil, status, err
		}
	}
	panic("unreachable")
}

type textEvent struct {
	EventID, Time, Latitude, Longitude, Depth, Contributor, ContributorID string
	MagnitudeType, Magnitude, Location, EventType                         string
	Raw                                                                   map[string]string
}

func Parse(data []byte) ([]earthquake.Event, int, error) {
	reader := csv.NewReader(bufio.NewReader(bytes.NewReader(data)))
	reader.Comma = '|'
	reader.FieldsPerRecord = -1
	header, err := reader.Read()
	if err != nil {
		return nil, 0, fmt.Errorf("read GEOFON header: %w", err)
	}
	indexes := make(map[string]int, len(header))
	for index, name := range header {
		indexes[strings.TrimPrefix(strings.TrimSpace(name), "#")] = index
	}
	for _, required := range []string{"EventID", "Time", "Latitude", "Longitude"} {
		if _, ok := indexes[required]; !ok {
			return nil, 0, fmt.Errorf("GEOFON response is missing %s", required)
		}
	}
	var events []earthquake.Event
	invalid := 0
	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			invalid++
			continue
		}
		value := func(name string) string {
			index, ok := indexes[name]
			if !ok || index >= len(record) {
				return ""
			}
			return strings.TrimSpace(record[index])
		}
		raw := make(map[string]string, len(indexes))
		for name := range indexes {
			raw[name] = value(name)
		}
		row := textEvent{EventID: value("EventID"), Time: value("Time"), Latitude: value("Latitude"),
			Longitude: value("Longitude"), Depth: value("Depth/km"), Contributor: value("Contributor"),
			ContributorID: value("ContributorID"), MagnitudeType: value("MagType"),
			Magnitude: value("Magnitude"), Location: value("EventLocationName"), EventType: value("EventType"), Raw: raw}
		event, parseErr := parseRow(row)
		if parseErr != nil {
			invalid++
			continue
		}
		events = append(events, event)
	}
	return events, invalid, nil
}

func parseRow(row textEvent) (earthquake.Event, error) {
	occurredAt, err := time.Parse(time.RFC3339Nano, row.Time+zoneSuffix(row.Time))
	if err != nil {
		return earthquake.Event{}, err
	}
	latitude, err := strconv.ParseFloat(row.Latitude, 64)
	if err != nil {
		return earthquake.Event{}, err
	}
	longitude, err := strconv.ParseFloat(row.Longitude, 64)
	if err != nil {
		return earthquake.Event{}, err
	}
	depth, err := optionalFloat(row.Depth)
	if err != nil {
		return earthquake.Event{}, err
	}
	magnitude, err := optionalFloat(row.Magnitude)
	if err != nil {
		return earthquake.Event{}, err
	}
	status := "confirmed"
	eventType := strings.ToLower(row.EventType)
	if eventType == "" {
		eventType = "earthquake"
	}
	detailURL := "https://geofon.gfz.de/eqinfo/event.php?id=" + url.QueryEscape(row.EventID)
	raw, _ := json.Marshal(row.Raw)
	event := earthquake.Event{Provider: ProviderName, ExternalID: row.EventID, OccurredAt: occurredAt.UTC(),
		// FDSN text has no revision timestamp. Equal origin timestamps plus the
		// canonical payload hash still detect corrected rows deterministically.
		SourceUpdatedAt: occurredAt.UTC(), Latitude: latitude, Longitude: longitude, DepthKM: depth,
		Magnitude: magnitude, Place: stringPointer(row.Location), Title: stringPointer(row.Location),
		MagnitudeType: stringPointer(row.MagnitudeType), Status: &status, EventType: &eventType,
		DetailURL: &detailURL, RawPayload: raw, ObservationChannel: Channel, SolutionClass: earthquake.ConfirmedSolution}
	return event, event.Validate()
}

func zoneSuffix(value string) string {
	if strings.HasSuffix(value, "Z") || (len(value) >= 6 && strings.ContainsAny(value[len(value)-6:], "+-")) {
		return ""
	}
	return "Z"
}

func optionalFloat(value string) (*float64, error) {
	if value == "" {
		return nil, nil
	}
	parsed, err := strconv.ParseFloat(value, 64)
	return &parsed, err
}

func stringPointer(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func readBounded(reader io.Reader, maxBytes int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(reader, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > maxBytes {
		return nil, errors.New("GEOFON response exceeds configured maximum")
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
