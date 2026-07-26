package kndc

import (
	"bytes"
	"context"
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

	"github.com/example/earthquake-service/internal/domain/earthquake"
	"github.com/example/earthquake-service/internal/provider"
)

const (
	ProviderName = "kndc"
	Channel      = "kndc_alarm_bulletin"
	realtimeSize = 100
	pageSize     = 200
)

type Client struct {
	baseURL, userAgent string
	httpClient         *http.Client
	maxBytes           int64
}

func New(baseURL, userAgent string, timeout time.Duration, maxBytes int64) *Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), userAgent: userAgent, maxBytes: maxBytes,
		httpClient: &http.Client{Transport: otelhttp.NewTransport(transport), Timeout: timeout}}
}

func (c *Client) Name() string { return ProviderName }

func (c *Client) FetchRealtime(ctx context.Context, _ provider.CacheValidators) ([]earthquake.Event, provider.FetchMetadata, error) {
	body, err := c.fetchPage(ctx, 0, realtimeSize)
	if err != nil {
		return nil, provider.FetchMetadata{}, err
	}
	events, invalid, err := Parse(body)
	return events, provider.FetchMetadata{InvalidCount: invalid}, err
}

func (c *Client) FetchHistorical(ctx context.Context, from, to time.Time, cursor *string) ([]earthquake.Event, *string, provider.FetchMetadata, error) {
	offset := 0
	var err error
	if cursor != nil {
		offset, err = strconv.Atoi(*cursor)
		if err != nil || offset < 0 {
			return nil, nil, provider.FetchMetadata{}, errors.New("invalid KNDC historical cursor")
		}
	}
	body, err := c.fetchPage(ctx, offset, pageSize)
	if err != nil {
		return nil, nil, provider.FetchMetadata{}, err
	}
	page, invalid, err := Parse(body)
	if err != nil {
		return nil, nil, provider.FetchMetadata{}, err
	}
	events := make([]earthquake.Event, 0, len(page))
	for _, event := range page {
		if !event.OccurredAt.Before(from) && event.OccurredAt.Before(to) {
			events = append(events, event)
		}
	}
	var next *string
	if len(page)+invalid == pageSize && len(page) > 0 && !page[len(page)-1].OccurredAt.Before(from) {
		value := strconv.Itoa(offset + pageSize)
		next = &value
	}
	return events, next, provider.FetchMetadata{InvalidCount: invalid}, nil
}

func (c *Client) fetchPage(ctx context.Context, offset, limit int) ([]byte, error) {
	endpoint, err := url.Parse(c.baseURL + "/getOriginList.php")
	if err != nil {
		return nil, err
	}
	query := endpoint.Query()
	query.Set("orderby", "epochtime")
	query.Set("desc", "yes")
	query.Set("activepage", "1")
	query.Set("start", strconv.Itoa(offset))
	query.Set("limit", strconv.Itoa(limit))
	endpoint.RawQuery = query.Encode()
	for attempt := 0; attempt < 3; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
		if err != nil {
			return nil, err
		}
		request.Header.Set("User-Agent", c.userAgent)
		request.Header.Set("Accept", "application/json")
		response, err := c.httpClient.Do(request)
		if err == nil && response.StatusCode >= 200 && response.StatusCode < 300 {
			body, readErr := readBounded(response.Body, c.maxBytes)
			_ = response.Body.Close()
			return body, readErr
		}
		if err != nil && ctx.Err() != nil {
			return nil, ctx.Err()
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
				return nil, err
			}
			return nil, fmt.Errorf("KNDC bulletin returned HTTP %d", status)
		}
		if err := wait(ctx, retryDelay(attempt, retryAfter)); err != nil {
			return nil, err
		}
	}
	panic("unreachable")
}

type bulletinRow struct {
	ID        string          `json:"id"`
	EventDate string          `json:"evdate"`
	EventTime string          `json:"evtime"`
	Millis    string          `json:"evmsec"`
	Latitude  string          `json:"lat"`
	Longitude string          `json:"lon"`
	Depth     json.RawMessage `json:"depth"`
	MB        json.RawMessage `json:"mb"`
	MPV       json.RawMessage `json:"mpv"`
	Region    string          `json:"gregion"`
	Updated   string          `json:"lddate"`
	RMS       json.RawMessage `json:"rms"`
	Gap       json.RawMessage `json:"gap"`
	MinDist   json.RawMessage `json:"mindist"`
	Stations  json.RawMessage `json:"nsta"`
}

func Parse(data []byte) ([]earthquake.Event, int, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var rows []json.RawMessage
	if err := decoder.Decode(&rows); err != nil {
		return nil, 0, fmt.Errorf("decode KNDC bulletin: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, 0, errors.New("KNDC bulletin contains multiple JSON values")
	}
	events := make([]earthquake.Event, 0, len(rows))
	invalid := 0
	for _, raw := range rows {
		var row bulletinRow
		if err := json.Unmarshal(raw, &row); err != nil {
			invalid++
			continue
		}
		event, err := parseRow(row, raw)
		if err != nil {
			invalid++
			continue
		}
		events = append(events, event)
	}
	return events, invalid, nil
}

func parseRow(row bulletinRow, raw json.RawMessage) (earthquake.Event, error) {
	if row.ID == "" {
		return earthquake.Event{}, errors.New("KNDC row is missing identity or origin time")
	}
	// KNDC's epochtime is shifted by six hours in observed payloads. The
	// bulletin's displayed origin fields match independent catalogues and are UTC.
	origin, err := time.ParseInLocation("2006-01-02 15:04:05", row.EventDate+" "+row.EventTime, time.UTC)
	if err != nil {
		return earthquake.Event{}, fmt.Errorf("parse KNDC origin time: %w", err)
	}
	fraction := int64(0)
	if row.Millis != "" {
		fractionValue, parseErr := strconv.ParseFloat("0."+row.Millis, 64)
		if parseErr != nil {
			return earthquake.Event{}, parseErr
		}
		fraction = int64(fractionValue * float64(time.Second))
	}
	latitude, err := strconv.ParseFloat(row.Latitude, 64)
	if err != nil {
		return earthquake.Event{}, err
	}
	longitude, err := strconv.ParseFloat(row.Longitude, 64)
	if err != nil {
		return earthquake.Event{}, err
	}
	depth, err := optionalJSONFloat(row.Depth)
	if err != nil {
		return earthquake.Event{}, err
	}
	mb, err := optionalJSONFloat(row.MB)
	if err != nil {
		return earthquake.Event{}, err
	}
	mpv, err := optionalJSONFloat(row.MPV)
	if err != nil {
		return earthquake.Event{}, err
	}
	magnitude, magnitudeType := mb, "mb"
	if magnitude == nil {
		magnitude, magnitudeType = mpv, "Mpv"
	}
	updatedAt, err := time.ParseInLocation("2006-01-02 15:04:05", row.Updated, time.FixedZone("KNDC web", 6*60*60))
	if err != nil {
		return earthquake.Event{}, err
	}
	rms, _ := optionalJSONFloat(row.RMS)
	gap, _ := optionalJSONFloat(row.Gap)
	minimumDistance, _ := optionalJSONFloat(row.MinDist)
	stationCount, _ := optionalJSONInt(row.Stations)
	status, eventType := "confirmed", "earthquake"
	detailURL := "https://kndc.kz/kndc/pagecontent/alarm-bulletin/moredetails.php?id=" + url.QueryEscape(row.ID)
	event := earthquake.Event{Provider: ProviderName, ExternalID: row.ID,
		OccurredAt: origin.Add(time.Duration(fraction)), SourceUpdatedAt: updatedAt.UTC(), Latitude: latitude,
		Longitude: longitude, DepthKM: depth, Magnitude: magnitude, MagnitudeType: &magnitudeType,
		Place: stringPointer(row.Region), Title: stringPointer(row.Region), Status: &status, EventType: &eventType,
		StationCount: stationCount, AzimuthalGap: gap, MinimumDistance: minimumDistance, RMS: rms,
		DetailURL: &detailURL, RawPayload: append([]byte(nil), raw...), ObservationChannel: Channel,
		SolutionClass: earthquake.ConfirmedSolution}
	return event, event.Validate()
}

func optionalJSONFloat(raw json.RawMessage) (*float64, error) {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil, nil
	}
	value := strings.Trim(string(raw), "\"")
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return nil, err
	}
	if parsed == -999 {
		return nil, nil
	}
	return &parsed, nil
}

func optionalJSONInt(raw json.RawMessage) (*int, error) {
	value, err := optionalJSONFloat(raw)
	if err != nil || value == nil {
		return nil, err
	}
	converted := int(*value)
	return &converted, nil
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
		return nil, errors.New("KNDC response exceeds configured maximum")
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
