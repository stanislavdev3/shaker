package usgs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/example/earthquake-service/internal/domain/earthquake"
	"github.com/example/earthquake-service/internal/provider"
)

type Client struct {
	realtimeURL, historicalURL, userAgent string
	httpClient                            *http.Client
	maxBytes                              int64
	maxAttempts                           int
}

func New(realtimeURL, historicalURL, userAgent string, timeout time.Duration, maxBytes int64) *Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.DialContext = (&netDialer{timeout: 5 * time.Second}).DialContext
	return &Client{realtimeURL: realtimeURL, historicalURL: historicalURL, userAgent: userAgent,
		httpClient: &http.Client{Transport: otelhttp.NewTransport(tr), Timeout: timeout}, maxBytes: maxBytes, maxAttempts: 3}
}

func (c *Client) Name() string { return "usgs" }

func (c *Client) FetchRealtime(ctx context.Context, validators provider.CacheValidators) ([]earthquake.Event, provider.FetchMetadata, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.realtimeURL, nil)
	if err != nil {
		return nil, provider.FetchMetadata{}, err
	}
	if validators.ETag != "" {
		req.Header.Set("If-None-Match", validators.ETag)
	}
	if validators.LastModified != "" {
		req.Header.Set("If-Modified-Since", validators.LastModified)
	}
	body, hdr, status, err := c.do(req)
	meta := provider.FetchMetadata{ETag: hdr.Get("ETag"), LastModified: hdr.Get("Last-Modified")}
	if status == http.StatusNotModified {
		meta.NotModified = true
		return nil, meta, nil
	}
	if err != nil {
		return nil, meta, err
	}
	events, invalid, err := ParseFeed(body)
	meta.InvalidCount = invalid
	return events, meta, err
}

func (c *Client) FetchHistorical(ctx context.Context, from, to time.Time, cursor *string) ([]earthquake.Event, *string, provider.FetchMetadata, error) {
	u, err := url.Parse(c.historicalURL)
	if err != nil {
		return nil, nil, provider.FetchMetadata{}, err
	}
	q := u.Query()
	q.Set("format", "geojson")
	q.Set("starttime", from.UTC().Format(time.RFC3339))
	q.Set("endtime", to.UTC().Format(time.RFC3339))
	q.Set("orderby", "time-asc")
	q.Set("limit", "20000")
	offset := 1
	if cursor != nil {
		offset, err = strconv.Atoi(*cursor)
		if err != nil || offset < 1 {
			return nil, nil, provider.FetchMetadata{}, errors.New("invalid historical cursor")
		}
	}
	q.Set("offset", strconv.Itoa(offset))
	u.RawQuery = q.Encode()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	body, _, _, err := c.do(req)
	if err != nil {
		return nil, nil, provider.FetchMetadata{}, err
	}
	events, invalid, err := ParseFeed(body)
	var next *string
	if len(events) == 20000 {
		n := strconv.Itoa(offset + len(events))
		next = &n
	}
	return events, next, provider.FetchMetadata{InvalidCount: invalid}, err
}

func (c *Client) do(req *http.Request) ([]byte, http.Header, int, error) {
	req.Header.Set("User-Agent", c.userAgent)
	for attempt := 0; attempt < c.maxAttempts; attempt++ {
		resp, err := c.httpClient.Do(req)
		if err != nil {
			if req.Context().Err() != nil {
				return nil, nil, 0, req.Context().Err()
			}
			if attempt+1 == c.maxAttempts {
				return nil, nil, 0, err
			}
			if err := wait(req.Context(), backoff(attempt, "")); err != nil {
				return nil, nil, 0, err
			}
			continue
		}
		if resp.StatusCode == http.StatusNotModified {
			_ = resp.Body.Close()
			return nil, resp.Header, resp.StatusCode, nil
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			body, err := readBounded(resp.Body, c.maxBytes)
			_ = resp.Body.Close()
			return body, resp.Header, resp.StatusCode, err
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
		retryable := resp.StatusCode == 408 || resp.StatusCode == 429 || resp.StatusCode >= 500
		if !retryable || attempt+1 == c.maxAttempts {
			return nil, resp.Header, resp.StatusCode, fmt.Errorf("USGS returned HTTP %d", resp.StatusCode)
		}
		if err := wait(req.Context(), backoff(attempt, resp.Header.Get("Retry-After"))); err != nil {
			return nil, nil, 0, err
		}
	}
	panic("unreachable")
}

func readBounded(r io.Reader, max int64) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > max {
		return nil, errors.New("provider response exceeds configured maximum")
	}
	return b, nil
}
func backoff(attempt int, retryAfter string) time.Duration {
	if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	base := time.Second << attempt
	return base/2 + time.Duration(rand.Int64N(int64(base/2)+1))
}
func wait(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

type feed struct {
	Type     string            `json:"type"`
	Features []json.RawMessage `json:"features"`
}
type feature struct {
	Type     string `json:"type"`
	ID       string `json:"id"`
	Geometry struct {
		Type        string        `json:"type"`
		Coordinates []json.Number `json:"coordinates"`
	} `json:"geometry"`
	Properties struct {
		Mag       *float64 `json:"mag"`
		Place     *string  `json:"place"`
		Time      *int64   `json:"time"`
		Updated   *int64   `json:"updated"`
		TZ        *int     `json:"tz"`
		URL       *string  `json:"url"`
		Detail    *string  `json:"detail"`
		Felt      *int     `json:"felt"`
		CDI       *float64 `json:"cdi"`
		MMI       *float64 `json:"mmi"`
		Alert     *string  `json:"alert"`
		Status    *string  `json:"status"`
		Tsunami   *int     `json:"tsunami"`
		Sig       *int     `json:"sig"`
		Net       *string  `json:"net"`
		Code      *string  `json:"code"`
		IDs       *string  `json:"ids"`
		Sources   *string  `json:"sources"`
		Types     *string  `json:"types"`
		NST       *int     `json:"nst"`
		DMin      *float64 `json:"dmin"`
		RMS       *float64 `json:"rms"`
		Gap       *float64 `json:"gap"`
		MagType   *string  `json:"magType"`
		EventType *string  `json:"type"`
		Title     *string  `json:"title"`
	} `json:"properties"`
}

func ParseFeed(data []byte) ([]earthquake.Event, int, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var f feed
	if err := dec.Decode(&f); err != nil {
		return nil, 0, fmt.Errorf("decode GeoJSON: %w", err)
	}
	if f.Type != "FeatureCollection" || f.Features == nil {
		return nil, 0, errors.New("invalid GeoJSON FeatureCollection")
	}
	events := make([]earthquake.Event, 0, len(f.Features))
	invalid := 0
	for _, raw := range f.Features {
		e, err := parseFeature(raw)
		if err != nil {
			invalid++
			continue
		}
		events = append(events, e)
	}
	return events, invalid, nil
}

func parseFeature(raw json.RawMessage) (earthquake.Event, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var f feature
	if err := dec.Decode(&f); err != nil {
		return earthquake.Event{}, err
	}
	if f.Type != "Feature" || f.ID == "" || f.Geometry.Type != "Point" || len(f.Geometry.Coordinates) < 2 || f.Properties.Time == nil || f.Properties.Updated == nil {
		return earthquake.Event{}, errors.New("malformed USGS feature")
	}
	lon, err := f.Geometry.Coordinates[0].Float64()
	if err != nil {
		return earthquake.Event{}, err
	}
	lat, err := f.Geometry.Coordinates[1].Float64()
	if err != nil {
		return earthquake.Event{}, err
	}
	var depth *float64
	if len(f.Geometry.Coordinates) > 2 {
		d, e := f.Geometry.Coordinates[2].Float64()
		if e != nil {
			return earthquake.Event{}, e
		}
		depth = &d
	}
	var tsunami *bool
	if f.Properties.Tsunami != nil {
		v := *f.Properties.Tsunami != 0
		tsunami = &v
	}
	e := earthquake.Event{Provider: "usgs", ExternalID: f.ID, OccurredAt: time.UnixMilli(*f.Properties.Time).UTC(),
		SourceUpdatedAt: time.UnixMilli(*f.Properties.Updated).UTC(), Latitude: lat, Longitude: lon, DepthKM: depth,
		Magnitude: f.Properties.Mag, MagnitudeType: f.Properties.MagType, Place: f.Properties.Place, Title: f.Properties.Title,
		Status: f.Properties.Status, EventType: f.Properties.EventType, AlertLevel: f.Properties.Alert, Tsunami: tsunami,
		Significance: f.Properties.Sig, FeltReports: f.Properties.Felt, CDI: f.Properties.CDI, MMI: f.Properties.MMI,
		StationCount: f.Properties.NST, AzimuthalGap: f.Properties.Gap, MinimumDistance: f.Properties.DMin,
		RMS: f.Properties.RMS, SourceURL: f.Properties.URL, DetailURL: f.Properties.Detail, RawPayload: append([]byte(nil), raw...)}
	return e, e.Validate()
}
