package httpapi

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/example/earthquake-service/internal/clock"
	domainnotification "github.com/example/earthquake-service/internal/domain/notification"
	appnotification "github.com/example/earthquake-service/internal/notification"
	"github.com/example/earthquake-service/internal/observability"
	"github.com/example/earthquake-service/internal/realtime"
	"github.com/example/earthquake-service/internal/repository/postgres"
)

type Server struct {
	repo       *postgres.Repository
	log        *slog.Logger
	clock      clock.Clock
	metrics    *observability.Metrics
	adminKey   string
	cursorKey  []byte
	cipher     *appnotification.Cipher
	production bool
	maxRadius  float64
	realtime   *realtime.Hub
}

func New(repo *postgres.Repository, log *slog.Logger, c clock.Clock, m *observability.Metrics, admin string, cursorKey []byte, cipher *appnotification.Cipher, production bool, maxRadius float64, realtimeHub *realtime.Hub, adminHandler http.Handler) http.Handler {
	s := &Server{repo: repo, log: log, clock: c, metrics: m, adminKey: admin, cursorKey: cursorKey, cipher: cipher, production: production, maxRadius: maxRadius, realtime: realtimeHub}
	r := chi.NewRouter()
	r.Use(s.requestID, s.recoverer, s.accessLog, newRateLimiter(120, time.Minute).middleware)
	r.Get("/health/live", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	r.Get("/health/ready", s.ready)
	r.Handle("/metrics", promhttp.Handler())
	r.Route("/api", s.publicRoutes)
	r.Route("/admin/api", func(r chi.Router) {
		r.Use(s.adminAuth)
		r.Get("/earthquakes/{id}/revisions", s.revisions)
		r.Post("/notification-subscriptions", s.createSubscription)
		r.Get("/notification-subscriptions", s.listSubscriptions)
		r.Get("/notification-subscriptions/{id}", s.getSubscription)
		r.Patch("/notification-subscriptions/{id}", s.patchSubscription)
		r.Delete("/notification-subscriptions/{id}", s.deleteSubscription)
		r.Get("/notification-deliveries", s.listDeliveries)
		r.Get("/notification-deliveries/{id}", s.getDelivery)
		r.Post("/notification-deliveries/{id}/retry", s.retryDelivery)
	})
	if adminHandler != nil {
		r.Mount("/admin", adminHandler)
	}
	return otelhttp.NewHandler(r, "http.server")
}

func (s *Server) publicRoutes(r chi.Router) {
	r.Get("/earthquakes", s.list)
	r.Get("/earthquakes/{id}", s.details)
	r.Get("/stream", s.stream)
}

type listResponse struct {
	Data       any `json:"data"`
	Pagination struct {
		NextCursor *string `json:"next_cursor"`
	} `json:"pagination"`
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	f, format, err := s.parseList(r)
	if err != nil {
		s.problem(w, r, http.StatusBadRequest, "validation-error", "Validation error", err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	events, err := s.repo.List(ctx, f)
	if err != nil {
		s.internal(w, r, err)
		return
	}
	var next *string
	if len(events) == f.Limit {
		last := events[len(events)-1]
		c := cursorPayload{Version: 1, Sort: f.Sort, ID: last.ID}
		if f.Sort == "magnitude_desc" {
			c.Magnitude = last.Magnitude
		} else {
			c.OccurredAt = &last.OccurredAt
		}
		v, e := encodeCursor(c, s.cursorKey)
		if e == nil {
			next = &v
		}
	}
	if format == "geojson" {
		features := make([]any, 0, len(events))
		for _, e := range events {
			features = append(features, map[string]any{"type": "Feature", "id": e.ID, "geometry": map[string]any{"type": "Point", "coordinates": []any{e.Longitude, e.Latitude, e.DepthKM}}, "properties": e})
		}
		w.Header().Set("Content-Type", "application/geo+json")
		writeJSON(w, 200, map[string]any{"type": "FeatureCollection", "features": features, "pagination": map[string]any{"next_cursor": next}})
		return
	}
	var out listResponse
	out.Data = events
	out.Pagination.NextCursor = next
	writeJSON(w, 200, out)
}
func (s *Server) parseList(r *http.Request) (postgres.ListFilter, string, error) {
	q := r.URL.Query()
	f := postgres.ListFilter{Limit: 50, Sort: "occurred_at_desc"}
	var err error
	if q.Get("limit") != "" {
		f.Limit, err = strconv.Atoi(q.Get("limit"))
		if err != nil || f.Limit < 1 || f.Limit > 200 {
			return f, "", errors.New("limit must be between 1 and 200")
		}
	}
	if v := q.Get("sort"); v != "" {
		if v != "occurred_at_desc" && v != "occurred_at_asc" && v != "magnitude_desc" {
			return f, "", errors.New("unsupported sort")
		}
		f.Sort = v
	}
	if f.From, err = parseTimePtr(q.Get("from")); err != nil {
		return f, "", err
	}
	if f.To, err = parseTimePtr(q.Get("to")); err != nil {
		return f, "", err
	}
	if f.MinMagnitude, err = parseFloatPtr(q.Get("min_magnitude")); err != nil {
		return f, "", err
	}
	if f.MaxMagnitude, err = parseFloatPtr(q.Get("max_magnitude")); err != nil {
		return f, "", err
	}
	if f.MinDepth, err = parseFloatPtr(q.Get("min_depth_km")); err != nil {
		return f, "", err
	}
	if f.MaxDepth, err = parseFloatPtr(q.Get("max_depth_km")); err != nil {
		return f, "", err
	}
	lat, le := parseFloatPtr(q.Get("latitude"))
	lon, loe := parseFloatPtr(q.Get("longitude"))
	radius, re := parseFloatPtr(q.Get("radius_km"))
	if le != nil || loe != nil || re != nil {
		return f, "", errors.New("invalid radius parameters")
	}
	if lat != nil || lon != nil || radius != nil {
		if lat == nil || lon == nil || radius == nil {
			return f, "", errors.New("latitude, longitude, and radius_km must be supplied together")
		}
		if *lat < -90 || *lat > 90 || *lon < -180 || *lon > 180 || *radius <= 0 || *radius > s.maxRadius {
			return f, "", errors.New("invalid or excessive radius")
		}
		f.Latitude, f.Longitude, f.RadiusKM = lat, lon, radius
	}
	if v := q.Get("bbox"); v != "" {
		if radius != nil {
			return f, "", errors.New("bbox and radius search are mutually exclusive")
		}
		parts := strings.Split(v, ",")
		if len(parts) != 4 {
			return f, "", errors.New("bbox must contain four coordinates")
		}
		var box [4]float64
		for i := range parts {
			box[i], err = strconv.ParseFloat(parts[i], 64)
			if err != nil {
				return f, "", errors.New("invalid bbox")
			}
		}
		if box[0] >= box[2] || box[1] >= box[3] || box[0] < -180 || box[2] > 180 || box[1] < -90 || box[3] > 90 {
			return f, "", errors.New("invalid bbox")
		}
		f.BBox = &box
	}
	if v := q.Get("tsunami"); v != "" {
		x, e := strconv.ParseBool(v)
		if e != nil {
			return f, "", errors.New("invalid tsunami filter")
		}
		f.Tsunami = &x
	}
	f.AlertLevel = strPtr(q.Get("alert_level"))
	f.Status = strPtr(q.Get("status"))
	f.EventType = strPtr(q.Get("event_type"))
	f.Source = strPtr(q.Get("source"))
	if v := q.Get("cursor"); v != "" {
		c, e := decodeCursor(v, f.Sort, s.cursorKey)
		if e != nil {
			return f, "", e
		}
		f.CursorID = &c.ID
		f.CursorOccurredAt = c.OccurredAt
		f.CursorMagnitude = c.Magnitude
	}
	format := q.Get("format")
	if format == "" && strings.Contains(r.Header.Get("Accept"), "application/geo+json") {
		format = "geojson"
	}
	if format != "" && format != "json" && format != "geojson" {
		return f, "", errors.New("invalid format")
	}
	return f, format, nil
}
func (s *Server) details(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		s.problem(w, r, 400, "validation-error", "Validation error", "invalid earthquake id")
		return
	}
	e, err := s.repo.Get(r.Context(), id)
	if errors.Is(err, postgres.ErrNotFound) {
		s.problem(w, r, 404, "not-found", "Not found", "earthquake not found")
		return
	}
	if err != nil {
		s.internal(w, r, err)
		return
	}
	writeJSON(w, 200, map[string]any{"data": e})
}
func (s *Server) revisions(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		s.problem(w, r, 400, "validation-error", "Validation error", "invalid earthquake id")
		return
	}
	items, err := s.repo.Revisions(r.Context(), id, r.URL.Query().Get("include_raw") == "true")
	if err != nil {
		s.internal(w, r, err)
		return
	}
	writeJSON(w, 200, map[string]any{"data": items})
}

func (s *Server) createSubscription(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Name               string   `json:"name"`
		WebhookURL         string   `json:"webhook_url"`
		WebhookSecret      *string  `json:"webhook_secret"`
		MinimumMagnitude   *float64 `json:"minimum_magnitude"`
		MaximumMagnitude   *float64 `json:"maximum_magnitude"`
		CenterLatitude     *float64 `json:"center_latitude"`
		CenterLongitude    *float64 `json:"center_longitude"`
		RadiusKM           *float64 `json:"radius_km"`
		TsunamiOnly        bool     `json:"tsunami_only"`
		AllowedAlertLevels []string `json:"allowed_alert_levels"`
		AllowedEventTypes  []string `json:"allowed_event_types"`
		NotifyOnNew        *bool    `json:"notify_on_new"`
		NotifyOnThreshold  *bool    `json:"notify_on_threshold_crossing"`
		NotifyOnTsunami    *bool    `json:"notify_on_tsunami_change"`
		NotifyOnAlert      *bool    `json:"notify_on_alert_increase"`
		MaximumEventAge    string   `json:"maximum_event_age"`
	}
	if err := decodeStrict(w, r, &in); err != nil {
		s.problem(w, r, 400, "validation-error", "Validation error", err.Error())
		return
	}
	rawSecret := make([]byte, 32)
	if _, err := rand.Read(rawSecret); err != nil {
		s.internal(w, r, err)
		return
	}
	secret := base64.RawURLEncoding.EncodeToString(rawSecret)
	if in.WebhookSecret != nil {
		secret = *in.WebhookSecret
	}
	encrypted, err := s.cipher.Encrypt([]byte(secret))
	if err != nil {
		s.internal(w, r, err)
		return
	}
	age := 2 * time.Hour
	if in.MaximumEventAge != "" {
		age, err = time.ParseDuration(in.MaximumEventAge)
		if err != nil || age <= 0 {
			s.problem(w, r, 400, "validation-error", "Validation error", "invalid maximum_event_age")
			return
		}
	}
	sub := domainnotification.Subscription{Name: in.Name, Status: "active", Channel: "webhook", WebhookURL: in.WebhookURL, EncryptedWebhookSecret: encrypted,
		MinimumMagnitude: in.MinimumMagnitude, MaximumMagnitude: in.MaximumMagnitude, CenterLatitude: in.CenterLatitude, CenterLongitude: in.CenterLongitude,
		RadiusKM: in.RadiusKM, TsunamiOnly: in.TsunamiOnly, AllowedAlertLevels: in.AllowedAlertLevels, AllowedEventTypes: in.AllowedEventTypes,
		NotifyOnNew: valueOr(in.NotifyOnNew, true), NotifyOnThresholdCrossing: valueOr(in.NotifyOnThreshold, true), NotifyOnTsunamiChange: valueOr(in.NotifyOnTsunami, true),
		NotifyOnAlertIncrease: valueOr(in.NotifyOnAlert, true), MaximumEventAge: age}
	if err := sub.Validate(s.maxRadius, s.production); err != nil {
		s.problem(w, r, 400, "validation-error", "Validation error", err.Error())
		return
	}
	sub, err = s.repo.CreateSubscription(r.Context(), sub, s.clock.Now())
	if err != nil {
		s.internal(w, r, err)
		return
	}
	writeSubscription(w, 201, sub, &secret)
}
func (s *Server) listSubscriptions(w http.ResponseWriter, r *http.Request) {
	items, err := s.repo.ListSubscriptions(r.Context())
	if err != nil {
		s.internal(w, r, err)
		return
	}
	out := make([]any, 0, len(items))
	for _, x := range items {
		out = append(out, subscriptionView(x, nil))
	}
	writeJSON(w, 200, map[string]any{"data": out})
}
func (s *Server) getSubscription(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		s.problem(w, r, 400, "validation-error", "Validation error", "invalid id")
		return
	}
	x, err := s.repo.GetSubscription(r.Context(), id)
	if errors.Is(err, postgres.ErrNotFound) {
		s.problem(w, r, 404, "not-found", "Not found", "subscription not found")
		return
	}
	if err != nil {
		s.internal(w, r, err)
		return
	}
	writeSubscription(w, 200, x, nil)
}
func (s *Server) patchSubscription(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		s.problem(w, r, 400, "validation-error", "Validation error", "invalid id")
		return
	}
	current, err := s.repo.GetSubscription(r.Context(), id)
	if errors.Is(err, postgres.ErrNotFound) {
		s.problem(w, r, 404, "not-found", "Not found", "subscription not found")
		return
	}
	if err != nil {
		s.internal(w, r, err)
		return
	}
	var in struct {
		Name               *string   `json:"name"`
		Status             *string   `json:"status"`
		WebhookURL         *string   `json:"webhook_url"`
		MinimumMagnitude   *float64  `json:"minimum_magnitude"`
		MaximumMagnitude   *float64  `json:"maximum_magnitude"`
		CenterLatitude     *float64  `json:"center_latitude"`
		CenterLongitude    *float64  `json:"center_longitude"`
		RadiusKM           *float64  `json:"radius_km"`
		TsunamiOnly        *bool     `json:"tsunami_only"`
		AllowedAlertLevels *[]string `json:"allowed_alert_levels"`
		AllowedEventTypes  *[]string `json:"allowed_event_types"`
		NotifyOnNew        *bool     `json:"notify_on_new"`
		NotifyOnThreshold  *bool     `json:"notify_on_threshold_crossing"`
		NotifyOnTsunami    *bool     `json:"notify_on_tsunami_change"`
		NotifyOnAlert      *bool     `json:"notify_on_alert_increase"`
		MaximumEventAge    *string   `json:"maximum_event_age"`
	}
	if err := decodeStrict(w, r, &in); err != nil {
		s.problem(w, r, 400, "validation-error", "Validation error", err.Error())
		return
	}
	if in.Name != nil {
		current.Name = *in.Name
	}
	if in.Status != nil {
		current.Status = *in.Status
	}
	if in.WebhookURL != nil {
		current.WebhookURL = *in.WebhookURL
	}
	if in.MinimumMagnitude != nil {
		current.MinimumMagnitude = in.MinimumMagnitude
	}
	if in.MaximumMagnitude != nil {
		current.MaximumMagnitude = in.MaximumMagnitude
	}
	if in.CenterLatitude != nil {
		current.CenterLatitude = in.CenterLatitude
	}
	if in.CenterLongitude != nil {
		current.CenterLongitude = in.CenterLongitude
	}
	if in.RadiusKM != nil {
		current.RadiusKM = in.RadiusKM
	}
	if in.TsunamiOnly != nil {
		current.TsunamiOnly = *in.TsunamiOnly
	}
	if in.AllowedAlertLevels != nil {
		current.AllowedAlertLevels = *in.AllowedAlertLevels
	}
	if in.AllowedEventTypes != nil {
		current.AllowedEventTypes = *in.AllowedEventTypes
	}
	if in.NotifyOnNew != nil {
		current.NotifyOnNew = *in.NotifyOnNew
	}
	if in.NotifyOnThreshold != nil {
		current.NotifyOnThresholdCrossing = *in.NotifyOnThreshold
	}
	if in.NotifyOnTsunami != nil {
		current.NotifyOnTsunamiChange = *in.NotifyOnTsunami
	}
	if in.NotifyOnAlert != nil {
		current.NotifyOnAlertIncrease = *in.NotifyOnAlert
	}
	if in.MaximumEventAge != nil {
		current.MaximumEventAge, err = time.ParseDuration(*in.MaximumEventAge)
		if err != nil {
			s.problem(w, r, 400, "validation-error", "Validation error", "invalid maximum_event_age")
			return
		}
	}
	if current.Status != "active" && current.Status != "paused" && current.Status != "disabled" {
		s.problem(w, r, 400, "validation-error", "Validation error", "invalid status")
		return
	}
	if err := current.Validate(s.maxRadius, s.production); err != nil {
		s.problem(w, r, 400, "validation-error", "Validation error", err.Error())
		return
	}
	current, err = s.repo.UpdateSubscription(r.Context(), current, s.clock.Now())
	if err != nil {
		s.internal(w, r, err)
		return
	}
	writeSubscription(w, 200, current, nil)
}
func (s *Server) deleteSubscription(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		s.problem(w, r, 400, "validation-error", "Validation error", "invalid id")
		return
	}
	if err := s.repo.DisableSubscription(r.Context(), id, s.clock.Now()); err != nil {
		s.problem(w, r, 404, "not-found", "Not found", "subscription not found")
		return
	}
	w.WriteHeader(204)
}
func (s *Server) listDeliveries(w http.ResponseWriter, r *http.Request) {
	rows, err := s.repo.Pool.Query(r.Context(), `SELECT id,subscription_id,earthquake_id,earthquake_version,trigger_type,status,attempt_count,next_attempt_at,sent_at,response_status,last_error,created_at,updated_at FROM notification_deliveries ORDER BY created_at DESC LIMIT 200`)
	if err != nil {
		s.internal(w, r, err)
		return
	}
	defer rows.Close()
	data := []map[string]any{}
	for rows.Next() {
		var id, sid, eid uuid.UUID
		var version int64
		var trigger, status string
		var attempts int
		var next, created, updated time.Time
		var sent *time.Time
		var code *int
		var last *string
		if err := rows.Scan(&id, &sid, &eid, &version, &trigger, &status, &attempts, &next, &sent, &code, &last, &created, &updated); err != nil {
			s.internal(w, r, err)
			return
		}
		data = append(data, map[string]any{"id": id, "subscription_id": sid, "earthquake_id": eid, "earthquake_version": version, "trigger_type": trigger, "status": status, "attempt_count": attempts, "next_attempt_at": next, "sent_at": sent, "response_status": code, "last_error": last, "created_at": created, "updated_at": updated})
	}
	writeJSON(w, 200, map[string]any{"data": data})
}
func (s *Server) getDelivery(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		s.problem(w, r, 400, "validation-error", "Validation error", "invalid id")
		return
	}
	row := s.repo.Pool.QueryRow(r.Context(), `SELECT jsonb_build_object('id',id,'subscription_id',subscription_id,'earthquake_id',earthquake_id,'earthquake_version',earthquake_version,'trigger_type',trigger_type,'status',status,'attempt_count',attempt_count,'next_attempt_at',next_attempt_at,'sent_at',sent_at,'response_status',response_status,'last_error',last_error,'created_at',created_at,'updated_at',updated_at) FROM notification_deliveries WHERE id=$1`, id)
	var data any
	if err := row.Scan(&data); err != nil {
		s.problem(w, r, 404, "not-found", "Not found", "delivery not found")
		return
	}
	writeJSON(w, 200, map[string]any{"data": data})
}
func (s *Server) retryDelivery(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		s.problem(w, r, 400, "validation-error", "Validation error", "invalid id")
		return
	}
	if err := s.repo.RetryDelivery(r.Context(), id, s.clock.Now()); err != nil {
		s.problem(w, r, 409, "retry-not-allowed", "Retry not allowed", "delivery is sent, processing, or unknown")
		return
	}
	writeJSON(w, 202, map[string]any{"id": id, "status": "retry"})
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.repo.Ready(ctx); err != nil {
		s.problem(w, r, 503, "not-ready", "Not ready", err.Error())
		return
	}
	writeJSON(w, 200, map[string]string{"status": "ready"})
}
func (s *Server) adminAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		given := r.Header.Get("Authorization")
		given = strings.TrimPrefix(given, "Bearer ")
		if subtle.ConstantTimeCompare([]byte(given), []byte(s.adminKey)) != 1 {
			s.problem(w, r, 401, "unauthorized", "Unauthorized", "valid administrative API key required")
			return
		}
		next.ServeHTTP(w, r)
	})
}

type ctxKey string

const requestIDKey ctxKey = "request_id"

func (s *Server) requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" || len(id) > 128 {
			b := make([]byte, 16)
			if _, err := rand.Read(b); err != nil {
				s.internal(w, r, err)
				return
			}
			id = hex.EncodeToString(b)
		}
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey, id)))
	})
}
func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				s.log.Error("panic recovered", "error", fmt.Sprint(v), "request_id", requestID(r))
				s.problem(w, r, 500, "internal-error", "Internal server error", "unexpected server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *statusWriter) WriteHeader(v int)           { w.status = v; w.ResponseWriter.WriteHeader(v) }
func (s *Server) accessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sw := &statusWriter{ResponseWriter: w, status: 200}
		start := time.Now()
		next.ServeHTTP(sw, r)
		route := chi.RouteContext(r.Context()).RoutePattern()
		if route == "" {
			route = "unmatched"
		}
		s.metrics.HTTPRequests.WithLabelValues(r.Method, route, strconv.Itoa(sw.status)).Inc()
		s.metrics.HTTPDuration.WithLabelValues(r.Method, route).Observe(time.Since(start).Seconds())
		s.log.Info("http request", "method", r.Method, "route", route, "status", sw.status, "duration_ms", time.Since(start).Milliseconds(), "request_id", requestID(r))
	})
}
func (s *Server) problem(w http.ResponseWriter, r *http.Request, status int, kind, title, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	writeJSON(w, status, map[string]any{"type": "https://errors.example.invalid/" + kind, "title": title, "status": status, "detail": detail, "instance": r.URL.Path, "request_id": requestID(r)})
}
func (s *Server) internal(w http.ResponseWriter, r *http.Request, err error) {
	s.log.Error("request failed", "error", err, "request_id", requestID(r))
	s.problem(w, r, 500, "internal-error", "Internal server error", "unexpected server error")
}
func requestID(r *http.Request) string { v, _ := r.Context().Value(requestIDKey).(string); return v }
func writeJSON(w http.ResponseWriter, status int, v any) {
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/json")
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func decodeStrict(w http.ResponseWriter, r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return errors.New("request body must contain one JSON value")
	}
	return nil
}
func parseFloatPtr(v string) (*float64, error) {
	if v == "" {
		return nil, nil
	}
	x, e := strconv.ParseFloat(v, 64)
	if e != nil {
		return nil, e
	}
	return &x, nil
}
func parseTimePtr(v string) (*time.Time, error) {
	if v == "" {
		return nil, nil
	}
	x, e := time.Parse(time.RFC3339, v)
	if e != nil {
		return nil, e
	}
	return &x, nil
}
func strPtr(v string) *string {
	if v == "" {
		return nil
	}
	return &v
}
func valueOr(v *bool, d bool) bool {
	if v == nil {
		return d
	}
	return *v
}
func writeSubscription(w http.ResponseWriter, status int, x domainnotification.Subscription, secret *string) {
	writeJSON(w, status, map[string]any{"data": subscriptionView(x, secret)})
}
func subscriptionView(x domainnotification.Subscription, secret *string) map[string]any {
	var webhookURL any
	if x.Channel == "webhook" {
		webhookURL = x.WebhookURL
	}
	return map[string]any{"id": x.ID, "name": x.Name, "status": x.Status, "channel": x.Channel, "subscription_kind": x.SubscriptionKind, "webhook_url": webhookURL, "webhook_secret": secret, "telegram_chat_id": x.TelegramChatID, "telegram_chat_username": x.TelegramChatUsername, "notification_language": x.NotificationLanguage, "minimum_magnitude": x.MinimumMagnitude, "maximum_magnitude": x.MaximumMagnitude, "minimum_intensity": x.MinimumIntensity, "center_latitude": x.CenterLatitude, "center_longitude": x.CenterLongitude, "radius_km": x.RadiusKM, "tsunami_only": x.TsunamiOnly, "allowed_alert_levels": x.AllowedAlertLevels, "allowed_event_types": x.AllowedEventTypes, "notify_on_new": x.NotifyOnNew, "notify_on_threshold_crossing": x.NotifyOnThresholdCrossing, "notify_on_tsunami_change": x.NotifyOnTsunamiChange, "notify_on_alert_increase": x.NotifyOnAlertIncrease, "maximum_event_age": x.MaximumEventAge.String(), "created_at": x.CreatedAt, "updated_at": x.UpdatedAt}
}

type rateLimiter struct {
	mu      sync.Mutex
	entries map[string]*rateEntry
	limit   int
	window  time.Duration
}
type rateEntry struct {
	start time.Time
	count int
}

func newRateLimiter(limit int, window time.Duration) *rateLimiter {
	return &rateLimiter{entries: map[string]*rateEntry{}, limit: limit, window: window}
}
func (l *rateLimiter) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, _ := net.SplitHostPort(r.RemoteAddr)
		now := time.Now()
		l.mu.Lock()
		e := l.entries[host]
		if e == nil || now.Sub(e.start) > l.window {
			e = &rateEntry{start: now}
			l.entries[host] = e
		}
		e.count++
		allowed := e.count <= l.limit
		l.mu.Unlock()
		if !allowed {
			w.Header().Set("Retry-After", "60")
			writeJSON(w, 429, map[string]any{"type": "https://errors.example.invalid/rate-limit", "title": "Too many requests", "status": 429, "detail": "rate limit exceeded", "instance": r.URL.Path})
			return
		}
		next.ServeHTTP(w, r)
	})
}
