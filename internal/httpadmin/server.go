package httpadmin

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/example/earthquake-service/internal/administration"
	"github.com/example/earthquake-service/internal/domain/notification"
)

//go:embed assets/* templates/*
var adminFiles embed.FS

type Config struct {
	Host, TeamDomain, Audience, DevelopmentEmail, GrafanaBaseURL string
	CSRFKey                                                      []byte
	HTTPClient                                                   *http.Client
	Now                                                          func() time.Time
}

type Server struct {
	service              *administration.Service
	log                  *slog.Logger
	host, grafanaBaseURL string
	csrfKey              []byte
	now                  func() time.Time
	auth                 authenticator
	templates            map[string]*template.Template
}

type identityContextKey struct{}

type pageData struct {
	Title            string
	Description      string
	Section          string
	ContextID        string
	Identity         administration.Identity
	Incidents        any
	Incident         *administration.IncidentDetail
	Subscriptions    any
	Subscription     *notification.Subscription
	Notifications    any
	Notification     *administration.NotificationItem
	NotificationType string
	Audit            any
	NextCursor       string
	Filters          administration.IncidentFilter
	GrafanaURL       string
}

func New(service *administration.Service, log *slog.Logger, config Config) (http.Handler, error) {
	if service == nil || log == nil || config.Host == "" || len(config.CSRFKey) < 16 {
		return nil, errors.New("invalid administration server configuration")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.HTTPClient == nil {
		config.HTTPClient = &http.Client{Timeout: 5 * time.Second}
	}
	server := &Server{service: service, log: log, host: config.Host, csrfKey: config.CSRFKey,
		now: config.Now, grafanaBaseURL: strings.TrimRight(config.GrafanaBaseURL, "/")}
	if config.DevelopmentEmail != "" {
		server.auth = developmentAuthenticator{email: config.DevelopmentEmail}
	} else {
		server.auth = newCloudflareAuthenticator(config.TeamDomain, config.Audience, config.HTTPClient, config.Now)
	}
	if err := server.parseTemplates(); err != nil {
		return nil, err
	}
	tablerCSS, err := adminFiles.ReadFile("assets/tabler.min.css")
	if err != nil {
		return nil, fmt.Errorf("read embedded Tabler stylesheet: %w", err)
	}
	shakerCSS, err := adminFiles.ReadFile("assets/style.css")
	if err != nil {
		return nil, fmt.Errorf("read embedded Shaker stylesheet: %w", err)
	}

	router := chi.NewRouter()
	router.Use(server.securityHeaders, server.requireHost, server.requestTimeout, server.authenticate, server.requireCSRF)
	router.Get("/assets/tabler.min.css", serveStylesheet(tablerCSS))
	router.Get("/assets/style.css", serveStylesheet(shakerCSS))
	router.Get("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/admin/incidents", http.StatusSeeOther)
	})
	router.Get("/incidents", server.incidents)
	router.Get("/incidents/{id}", server.incident)
	router.Get("/subscriptions", server.subscriptions)
	router.Get("/subscriptions/{id}", server.subscription)
	router.Get("/notifications", server.notifications)
	router.Get("/notifications/{id}", server.notification)
	router.With(server.requireOwner).Get("/audit", server.audit)
	return router, nil
}

func serveStylesheet(content []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
		http.ServeContent(w, r, "stylesheet.css", time.Time{}, bytes.NewReader(content))
	}
}

func (s *Server) parseTemplates() error {
	functions := template.FuncMap{
		"time": func(value time.Time) string {
			if value.IsZero() {
				return "—"
			}
			return value.UTC().Format("2006-01-02 15:04:05 UTC")
		},
		"number": func(value *float64) string {
			if value == nil {
				return "—"
			}
			return strconv.FormatFloat(*value, 'f', 1, 64)
		},
		"inputNumber": func(value *float64) string {
			if value == nil {
				return ""
			}
			return strconv.FormatFloat(*value, 'f', -1, 64)
		},
		"text": func(value *string) string {
			if value == nil || *value == "" {
				return "—"
			}
			return *value
		},
		"bool": func(value bool) string {
			if value {
				return "yes"
			}
			return "no"
		},
		"json": func(value []byte) string {
			if len(value) == 0 {
				return "{}"
			}
			var output bytes.Buffer
			if json.Indent(&output, value, "", "  ") == nil {
				return output.String()
			}
			return string(value)
		},
		"chat":             maskChatID,
		"username":         maskUsername,
		"coordinate":       maskCoordinate,
		"webhook":          maskWebhook,
		"subscriptionName": displaySubscriptionName,
		"statusClass":      statusClass,
		"short": func(value fmt.Stringer) string {
			text := value.String()
			if len(text) > 12 {
				return text[:12]
			}
			return text
		},
	}
	s.templates = make(map[string]*template.Template)
	for _, page := range []string{"incidents", "incident", "subscriptions", "subscription", "notifications", "notification", "audit"} {
		parsed, err := template.New("layout.html").Funcs(functions).ParseFS(adminFiles, "templates/layout.html", "templates/"+page+".html")
		if err != nil {
			return fmt.Errorf("parse admin template %s: %w", page, err)
		}
		s.templates[page] = parsed
	}
	return nil
}

func (s *Server) incidents(w http.ResponseWriter, r *http.Request) {
	filter := administration.IncidentFilter{Provider: r.URL.Query().Get("provider"), Lifecycle: r.URL.Query().Get("lifecycle"),
		Status: r.URL.Query().Get("status"), Limit: 50}
	if value := r.URL.Query().Get("min_magnitude"); value != "" {
		parsed, err := strconv.ParseFloat(value, 64)
		if err != nil {
			s.renderError(w, http.StatusBadRequest, "Invalid minimum magnitude")
			return
		}
		filter.MinMagnitude = &parsed
	}
	if value := r.URL.Query().Get("cursor"); value != "" {
		occurredAt, id, err := s.decodeCursor(value, "incidents")
		if err != nil {
			s.renderError(w, http.StatusBadRequest, "Invalid page cursor")
			return
		}
		filter.BeforeAt, filter.BeforeID = &occurredAt, &id
	}
	items, err := s.service.Incidents(r.Context(), filter)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	data := s.page(r, "Incidents")
	data.Incidents, data.Filters = items, filter
	if len(items) == filter.Limit {
		data.NextCursor = s.encodeCursor("incidents", items[len(items)-1].OccurredAt, items[len(items)-1].ID)
	}
	s.render(w, "incidents", data)
}

func (s *Server) incident(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, chi.URLParam(r, "id"))
	if !ok {
		return
	}
	item, err := s.service.Incident(r.Context(), id)
	if errors.Is(err, administration.ErrNotFound) {
		s.renderError(w, http.StatusNotFound, "Incident not found")
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	data := s.page(r, "Incident details")
	data.ContextID = id.String()
	data.Incident = &item
	data.GrafanaURL = s.grafanaLink("earthquake_id", id.String())
	s.render(w, "incident", data)
}

func (s *Server) subscriptions(w http.ResponseWriter, r *http.Request) {
	filter, ok := s.pageFilter(w, r, "subscriptions")
	if !ok {
		return
	}
	items, err := s.service.Subscriptions(r.Context(), filter)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	data := s.page(r, "Subscriptions")
	data.Subscriptions = items
	if len(items) == filter.Limit {
		data.NextCursor = s.encodeCursor("subscriptions", items[len(items)-1].CreatedAt, items[len(items)-1].ID)
	}
	s.render(w, "subscriptions", data)
}

func (s *Server) subscription(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, chi.URLParam(r, "id"))
	if !ok {
		return
	}
	item, err := s.service.Subscription(r.Context(), id)
	if errors.Is(err, administration.ErrNotFound) {
		s.renderError(w, http.StatusNotFound, "Subscription not found")
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	data := s.page(r, "Subscription details")
	data.ContextID = id.String()
	data.Subscription = &item
	data.GrafanaURL = s.grafanaLink("subscription_id", id.String())
	s.render(w, "subscription", data)
}

func (s *Server) notifications(w http.ResponseWriter, r *http.Request) {
	deliveryClass := r.URL.Query().Get("type")
	if deliveryClass == "" {
		deliveryClass = "telegram_private"
	}
	if deliveryClass != "telegram_channel" && deliveryClass != "telegram_private" && deliveryClass != "webhook" {
		s.renderError(w, http.StatusBadRequest, "Invalid notification type")
		return
	}
	filter, ok := s.pageFilter(w, r, "notifications:"+deliveryClass)
	if !ok {
		return
	}
	items, err := s.service.Notifications(r.Context(), administration.NotificationFilter{
		PageFilter: filter, DeliveryClass: deliveryClass,
	})
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	data := s.page(r, "Notifications")
	data.Notifications = items
	data.NotificationType = deliveryClass
	if len(items) == filter.Limit {
		data.NextCursor = s.encodeCursor("notifications", items[len(items)-1].CreatedAt, items[len(items)-1].ID)
	}
	s.render(w, "notifications", data)
}

func (s *Server) notification(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, chi.URLParam(r, "id"))
	if !ok {
		return
	}
	item, err := s.service.Notification(r.Context(), id)
	if errors.Is(err, administration.ErrNotFound) {
		s.renderError(w, http.StatusNotFound, "Notification not found")
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	data := s.page(r, "Notification details")
	data.ContextID = id.String()
	data.Notification = &item
	data.GrafanaURL = s.grafanaLink("notification_id", id.String())
	s.render(w, "notification", data)
}

func (s *Server) audit(w http.ResponseWriter, r *http.Request) {
	filter, ok := s.pageFilter(w, r, "audit")
	if !ok {
		return
	}
	items, err := s.service.Audit(r.Context(), filter)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	data := s.page(r, "Audit log")
	data.Audit = items
	if len(items) == filter.Limit {
		data.NextCursor = s.encodeCursor("audit", items[len(items)-1].CreatedAt, items[len(items)-1].ID)
	}
	s.render(w, "audit", data)
}

func (s *Server) page(r *http.Request, title string) pageData {
	data := pageData{Title: title, Identity: identityFromContext(r.Context())}
	switch {
	case strings.HasPrefix(r.URL.Path, "/incidents"):
		data.Section = "incidents"
		data.Description = "Canonical seismic incidents, provider evidence, and shaking evaluations."
	case strings.HasPrefix(r.URL.Path, "/subscriptions"):
		data.Section = "subscriptions"
		data.Description = "Notification audiences, thresholds, language, and delivery configuration."
	case strings.HasPrefix(r.URL.Path, "/notifications"):
		data.Section = "notifications"
		data.Description = "Durable webhook deliveries and Telegram message projections."
	case strings.HasPrefix(r.URL.Path, "/audit"):
		data.Section = "audit"
		data.Description = "Immutable history of privileged administrative operations."
	}
	return data
}

func (s *Server) render(w http.ResponseWriter, name string, data pageData) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.templates[name].ExecuteTemplate(w, "layout", data); err != nil {
		s.log.Error("render admin page", "page", name, "error", err)
	}
}

func (s *Server) renderError(w http.ResponseWriter, status int, message string) {
	http.Error(w, message, status)
}

func (s *Server) internalError(w http.ResponseWriter, r *http.Request, err error) {
	s.log.Error("admin request failed", "path", r.URL.Path, "error", err)
	s.renderError(w, http.StatusInternalServerError, "Internal server error")
}

func parseID(w http.ResponseWriter, value string) (uuid.UUID, bool) {
	id, err := uuid.Parse(value)
	if err != nil {
		http.Error(w, "Invalid identifier", http.StatusBadRequest)
		return uuid.Nil, false
	}
	return id, true
}

func (s *Server) grafanaLink(label, value string) string {
	if s.grafanaBaseURL == "" {
		return ""
	}
	query := url.Values{"var-filter": []string{label + "=" + value}}
	return s.grafanaBaseURL + "/explore?" + query.Encode()
}

func maskChatID(value *int64) string {
	if value == nil {
		return "—"
	}
	text := strconv.FormatInt(*value, 10)
	if len(text) <= 4 {
		return "••••"
	}
	return strings.Repeat("•", len(text)-4) + text[len(text)-4:]
}

func maskCoordinate(value *float64) string {
	if value == nil {
		return "—"
	}
	return "hidden (" + strconv.FormatFloat(*value, 'f', 0, 64) + "° area)"
}

func maskWebhook(value string) string {
	if value == "" {
		return "—"
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" {
		return "configured"
	}
	return parsed.Scheme + "://" + parsed.Host + "/…"
}

func maskUsername(value *string) string {
	if value == nil || *value == "" {
		return "—"
	}
	text := strings.TrimPrefix(*value, "@")
	if len(text) <= 2 {
		return "@••"
	}
	return "@••••" + text[len(text)-2:]
}

func displaySubscriptionName(name, channel, kind string, chatID *int64) string {
	if channel != "telegram" {
		return name
	}
	if kind == "global_channel" {
		return "Global Telegram channel"
	}
	return "Telegram user " + maskChatID(chatID)
}

func statusClass(value any) string {
	normalized := strings.ToLower(fmt.Sprint(value))
	switch normalized {
	case "confirmed", "reviewed", "active", "sent", "notify", "owner":
		return "success"
	case "preliminary", "pending", "pending_send", "pending_edit", "processing", "retry", "paused", "operator", "refresh", "below_threshold":
		return "warning"
	case "retracted", "dead", "disabled":
		return "danger"
	case "telegram_alert", "telegram", "emsc", "viewer":
		return "violet"
	case "webhook_delivery", "webhook", "usgs":
		return "blue"
	default:
		return "neutral"
	}
}

func (s *Server) securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", "default-src 'none'; style-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'self'; frame-ancestors 'none'")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}

func (s *Server) requireHost(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.EqualFold(stripPort(r.Host), stripPort(s.host)) {
			http.NotFound(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func stripPort(value string) string {
	if index := strings.LastIndex(value, ":"); index > -1 && !strings.Contains(value[index+1:], "]") {
		return strings.Trim(value[:index], "[]")
	}
	return strings.Trim(value, "[]")
}

func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, err := s.auth.Authenticate(r.Context(), r)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		identity, err := s.service.Authorize(r.Context(), principal.Subject, principal.Email)
		if errors.Is(err, administration.ErrNotFound) {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		if err != nil {
			s.internalError(w, r, err)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), identityContextKey{}, identity)))
	})
}

func (s *Server) requireOwner(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if identityFromContext(r.Context()).Role != administration.Owner {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func identityFromContext(ctx context.Context) administration.Identity {
	value, _ := ctx.Value(identityContextKey{}).(administration.Identity)
	return value
}

func (s *Server) requireCSRF(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet || r.Method == http.MethodHead || r.Method == http.MethodOptions {
			next.ServeHTTP(w, r)
			return
		}
		identity := identityFromContext(r.Context())
		expectedOrigin := "https://" + s.host
		if r.Header.Get("Origin") != expectedOrigin || subtle.ConstantTimeCompare([]byte(r.Header.Get("X-CSRF-Token")), []byte(s.csrfToken(identity.Subject))) != 1 {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) csrfToken(subject string) string {
	mac := hmac.New(sha256.New, s.csrfKey)
	_, _ = mac.Write([]byte(subject + "\x00" + s.now().UTC().Format("2006-01-02")))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

type listCursor struct {
	Scope  string    `json:"scope"`
	SortAt time.Time `json:"sort_at"`
	ID     uuid.UUID `json:"id"`
}

func (s *Server) encodeCursor(scope string, sortAt time.Time, id uuid.UUID) string {
	payload, _ := json.Marshal(listCursor{Scope: scope, SortAt: sortAt, ID: id})
	mac := hmac.New(sha256.New, s.csrfKey)
	_, _ = mac.Write(payload)
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func (s *Server) decodeCursor(value, scope string) (time.Time, uuid.UUID, error) {
	parts := strings.Split(value, ".")
	if len(parts) != 2 {
		return time.Time{}, uuid.Nil, errors.New("invalid cursor")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil || len(payload) > 1024 {
		return time.Time{}, uuid.Nil, errors.New("invalid cursor")
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, uuid.Nil, errors.New("invalid cursor")
	}
	mac := hmac.New(sha256.New, s.csrfKey)
	_, _ = mac.Write(payload)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return time.Time{}, uuid.Nil, errors.New("invalid cursor")
	}
	var cursor listCursor
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cursor); err != nil || cursor.Scope != scope || cursor.SortAt.IsZero() || cursor.ID == uuid.Nil {
		return time.Time{}, uuid.Nil, errors.New("invalid cursor")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return time.Time{}, uuid.Nil, errors.New("invalid cursor")
	}
	return cursor.SortAt, cursor.ID, nil
}

func (s *Server) pageFilter(w http.ResponseWriter, r *http.Request, scope string) (administration.PageFilter, bool) {
	filter := administration.PageFilter{Limit: 50}
	if value := r.URL.Query().Get("cursor"); value != "" {
		sortAt, id, err := s.decodeCursor(value, scope)
		if err != nil {
			s.renderError(w, http.StatusBadRequest, "Invalid page cursor")
			return filter, false
		}
		filter.BeforeAt, filter.BeforeID = &sortAt, &id
	}
	return filter, true
}

func (s *Server) requestTimeout(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
