package config

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Environment, Version, Role, HTTPAddress, MetricsAddress, DatabaseURL string
	DatabaseMaxConnections, DatabaseMinConnections                       int
	USGSRealtimeURL, USGSFDSNURL                                         string
	USGSPollInterval, USGSHTTPTimeout                                    time.Duration
	USGSMaxResponseBytes                                                 int64
	EMSCEnabled                                                          bool
	EMSCWebSocketURL, EMSCFDSNURL                                        string
	EMSCPollInterval, EMSCHTTPTimeout, EMSCLookback                      time.Duration
	EMSCPingInterval                                                     time.Duration
	EMSCMaxResponseBytes, EMSCMaxFrameBytes                              int64
	BackfillChunkDuration, RecoveryOverlapDuration                       time.Duration
	NotificationBatchSize, NotificationMaxAttempts                       int
	NotificationLockTimeout, NotificationPollInterval                    time.Duration
	AdminAPIKey                                                          string
	AdminEnabled                                                         bool
	AdminHost, CloudflareAccessTeamDomain, CloudflareAccessAudience      string
	AdminBootstrapOwners                                                 []string
	AdminDevelopmentEmail, GrafanaBaseURL                                string
	EncryptionKey                                                        []byte
	WebhookAllowPrivate                                                  bool
	WebhookHTTPTimeout                                                   time.Duration
	WebhookMaxResponseBytes                                              int64
	TelegramBotToken, TelegramAPIURL, TelegramGlobalChannel              string
	TelegramPollTimeout                                                  time.Duration
	TelegramMaxResponseBytes                                             int64
	LogLevel, OTELEndpoint                                               string
	MaxSearchRadiusKM                                                    float64
	CursorHMACKey                                                        []byte
}

func Load(roleOverride string) (Config, error) {
	c := Config{
		Environment: env("APP_ENV", "development"), Version: env("APP_VERSION", "dev"),
		Role: env("APP_ROLE", "all"), HTTPAddress: env("HTTP_ADDRESS", ":8080"), MetricsAddress: env("WORKER_METRICS_ADDRESS", ":9090"),
		DatabaseURL: os.Getenv("DATABASE_URL"), USGSRealtimeURL: env("USGS_REALTIME_URL", "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"),
		USGSFDSNURL:      env("USGS_FDSN_URL", "https://earthquake.usgs.gov/fdsnws/event/1/query"),
		EMSCWebSocketURL: env("EMSC_WEBSOCKET_URL", "wss://www.seismicportal.eu/standing_order/websocket"),
		EMSCFDSNURL:      env("EMSC_FDSN_URL", "https://www.seismicportal.eu/fdsnws/event/1/query"),
		AdminAPIKey:      os.Getenv("ADMIN_API_KEY"), LogLevel: env("LOG_LEVEL", "info"), OTELEndpoint: os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		AdminHost: os.Getenv("ADMIN_HOST"), CloudflareAccessTeamDomain: os.Getenv("CLOUDFLARE_ACCESS_TEAM_DOMAIN"),
		CloudflareAccessAudience: os.Getenv("CLOUDFLARE_ACCESS_AUDIENCE"),
		AdminBootstrapOwners:     splitCSV(os.Getenv("ADMIN_BOOTSTRAP_OWNERS")),
		AdminDevelopmentEmail:    os.Getenv("ADMIN_DEVELOPMENT_EMAIL"), GrafanaBaseURL: os.Getenv("GRAFANA_BASE_URL"),
		TelegramBotToken: os.Getenv("TELEGRAM_BOT_TOKEN"), TelegramAPIURL: env("TELEGRAM_API_URL", "https://api.telegram.org"),
		TelegramGlobalChannel: os.Getenv("TELEGRAM_GLOBAL_CHANNEL"),
	}
	if roleOverride != "" {
		c.Role = roleOverride
	}
	var err error
	c.DatabaseMaxConnections, err = intEnv("DATABASE_MAX_CONNECTIONS", 20)
	if err != nil {
		return c, err
	}
	c.DatabaseMinConnections, err = intEnv("DATABASE_MIN_CONNECTIONS", 2)
	if err != nil {
		return c, err
	}
	c.USGSPollInterval, err = durationEnv("USGS_POLL_INTERVAL", time.Minute)
	if err != nil {
		return c, err
	}
	c.USGSHTTPTimeout, err = durationEnv("USGS_HTTP_TIMEOUT", 20*time.Second)
	if err != nil {
		return c, err
	}
	c.USGSMaxResponseBytes, err = int64Env("USGS_MAX_RESPONSE_BYTES", 25<<20)
	if err != nil {
		return c, err
	}
	c.EMSCEnabled, err = boolEnv("EMSC_ENABLED", false)
	if err != nil {
		return c, err
	}
	c.EMSCPollInterval, err = durationEnv("EMSC_POLL_INTERVAL", 30*time.Second)
	if err != nil {
		return c, err
	}
	c.EMSCHTTPTimeout, err = durationEnv("EMSC_HTTP_TIMEOUT", 20*time.Second)
	if err != nil {
		return c, err
	}
	c.EMSCLookback, err = durationEnv("EMSC_LOOKBACK_DURATION", 2*time.Hour)
	if err != nil {
		return c, err
	}
	c.EMSCPingInterval, err = durationEnv("EMSC_PING_INTERVAL", 15*time.Second)
	if err != nil {
		return c, err
	}
	c.EMSCMaxResponseBytes, err = int64Env("EMSC_MAX_RESPONSE_BYTES", 25<<20)
	if err != nil {
		return c, err
	}
	c.EMSCMaxFrameBytes, err = int64Env("EMSC_MAX_FRAME_BYTES", 256<<10)
	if err != nil {
		return c, err
	}
	c.BackfillChunkDuration, err = durationEnv("BACKFILL_CHUNK_DURATION", 24*time.Hour)
	if err != nil {
		return c, err
	}
	c.RecoveryOverlapDuration, err = durationEnv("RECOVERY_OVERLAP_DURATION", time.Hour)
	if err != nil {
		return c, err
	}
	c.NotificationBatchSize, err = intEnv("NOTIFICATION_BATCH_SIZE", 50)
	if err != nil {
		return c, err
	}
	c.NotificationMaxAttempts, err = intEnv("NOTIFICATION_MAX_ATTEMPTS", 10)
	if err != nil {
		return c, err
	}
	c.NotificationLockTimeout, err = durationEnv("NOTIFICATION_LOCK_TIMEOUT", 5*time.Minute)
	if err != nil {
		return c, err
	}
	c.NotificationPollInterval, err = durationEnv("NOTIFICATION_POLL_INTERVAL", time.Second)
	if err != nil {
		return c, err
	}
	c.AdminEnabled, err = boolEnv("ADMIN_ENABLED", false)
	if err != nil {
		return c, err
	}
	c.WebhookHTTPTimeout, err = durationEnv("WEBHOOK_HTTP_TIMEOUT", 15*time.Second)
	if err != nil {
		return c, err
	}
	c.WebhookMaxResponseBytes, err = int64Env("WEBHOOK_MAX_RESPONSE_BYTES", 64<<10)
	if err != nil {
		return c, err
	}
	c.TelegramPollTimeout, err = durationEnv("TELEGRAM_POLL_TIMEOUT", 25*time.Second)
	if err != nil {
		return c, err
	}
	c.TelegramMaxResponseBytes, err = int64Env("TELEGRAM_MAX_RESPONSE_BYTES", 1<<20)
	if err != nil {
		return c, err
	}
	c.WebhookAllowPrivate, err = boolEnv("WEBHOOK_ALLOW_PRIVATE_NETWORKS", c.Environment == "development")
	if err != nil {
		return c, err
	}
	c.MaxSearchRadiusKM = 2000
	c.EncryptionKey, err = keyEnv("SECRETS_ENCRYPTION_KEY")
	if err != nil {
		return c, err
	}
	c.CursorHMACKey = c.EncryptionKey
	if c.DatabaseURL == "" {
		return c, errors.New("DATABASE_URL is required")
	}
	if c.Role != "api" && c.Role != "worker" && c.Role != "all" && c.Role != "backfill" {
		return c, fmt.Errorf("invalid APP_ROLE %q", c.Role)
	}
	if c.AdminAPIKey == "" {
		return c, errors.New("ADMIN_API_KEY is required")
	}
	if c.AdminEnabled {
		if c.AdminHost == "" || strings.Contains(c.AdminHost, "://") || strings.ContainsAny(c.AdminHost, " /\t\r\n") {
			return c, errors.New("ADMIN_HOST must be a hostname without a URL scheme")
		}
		if c.Environment == "development" && c.AdminDevelopmentEmail != "" {
			if len(c.AdminBootstrapOwners) == 0 {
				return c, errors.New("ADMIN_BOOTSTRAP_OWNERS is required when administration is enabled")
			}
		} else if c.CloudflareAccessTeamDomain == "" || c.CloudflareAccessAudience == "" {
			return c, errors.New("cloudflare access team domain and audience are required when administration is enabled")
		}
		if c.AdminDevelopmentEmail == "" {
			if err := validateCloudflareTeamDomain(c.CloudflareAccessTeamDomain); err != nil {
				return c, err
			}
		}
		if c.Environment != "development" && c.AdminDevelopmentEmail != "" {
			return c, errors.New("ADMIN_DEVELOPMENT_EMAIL is development-only")
		}
		if len(c.AdminBootstrapOwners) == 0 {
			return c, errors.New("ADMIN_BOOTSTRAP_OWNERS is required when administration is enabled")
		}
	}
	if len(c.EncryptionKey) != 32 {
		return c, errors.New("SECRETS_ENCRYPTION_KEY must be base64 encoding of exactly 32 bytes")
	}
	if c.Environment != "development" && c.WebhookAllowPrivate {
		return c, errors.New("WEBHOOK_ALLOW_PRIVATE_NETWORKS is development-only")
	}
	if c.TelegramGlobalChannel != "" && c.TelegramBotToken == "" {
		return c, errors.New("TELEGRAM_BOT_TOKEN is required when TELEGRAM_GLOBAL_CHANNEL is configured")
	}
	if c.TelegramGlobalChannel != "" && (!strings.HasPrefix(c.TelegramGlobalChannel, "@") || len(c.TelegramGlobalChannel) > 64 || strings.ContainsAny(c.TelegramGlobalChannel, " \t\r\n")) {
		return c, errors.New("TELEGRAM_GLOBAL_CHANNEL must be a channel username starting with @")
	}
	if c.EMSCEnabled {
		if err := validateEndpoint("EMSC_WEBSOCKET_URL", c.EMSCWebSocketURL, "ws", "wss"); err != nil {
			return c, err
		}
		if err := validateEndpoint("EMSC_FDSN_URL", c.EMSCFDSNURL, "http", "https"); err != nil {
			return c, err
		}
	}
	return c, nil
}

func validateEndpoint(name, value string, schemes ...string) error {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" {
		return fmt.Errorf("%s must be an absolute URL", name)
	}
	for _, scheme := range schemes {
		if parsed.Scheme == scheme {
			return nil
		}
	}
	return fmt.Errorf("%s has unsupported URL scheme %q", name, parsed.Scheme)
}

func validateCloudflareTeamDomain(value string) error {
	if !strings.Contains(value, "://") {
		value = "https://" + value
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return errors.New("CLOUDFLARE_ACCESS_TEAM_DOMAIN must be an HTTPS cloudflareaccess.com team domain")
	}
	hostname := strings.ToLower(parsed.Hostname())
	if parsed.Scheme != "https" || parsed.User != nil || parsed.Port() != "" ||
		parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" ||
		!strings.HasSuffix(hostname, ".cloudflareaccess.com") {
		return errors.New("CLOUDFLARE_ACCESS_TEAM_DOMAIN must be an HTTPS cloudflareaccess.com team domain")
	}
	return nil
}

func env(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}
func durationEnv(k string, d time.Duration) (time.Duration, error) {
	v := env(k, d.String())
	x, e := time.ParseDuration(v)
	if e != nil || x <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", k)
	}
	return x, nil
}
func intEnv(k string, d int) (int, error) {
	v, e := strconv.Atoi(env(k, strconv.Itoa(d)))
	if e != nil || v <= 0 {
		return 0, fmt.Errorf("%s must be positive", k)
	}
	return v, nil
}
func int64Env(k string, d int64) (int64, error) {
	v, e := strconv.ParseInt(env(k, strconv.FormatInt(d, 10)), 10, 64)
	if e != nil || v <= 0 {
		return 0, fmt.Errorf("%s must be positive", k)
	}
	return v, nil
}
func boolEnv(k string, d bool) (bool, error) {
	v, e := strconv.ParseBool(env(k, strconv.FormatBool(d)))
	if e != nil {
		return false, fmt.Errorf("%s must be boolean", k)
	}
	return v, nil
}
func keyEnv(k string) ([]byte, error) {
	v := os.Getenv(k)
	if v == "" {
		return nil, fmt.Errorf("%s is required", k)
	}
	b, e := base64.StdEncoding.DecodeString(v)
	if e != nil {
		return nil, fmt.Errorf("%s must be base64: %w", k, e)
	}
	return b, nil
}

func splitCSV(value string) []string {
	var values []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			values = append(values, item)
		}
	}
	return values
}
