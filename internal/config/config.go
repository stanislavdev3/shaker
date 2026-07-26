package config

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pelletier/go-toml/v2"
)

const maxConfigBytes = 1 << 20

type Config struct {
	Environment, Version, Role, HTTPAddress, MetricsAddress, DatabaseURL  string
	ProviderName, ProviderStateFile                                       string
	KafkaBrokers                                                          []string
	KafkaClientID, KafkaCoreConsumerGroup, KafkaNotificationConsumerGroup string
	KafkaMaxMessageBytes, CoreOutboxBatchSize                             int
	CoreOutboxLockTimeout, CoreOutboxPollInterval                         time.Duration
	DatabaseMaxConnections, DatabaseMinConnections                        int
	USGSRealtimeURL, USGSFDSNURL                                          string
	USGSPollInterval, USGSHTTPTimeout                                     time.Duration
	USGSMaxResponseBytes                                                  int64
	EMSCEnabled                                                           bool
	EMSCWebSocketURL, EMSCFDSNURL                                         string
	EMSCPollInterval, EMSCHTTPTimeout, EMSCLookback                       time.Duration
	EMSCPingInterval                                                      time.Duration
	EMSCMaxResponseBytes, EMSCMaxFrameBytes                               int64
	GEOFONEnabled, KNDCEnabled                                            bool
	GEOFONFDSNURL, KNDCBulletinURL                                        string
	GEOFONPollInterval, GEOFONHTTPTimeout, GEOFONLookback                 time.Duration
	KNDCPollInterval, KNDCHTTPTimeout                                     time.Duration
	GEOFONMaxResponseBytes, KNDCMaxResponseBytes                          int64
	BackfillChunkDuration, RecoveryOverlapDuration                        time.Duration
	NotificationBatchSize, NotificationMaxAttempts                        int
	NotificationLockTimeout, NotificationPollInterval                     time.Duration
	AdminAPIKey                                                           string
	AdminEnabled                                                          bool
	AdminHost, CloudflareAccessTeamDomain, CloudflareAccessAudience       string
	AdminBootstrapOwners                                                  []string
	AdminDevelopmentEmail, GrafanaBaseURL                                 string
	EncryptionKey                                                         []byte
	WebhookAllowPrivate                                                   bool
	WebhookHTTPTimeout                                                    time.Duration
	WebhookMaxResponseBytes                                               int64
	TelegramBotToken, TelegramAPIURL, TelegramGlobalChannel               string
	TelegramPollTimeout                                                   time.Duration
	TelegramMaxResponseBytes                                              int64
	LogLevel, OTELEndpoint                                                string
	MaxSearchRadiusKM                                                     float64
	CursorHMACKey                                                         []byte
}

type duration time.Duration

func (d *duration) UnmarshalText(value []byte) error {
	parsed, err := time.ParseDuration(string(value))
	if err != nil || parsed <= 0 {
		return errors.New("must be a positive duration")
	}
	*d = duration(parsed)
	return nil
}

type fileConfig struct {
	App            appConfig            `toml:"app"`
	Database       databaseConfig       `toml:"database"`
	Kafka          kafkaConfig          `toml:"kafka"`
	Ingestion      ingestionConfig      `toml:"ingestion"`
	Providers      providersConfig      `toml:"providers"`
	Notification   notificationConfig   `toml:"notification"`
	API            apiConfig            `toml:"api"`
	Administration administrationConfig `toml:"administration"`
	Security       securityConfig       `toml:"security"`
	Observability  observabilityConfig  `toml:"observability"`
}

type appConfig struct {
	Environment    string `toml:"environment"`
	Version        string `toml:"version"`
	DefaultRole    string `toml:"default_role"`
	HTTPAddress    string `toml:"http_address"`
	MetricsAddress string `toml:"metrics_address"`
	LogLevel       string `toml:"log_level"`
}

type databaseConfig struct {
	URL            string `toml:"url"`
	MaxConnections int    `toml:"max_connections"`
	MinConnections int    `toml:"min_connections"`
}

type kafkaConfig struct {
	Brokers                   []string `toml:"brokers"`
	ClientID                  string   `toml:"client_id"`
	CoreConsumerGroup         string   `toml:"core_consumer_group"`
	NotificationConsumerGroup string   `toml:"notification_consumer_group"`
	MaxMessageBytes           int      `toml:"max_message_bytes"`
	OutboxBatchSize           int      `toml:"outbox_batch_size"`
	OutboxLockTimeout         duration `toml:"outbox_lock_timeout"`
	OutboxPollInterval        duration `toml:"outbox_poll_interval"`
}

type ingestionConfig struct {
	BackfillChunkDuration   duration `toml:"backfill_chunk_duration"`
	RecoveryOverlapDuration duration `toml:"recovery_overlap_duration"`
}

type providersConfig struct {
	USGS   usgsConfig   `toml:"usgs"`
	EMSC   emscConfig   `toml:"emsc"`
	GEOFON geofonConfig `toml:"geofon"`
	KNDC   kndcConfig   `toml:"kndc"`
}

type usgsConfig struct {
	RealtimeURL      string   `toml:"realtime_url"`
	FDSNURL          string   `toml:"fdsn_url"`
	StateFile        string   `toml:"state_file"`
	PollInterval     duration `toml:"poll_interval"`
	HTTPTimeout      duration `toml:"http_timeout"`
	MaxResponseBytes int64    `toml:"max_response_bytes"`
}

type emscConfig struct {
	Enabled          bool     `toml:"enabled"`
	WebSocketURL     string   `toml:"websocket_url"`
	FDSNURL          string   `toml:"fdsn_url"`
	StateFile        string   `toml:"state_file"`
	PollInterval     duration `toml:"poll_interval"`
	HTTPTimeout      duration `toml:"http_timeout"`
	Lookback         duration `toml:"lookback"`
	PingInterval     duration `toml:"ping_interval"`
	MaxResponseBytes int64    `toml:"max_response_bytes"`
	MaxFrameBytes    int64    `toml:"max_frame_bytes"`
}

type geofonConfig struct {
	Enabled          bool     `toml:"enabled"`
	FDSNURL          string   `toml:"fdsn_url"`
	StateFile        string   `toml:"state_file"`
	PollInterval     duration `toml:"poll_interval"`
	HTTPTimeout      duration `toml:"http_timeout"`
	Lookback         duration `toml:"lookback"`
	MaxResponseBytes int64    `toml:"max_response_bytes"`
}

type kndcConfig struct {
	Enabled          bool     `toml:"enabled"`
	BulletinURL      string   `toml:"bulletin_url"`
	StateFile        string   `toml:"state_file"`
	PollInterval     duration `toml:"poll_interval"`
	HTTPTimeout      duration `toml:"http_timeout"`
	MaxResponseBytes int64    `toml:"max_response_bytes"`
}

type notificationConfig struct {
	BatchSize    int            `toml:"batch_size"`
	MaxAttempts  int            `toml:"max_attempts"`
	LockTimeout  duration       `toml:"lock_timeout"`
	PollInterval duration       `toml:"poll_interval"`
	Webhook      webhookConfig  `toml:"webhook"`
	Telegram     telegramConfig `toml:"telegram"`
}

type webhookConfig struct {
	AllowPrivate     bool     `toml:"allow_private_networks"`
	HTTPTimeout      duration `toml:"http_timeout"`
	MaxResponseBytes int64    `toml:"max_response_bytes"`
}

type telegramConfig struct {
	BotToken         string   `toml:"bot_token"`
	APIURL           string   `toml:"api_url"`
	GlobalChannel    string   `toml:"global_channel"`
	PollTimeout      duration `toml:"poll_timeout"`
	MaxResponseBytes int64    `toml:"max_response_bytes"`
}

type apiConfig struct {
	AdminAPIKey       string  `toml:"admin_api_key"`
	MaxSearchRadiusKM float64 `toml:"max_search_radius_km"`
}

type administrationConfig struct {
	Enabled              bool     `toml:"enabled"`
	Host                 string   `toml:"host"`
	CloudflareTeamDomain string   `toml:"cloudflare_team_domain"`
	CloudflareAudience   string   `toml:"cloudflare_audience"`
	BootstrapOwners      []string `toml:"bootstrap_owners"`
	DevelopmentEmail     string   `toml:"development_email"`
	GrafanaBaseURL       string   `toml:"grafana_base_url"`
}

type securityConfig struct {
	EncryptionKey string `toml:"encryption_key"`
}

type observabilityConfig struct {
	OTLPEndpoint string `toml:"otlp_endpoint"`
}

func Load(path, roleOverride, providerOverride string) (Config, error) {
	raw, err := decode(path)
	if err != nil {
		return Config{}, err
	}
	role := raw.App.DefaultRole
	if roleOverride != "" {
		role = roleOverride
	}
	c := Config{
		Environment: raw.App.Environment, Version: raw.App.Version, Role: role,
		HTTPAddress: raw.App.HTTPAddress, MetricsAddress: raw.App.MetricsAddress, LogLevel: raw.App.LogLevel,
		DatabaseURL: raw.Database.URL, DatabaseMaxConnections: raw.Database.MaxConnections,
		DatabaseMinConnections: raw.Database.MinConnections,
		KafkaBrokers:           append([]string(nil), raw.Kafka.Brokers...), KafkaClientID: raw.Kafka.ClientID,
		KafkaCoreConsumerGroup:         raw.Kafka.CoreConsumerGroup,
		KafkaNotificationConsumerGroup: raw.Kafka.NotificationConsumerGroup,
		KafkaMaxMessageBytes:           raw.Kafka.MaxMessageBytes, CoreOutboxBatchSize: raw.Kafka.OutboxBatchSize,
		CoreOutboxLockTimeout:  time.Duration(raw.Kafka.OutboxLockTimeout),
		CoreOutboxPollInterval: time.Duration(raw.Kafka.OutboxPollInterval),
		USGSRealtimeURL:        raw.Providers.USGS.RealtimeURL, USGSFDSNURL: raw.Providers.USGS.FDSNURL,
		USGSPollInterval: time.Duration(raw.Providers.USGS.PollInterval),
		USGSHTTPTimeout:  time.Duration(raw.Providers.USGS.HTTPTimeout), USGSMaxResponseBytes: raw.Providers.USGS.MaxResponseBytes,
		EMSCEnabled: raw.Providers.EMSC.Enabled, EMSCWebSocketURL: raw.Providers.EMSC.WebSocketURL,
		EMSCFDSNURL: raw.Providers.EMSC.FDSNURL, EMSCPollInterval: time.Duration(raw.Providers.EMSC.PollInterval),
		EMSCHTTPTimeout: time.Duration(raw.Providers.EMSC.HTTPTimeout), EMSCLookback: time.Duration(raw.Providers.EMSC.Lookback),
		EMSCPingInterval:     time.Duration(raw.Providers.EMSC.PingInterval),
		EMSCMaxResponseBytes: raw.Providers.EMSC.MaxResponseBytes, EMSCMaxFrameBytes: raw.Providers.EMSC.MaxFrameBytes,
		GEOFONEnabled: raw.Providers.GEOFON.Enabled, GEOFONFDSNURL: raw.Providers.GEOFON.FDSNURL,
		GEOFONPollInterval:     time.Duration(raw.Providers.GEOFON.PollInterval),
		GEOFONHTTPTimeout:      time.Duration(raw.Providers.GEOFON.HTTPTimeout),
		GEOFONLookback:         time.Duration(raw.Providers.GEOFON.Lookback),
		GEOFONMaxResponseBytes: raw.Providers.GEOFON.MaxResponseBytes,
		KNDCEnabled:            raw.Providers.KNDC.Enabled, KNDCBulletinURL: raw.Providers.KNDC.BulletinURL,
		KNDCPollInterval:        time.Duration(raw.Providers.KNDC.PollInterval),
		KNDCHTTPTimeout:         time.Duration(raw.Providers.KNDC.HTTPTimeout),
		KNDCMaxResponseBytes:    raw.Providers.KNDC.MaxResponseBytes,
		BackfillChunkDuration:   time.Duration(raw.Ingestion.BackfillChunkDuration),
		RecoveryOverlapDuration: time.Duration(raw.Ingestion.RecoveryOverlapDuration),
		NotificationBatchSize:   raw.Notification.BatchSize, NotificationMaxAttempts: raw.Notification.MaxAttempts,
		NotificationLockTimeout:  time.Duration(raw.Notification.LockTimeout),
		NotificationPollInterval: time.Duration(raw.Notification.PollInterval),
		WebhookAllowPrivate:      raw.Notification.Webhook.AllowPrivate,
		WebhookHTTPTimeout:       time.Duration(raw.Notification.Webhook.HTTPTimeout),
		WebhookMaxResponseBytes:  raw.Notification.Webhook.MaxResponseBytes,
		TelegramBotToken:         raw.Notification.Telegram.BotToken, TelegramAPIURL: raw.Notification.Telegram.APIURL,
		TelegramGlobalChannel:    raw.Notification.Telegram.GlobalChannel,
		TelegramPollTimeout:      time.Duration(raw.Notification.Telegram.PollTimeout),
		TelegramMaxResponseBytes: raw.Notification.Telegram.MaxResponseBytes,
		AdminAPIKey:              raw.API.AdminAPIKey, MaxSearchRadiusKM: raw.API.MaxSearchRadiusKM,
		AdminEnabled: raw.Administration.Enabled, AdminHost: raw.Administration.Host,
		CloudflareAccessTeamDomain: raw.Administration.CloudflareTeamDomain,
		CloudflareAccessAudience:   raw.Administration.CloudflareAudience,
		AdminBootstrapOwners:       append([]string(nil), raw.Administration.BootstrapOwners...),
		AdminDevelopmentEmail:      raw.Administration.DevelopmentEmail,
		GrafanaBaseURL:             raw.Administration.GrafanaBaseURL, OTELEndpoint: raw.Observability.OTLPEndpoint,
	}
	if c.KafkaClientID == "" {
		c.KafkaClientID = "shaker-" + role
		if providerOverride != "" {
			c.KafkaClientID += "-" + providerOverride
		}
	}
	if providerOverride != "" {
		c.ProviderName = providerOverride
		c.ProviderStateFile = providerStateFile(raw.Providers, providerOverride)
	}
	if raw.Security.EncryptionKey != "" {
		c.EncryptionKey, err = base64.StdEncoding.DecodeString(raw.Security.EncryptionKey)
		if err != nil {
			return Config{}, fmt.Errorf("security.encryption_key must be base64: %w", err)
		}
		c.CursorHMACKey = c.EncryptionKey
	}
	if err := validate(c); err != nil {
		return Config{}, err
	}
	return c, nil
}

func HTTPAddress(path string) (string, error) {
	raw, err := decode(path)
	if err != nil {
		return "", err
	}
	if raw.App.HTTPAddress == "" {
		return "", errors.New("app.http_address is required")
	}
	return raw.App.HTTPAddress, nil
}

func decode(path string) (fileConfig, error) {
	if path == "" {
		return fileConfig{}, errors.New("config path is required")
	}
	file, err := os.Open(path)
	if err != nil {
		return fileConfig{}, fmt.Errorf("open config %q: %w", path, err)
	}
	defer func() { _ = file.Close() }()
	data, err := io.ReadAll(io.LimitReader(file, maxConfigBytes+1))
	if err != nil {
		return fileConfig{}, fmt.Errorf("read config %q: %w", path, err)
	}
	if len(data) > maxConfigBytes {
		return fileConfig{}, fmt.Errorf("config %q exceeds 1 MiB", path)
	}
	raw := defaults()
	decoder := toml.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&raw); err != nil {
		return fileConfig{}, fmt.Errorf("decode config %q: %w", path, err)
	}
	return raw, nil
}

func defaults() fileConfig {
	return fileConfig{
		App: appConfig{Environment: "development", Version: "dev", DefaultRole: "all",
			HTTPAddress: ":8080", MetricsAddress: ":9090", LogLevel: "info"},
		Database: databaseConfig{MaxConnections: 20, MinConnections: 2},
		Kafka: kafkaConfig{CoreConsumerGroup: "shaker-core-v1", NotificationConsumerGroup: "shaker-notification-v1",
			MaxMessageBytes: 4 << 20, OutboxBatchSize: 100, OutboxLockTimeout: duration(5 * time.Minute),
			OutboxPollInterval: duration(time.Second)},
		Ingestion: ingestionConfig{BackfillChunkDuration: duration(24 * time.Hour), RecoveryOverlapDuration: duration(time.Hour)},
		Providers: providersConfig{
			USGS: usgsConfig{RealtimeURL: "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson",
				FDSNURL: "https://earthquake.usgs.gov/fdsnws/event/1/query", StateFile: "/var/lib/shaker/provider-state/usgs.json",
				PollInterval: duration(time.Minute), HTTPTimeout: duration(20 * time.Second), MaxResponseBytes: 25 << 20},
			EMSC: emscConfig{WebSocketURL: "wss://www.seismicportal.eu/standing_order/websocket",
				FDSNURL: "https://www.seismicportal.eu/fdsnws/event/1/query", StateFile: "/var/lib/shaker/provider-state/emsc.json",
				PollInterval: duration(30 * time.Second), HTTPTimeout: duration(20 * time.Second), Lookback: duration(2 * time.Hour),
				PingInterval: duration(15 * time.Second), MaxResponseBytes: 25 << 20, MaxFrameBytes: 256 << 10},
			GEOFON: geofonConfig{FDSNURL: "https://geofon.gfz.de/fdsnws/event/1/query",
				StateFile: "/var/lib/shaker/provider-state/geofon.json", PollInterval: duration(time.Minute),
				HTTPTimeout: duration(20 * time.Second), Lookback: duration(2 * time.Hour), MaxResponseBytes: 25 << 20},
			KNDC: kndcConfig{BulletinURL: "https://kndc.kz/kndc/pagecontent/alarm-bulletin",
				StateFile: "/var/lib/shaker/provider-state/kndc.json", PollInterval: duration(5 * time.Minute),
				HTTPTimeout: duration(20 * time.Second), MaxResponseBytes: 4 << 20},
		},
		Notification: notificationConfig{BatchSize: 50, MaxAttempts: 10, LockTimeout: duration(5 * time.Minute),
			PollInterval: duration(time.Second),
			Webhook:      webhookConfig{HTTPTimeout: duration(15 * time.Second), MaxResponseBytes: 64 << 10},
			Telegram: telegramConfig{APIURL: "https://api.telegram.org", PollTimeout: duration(25 * time.Second),
				MaxResponseBytes: 1 << 20}},
		API: apiConfig{MaxSearchRadiusKM: 2000},
	}
}

func providerStateFile(providers providersConfig, name string) string {
	switch name {
	case "emsc":
		return providers.EMSC.StateFile
	case "usgs":
		return providers.USGS.StateFile
	case "geofon":
		return providers.GEOFON.StateFile
	case "kndc":
		return providers.KNDC.StateFile
	default:
		return ""
	}
}

func validate(c Config) error {
	if c.Environment == "" || c.Version == "" || c.HTTPAddress == "" || c.MetricsAddress == "" || c.LogLevel == "" {
		return errors.New("app environment, version, addresses, and log_level are required")
	}
	if c.Role != "api" && c.Role != "worker" && c.Role != "all" && c.Role != "backfill" &&
		c.Role != "provider-worker" && c.Role != "core" && c.Role != "notification" {
		return fmt.Errorf("invalid app.default_role %q", c.Role)
	}
	if c.Role != "provider-worker" && c.DatabaseURL == "" {
		return errors.New("database.url is required")
	}
	if (c.Role == "provider-worker" || c.Role == "core" || c.Role == "notification") && len(c.KafkaBrokers) == 0 {
		return errors.New("kafka.brokers is required")
	}
	for _, broker := range c.KafkaBrokers {
		if strings.TrimSpace(broker) == "" {
			return errors.New("kafka.brokers cannot contain an empty address")
		}
	}
	if c.KafkaClientID == "" || c.KafkaCoreConsumerGroup == "" || c.KafkaNotificationConsumerGroup == "" {
		return errors.New("kafka client and consumer group names are required")
	}
	if c.Role == "provider-worker" {
		if c.ProviderName != "emsc" && c.ProviderName != "usgs" && c.ProviderName != "geofon" && c.ProviderName != "kndc" {
			return errors.New("provider-worker requires an emsc, usgs, geofon, or kndc provider argument")
		}
		if c.ProviderStateFile == "" || !filepath.IsAbs(c.ProviderStateFile) {
			return errors.New("provider state_file must be an absolute path")
		}
	}
	if (c.Role == "api" || c.Role == "all") && c.AdminAPIKey == "" {
		return errors.New("api.admin_api_key is required")
	}
	if c.AdminEnabled && (c.Role == "api" || c.Role == "all") {
		if c.AdminHost == "" || strings.Contains(c.AdminHost, "://") || strings.ContainsAny(c.AdminHost, " /\t\r\n") {
			return errors.New("administration.host must be a hostname without a URL scheme")
		}
		if c.Environment == "development" && c.AdminDevelopmentEmail != "" {
			if len(c.AdminBootstrapOwners) == 0 {
				return errors.New("administration.bootstrap_owners is required when administration is enabled")
			}
		} else if c.CloudflareAccessTeamDomain == "" || c.CloudflareAccessAudience == "" {
			return errors.New("cloudflare team domain and audience are required when administration is enabled")
		}
		if c.AdminDevelopmentEmail == "" {
			if err := validateCloudflareTeamDomain(c.CloudflareAccessTeamDomain); err != nil {
				return err
			}
		}
		if c.Environment != "development" && c.AdminDevelopmentEmail != "" {
			return errors.New("administration.development_email is development-only")
		}
		if len(c.AdminBootstrapOwners) == 0 {
			return errors.New("administration.bootstrap_owners is required when administration is enabled")
		}
	}
	if c.Role == "api" || c.Role == "notification" || c.Role == "worker" || c.Role == "all" {
		if len(c.EncryptionKey) != 32 {
			return errors.New("security.encryption_key must be base64 encoding of exactly 32 bytes")
		}
	}
	if c.KafkaMaxMessageBytes <= 0 || c.KafkaMaxMessageBytes > 1<<31-1 || c.CoreOutboxBatchSize <= 0 ||
		c.CoreOutboxLockTimeout <= 0 || c.CoreOutboxPollInterval <= 0 {
		return errors.New("kafka and core outbox limits must be positive")
	}
	if c.DatabaseMinConnections < 0 || c.DatabaseMaxConnections <= 0 || c.DatabaseMinConnections > c.DatabaseMaxConnections {
		return errors.New("database connection limits are invalid")
	}
	if c.BackfillChunkDuration <= 0 || c.RecoveryOverlapDuration <= 0 ||
		c.USGSPollInterval <= 0 || c.USGSHTTPTimeout <= 0 || c.USGSMaxResponseBytes <= 0 ||
		c.EMSCPollInterval <= 0 || c.EMSCHTTPTimeout <= 0 || c.EMSCLookback <= 0 || c.EMSCPingInterval <= 0 ||
		c.EMSCMaxResponseBytes <= 0 || c.EMSCMaxFrameBytes <= 0 ||
		c.GEOFONPollInterval <= 0 || c.GEOFONHTTPTimeout <= 0 || c.GEOFONLookback <= 0 || c.GEOFONMaxResponseBytes <= 0 ||
		c.KNDCPollInterval <= 0 || c.KNDCHTTPTimeout <= 0 || c.KNDCMaxResponseBytes <= 0 {
		return errors.New("ingestion durations and I/O limits must be positive")
	}
	if c.NotificationBatchSize <= 0 || c.NotificationMaxAttempts <= 0 || c.NotificationLockTimeout <= 0 ||
		c.NotificationPollInterval <= 0 || c.WebhookHTTPTimeout <= 0 || c.WebhookMaxResponseBytes <= 0 ||
		c.TelegramPollTimeout <= 0 || c.TelegramMaxResponseBytes <= 0 {
		return errors.New("notification limits and durations must be positive")
	}
	if c.MaxSearchRadiusKM <= 0 {
		return errors.New("api.max_search_radius_km must be positive")
	}
	if c.Environment != "development" && c.WebhookAllowPrivate {
		return errors.New("notification.webhook.allow_private_networks is development-only")
	}
	if c.TelegramGlobalChannel != "" && c.TelegramBotToken == "" {
		return errors.New("notification.telegram.bot_token is required when global_channel is configured")
	}
	if c.TelegramGlobalChannel != "" && (!strings.HasPrefix(c.TelegramGlobalChannel, "@") ||
		len(c.TelegramGlobalChannel) > 64 || strings.ContainsAny(c.TelegramGlobalChannel, " \t\r\n")) {
		return errors.New("notification.telegram.global_channel must start with @")
	}
	if c.TelegramBotToken != "" {
		if err := validateEndpoint("notification.telegram.api_url", c.TelegramAPIURL, "http", "https"); err != nil {
			return err
		}
	}
	if c.EMSCEnabled || c.ProviderName == "emsc" {
		if err := validateEndpoint("providers.emsc.websocket_url", c.EMSCWebSocketURL, "ws", "wss"); err != nil {
			return err
		}
		if err := validateEndpoint("providers.emsc.fdsn_url", c.EMSCFDSNURL, "http", "https"); err != nil {
			return err
		}
	}
	if c.ProviderName == "usgs" {
		if err := validateEndpoint("providers.usgs.realtime_url", c.USGSRealtimeURL, "http", "https"); err != nil {
			return err
		}
		if err := validateEndpoint("providers.usgs.fdsn_url", c.USGSFDSNURL, "http", "https"); err != nil {
			return err
		}
	}
	if c.GEOFONEnabled || c.ProviderName == "geofon" {
		if err := validateEndpoint("providers.geofon.fdsn_url", c.GEOFONFDSNURL, "http", "https"); err != nil {
			return err
		}
	}
	if c.KNDCEnabled || c.ProviderName == "kndc" {
		if err := validateEndpoint("providers.kndc.bulletin_url", c.KNDCBulletinURL, "http", "https"); err != nil {
			return err
		}
	}
	return nil
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
		return errors.New("administration.cloudflare_team_domain must be an HTTPS cloudflareaccess.com team domain")
	}
	hostname := strings.ToLower(parsed.Hostname())
	if parsed.Scheme != "https" || parsed.User != nil || parsed.Port() != "" ||
		parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" ||
		!strings.HasSuffix(hostname, ".cloudflareaccess.com") {
		return errors.New("administration.cloudflare_team_domain must be an HTTPS cloudflareaccess.com team domain")
	}
	return nil
}
