package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/example/earthquake-service/internal/administration"
	"github.com/example/earthquake-service/internal/clock"
	"github.com/example/earthquake-service/internal/config"
	coreservice "github.com/example/earthquake-service/internal/core"
	"github.com/example/earthquake-service/internal/eventstream"
	"github.com/example/earthquake-service/internal/httpadmin"
	"github.com/example/earthquake-service/internal/httpapi"
	"github.com/example/earthquake-service/internal/ingestion"
	"github.com/example/earthquake-service/internal/kafka"
	"github.com/example/earthquake-service/internal/notification"
	"github.com/example/earthquake-service/internal/observability"
	"github.com/example/earthquake-service/internal/provider"
	"github.com/example/earthquake-service/internal/provider/emsc"
	"github.com/example/earthquake-service/internal/provider/geofon"
	"github.com/example/earthquake-service/internal/provider/kndc"
	"github.com/example/earthquake-service/internal/provider/usgs"
	"github.com/example/earthquake-service/internal/providerworker"
	"github.com/example/earthquake-service/internal/realtime"
	"github.com/example/earthquake-service/internal/repository/postgres"
	"github.com/example/earthquake-service/internal/telegram"
)

func main() {
	command, err := parseCommand(os.Args[1:])
	if err != nil {
		fatal("parse command", err)
	}
	if command.role == "healthcheck" {
		if err := healthcheck(command.configPath); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	cfg, err := config.Load(command.configPath, command.role, command.provider)
	if err != nil {
		fatal("load configuration", err)
	}
	log := newLogger(cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	userAgent := "earthquake-service/" + cfg.Version

	if cfg.Role == "provider-worker" {
		err = runProviderWorker(ctx, cfg, userAgent, log)
		if err != nil && !errors.Is(err, context.Canceled) {
			fatal("provider worker stopped", err)
		}
		return
	}

	repo, err := postgres.Open(ctx, cfg.DatabaseURL, cfg.DatabaseMinConnections, cfg.DatabaseMaxConnections)
	if err != nil {
		fatal("connect to database", err)
	}
	defer repo.Pool.Close()
	if cfg.Role == "core" {
		err = runCore(ctx, cfg, repo, log)
		if err != nil && !errors.Is(err, context.Canceled) {
			fatal("core service stopped", err)
		}
		return
	}

	var cipher *notification.Cipher
	if len(cfg.EncryptionKey) > 0 {
		cipher, err = notification.NewCipher(cfg.EncryptionKey)
		if err != nil {
			fatal("initialize secrets cipher", err)
		}
	}
	var telegramClient *telegram.Client
	if (cfg.Role == "worker" || cfg.Role == "all" || cfg.Role == "notification") && cfg.TelegramBotToken != "" {
		telegramClient = telegram.NewClient(cfg.TelegramAPIURL, cfg.TelegramBotToken, cfg.TelegramPollTimeout, cfg.TelegramMaxResponseBytes)
		if cfg.TelegramGlobalChannel != "" {
			subscription, registerErr := telegram.RegisterGlobalChannel(ctx, repo, telegramClient, cfg.TelegramGlobalChannel, clock.Real{}.Now())
			if registerErr != nil {
				fatal("register Telegram global channel", registerErr)
			}
			log.Info("Telegram global channel enabled", "channel", cfg.TelegramGlobalChannel, "subscription_id", subscription.ID)
		}
	}
	if cfg.Role == "notification" {
		err = runNotification(ctx, cfg, repo, cipher, userAgent, telegramClient, log)
		if err != nil && !errors.Is(err, context.Canceled) {
			fatal("notification service stopped", err)
		}
		return
	}

	usgsProvider := usgs.New(cfg.USGSRealtimeURL, cfg.USGSFDSNURL, userAgent, cfg.USGSHTTPTimeout, cfg.USGSMaxResponseBytes)
	usgsIngestion := ingestion.New(usgsProvider, repo, clock.Real{}, log)
	var emscIngestion *ingestion.Service
	var emscStream *emsc.Stream
	if cfg.EMSCEnabled && (cfg.Role == "worker" || cfg.Role == "all") {
		emscMetrics := observability.NewEMSCWebSocketMetrics(prometheus.DefaultRegisterer)
		emscProvider := emsc.NewFDSN(cfg.EMSCFDSNURL, userAgent, clock.Real{}, cfg.EMSCHTTPTimeout, cfg.EMSCLookback, cfg.EMSCMaxResponseBytes)
		emscIngestion = ingestion.New(emscProvider, repo, clock.Real{}, log)
		emscStream = emsc.NewStream(cfg.EMSCWebSocketURL, userAgent, cfg.EMSCHTTPTimeout, cfg.EMSCPingInterval, cfg.EMSCMaxFrameBytes, log, emscMetrics)
	}
	var geofonIngestion *ingestion.Service
	if cfg.GEOFONEnabled && (cfg.Role == "worker" || cfg.Role == "all") {
		geofonProvider := geofon.New(cfg.GEOFONFDSNURL, userAgent, clock.Real{}, cfg.GEOFONHTTPTimeout, cfg.GEOFONLookback, cfg.GEOFONMaxResponseBytes)
		geofonIngestion = ingestion.New(geofonProvider, repo, clock.Real{}, log)
	}
	var kndcIngestion *ingestion.Service
	if cfg.KNDCEnabled && (cfg.Role == "worker" || cfg.Role == "all") {
		kndcProvider := kndc.New(cfg.KNDCBulletinURL, userAgent, cfg.KNDCHTTPTimeout, cfg.KNDCMaxResponseBytes)
		kndcIngestion = ingestion.New(kndcProvider, repo, clock.Real{}, log)
	}
	switch cfg.Role {
	case "api":
		err = runAPI(ctx, cfg, repo, cipher, log)
	case "worker":
		runWorker(ctx, cfg, usgsIngestion, emscIngestion, geofonIngestion, kndcIngestion, emscStream, repo, cipher, userAgent, telegramClient, log)
	case "all":
		go runWorker(ctx, cfg, usgsIngestion, emscIngestion, geofonIngestion, kndcIngestion, emscStream, repo, cipher, userAgent, telegramClient, log)
		err = runAPI(ctx, cfg, repo, cipher, log)
	case "backfill":
		err = runBackfill(ctx, cfg, usgsIngestion, command.args)
	default:
		err = fmt.Errorf("unsupported role %q", cfg.Role)
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		fatal("service stopped", err)
	}
}

type commandOptions struct {
	configPath string
	role       string
	provider   string
	args       []string
}

func parseCommand(args []string) (commandOptions, error) {
	flags := flag.NewFlagSet("earthquake-service", flag.ContinueOnError)
	configPath := flags.String("config", "config.toml", "path to the TOML configuration file")
	if err := flags.Parse(args); err != nil {
		return commandOptions{}, err
	}
	remaining := flags.Args()
	command := commandOptions{configPath: *configPath}
	if len(remaining) == 0 {
		return command, nil
	}
	command.role = remaining[0]
	command.args = remaining[1:]
	if command.role == "provider-worker" {
		if len(command.args) == 0 {
			return commandOptions{}, errors.New("provider-worker requires a provider argument")
		}
		command.provider = command.args[0]
		command.args = command.args[1:]
	}
	if command.role != "backfill" && len(command.args) != 0 {
		return commandOptions{}, fmt.Errorf("unexpected arguments for %s: %s", command.role, strings.Join(command.args, " "))
	}
	return command, nil
}

func runProviderWorker(ctx context.Context, cfg config.Config, userAgent string, log *slog.Logger) error {
	if err := startWorkerMetrics(ctx, cfg.MetricsAddress, log); err != nil {
		return fmt.Errorf("start provider metrics server: %w", err)
	}
	metrics := observability.NewMetrics(prometheus.DefaultRegisterer)
	publisher, err := kafka.NewProducer(cfg.KafkaBrokers, cfg.KafkaClientID, int32(cfg.KafkaMaxMessageBytes))
	if err != nil {
		return fmt.Errorf("initialize Kafka producer: %w", err)
	}
	defer publisher.Close()
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := publisher.Ping(pingCtx); err != nil {
		return fmt.Errorf("connect to Kafka: %w", err)
	}
	state, err := providerworker.NewFileStateStore(cfg.ProviderStateFile)
	if err != nil {
		return err
	}
	var source provider.Provider
	var interval time.Duration
	var stream *emsc.Stream
	switch cfg.ProviderName {
	case "emsc":
		source = emsc.NewFDSN(cfg.EMSCFDSNURL, userAgent, clock.Real{}, cfg.EMSCHTTPTimeout,
			cfg.EMSCLookback, cfg.EMSCMaxResponseBytes)
		interval = cfg.EMSCPollInterval
		stream = emsc.NewStream(cfg.EMSCWebSocketURL, userAgent, cfg.EMSCHTTPTimeout, cfg.EMSCPingInterval,
			cfg.EMSCMaxFrameBytes, log, observability.NewEMSCWebSocketMetrics(prometheus.DefaultRegisterer))
	case "usgs":
		source = usgs.New(cfg.USGSRealtimeURL, cfg.USGSFDSNURL, userAgent, cfg.USGSHTTPTimeout, cfg.USGSMaxResponseBytes)
		interval = cfg.USGSPollInterval
	case "geofon":
		source = geofon.New(cfg.GEOFONFDSNURL, userAgent, clock.Real{}, cfg.GEOFONHTTPTimeout,
			cfg.GEOFONLookback, cfg.GEOFONMaxResponseBytes)
		interval = cfg.GEOFONPollInterval
	case "kndc":
		source = kndc.New(cfg.KNDCBulletinURL, userAgent, cfg.KNDCHTTPTimeout, cfg.KNDCMaxResponseBytes)
		interval = cfg.KNDCPollInterval
	default:
		return fmt.Errorf("unsupported provider %q", cfg.ProviderName)
	}
	service := providerworker.New(source, publisher, state, clock.Real{}, log, metrics)
	if err := service.Recover(ctx, cfg.RecoveryOverlapDuration, cfg.BackfillChunkDuration); err != nil {
		return fmt.Errorf("recover provider observations: %w", err)
	}
	if stream != nil {
		go stream.Run(ctx, service.PublishRealtime)
	}
	log.Info("provider worker started", "provider", cfg.ProviderName, "poll_interval", interval)
	return service.Run(ctx, interval)
}

func runCore(ctx context.Context, cfg config.Config, repo *postgres.Repository, log *slog.Logger) error {
	if err := startWorkerMetrics(ctx, cfg.MetricsAddress, log); err != nil {
		return fmt.Errorf("start core metrics server: %w", err)
	}
	metrics := observability.NewMetrics(prometheus.DefaultRegisterer)
	publisher, err := kafka.NewProducer(cfg.KafkaBrokers, cfg.KafkaClientID+"-outbox", int32(cfg.KafkaMaxMessageBytes))
	if err != nil {
		return fmt.Errorf("initialize core Kafka producer: %w", err)
	}
	defer publisher.Close()
	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaClientID+"-consumer",
		cfg.KafkaCoreConsumerGroup, eventstream.ProviderObservationsTopic, int32(cfg.KafkaMaxMessageBytes))
	if err != nil {
		return fmt.Errorf("initialize core Kafka consumer: %w", err)
	}
	processor := coreservice.NewObservationConsumer(consumer, repo, clock.Real{}, log, metrics)
	defer processor.Close()
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := publisher.Ping(pingCtx); err != nil {
		return fmt.Errorf("connect core producer to Kafka: %w", err)
	}
	if err := consumer.Ping(pingCtx); err != nil {
		return fmt.Errorf("connect core consumer to Kafka: %w", err)
	}
	relay := coreservice.NewOutboxRelay(repo, publisher, clock.Real{}, log, "core-outbox-"+uuid.NewString(),
		cfg.CoreOutboxBatchSize, cfg.CoreOutboxLockTimeout, cfg.CoreOutboxPollInterval, metrics)
	go relay.Run(ctx)
	log.Info("core service started", "consumer_group", cfg.KafkaCoreConsumerGroup)
	return processor.Run(ctx)
}

func runNotification(ctx context.Context, cfg config.Config, repo *postgres.Repository, cipher *notification.Cipher,
	userAgent string, telegramClient *telegram.Client, log *slog.Logger,
) error {
	if err := startWorkerMetrics(ctx, cfg.MetricsAddress, log); err != nil {
		return fmt.Errorf("start notification metrics server: %w", err)
	}
	metrics := observability.NewMetrics(prometheus.DefaultRegisterer)
	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaClientID+"-consumer",
		cfg.KafkaNotificationConsumerGroup, eventstream.IncidentChangesTopic, int32(cfg.KafkaMaxMessageBytes))
	if err != nil {
		return fmt.Errorf("initialize notification Kafka consumer: %w", err)
	}
	processor := notification.NewIncidentConsumer(consumer, repo, clock.Real{}, log, metrics)
	defer processor.Close()
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := consumer.Ping(pingCtx); err != nil {
		return fmt.Errorf("connect notification consumer to Kafka: %w", err)
	}
	if telegramClient != nil {
		go telegram.NewBot(repo, telegramClient, clock.Real{}, log).Run(ctx)
		log.Info("Telegram bot polling enabled")
	}
	worker := notification.NewWorker(repo, cipher, clock.Real{}, log, "notification-"+uuid.NewString(), userAgent,
		cfg.NotificationBatchSize, cfg.NotificationMaxAttempts, cfg.NotificationLockTimeout,
		cfg.NotificationPollInterval, cfg.WebhookHTTPTimeout, cfg.WebhookMaxResponseBytes,
		cfg.WebhookAllowPrivate, telegramClient, metrics)
	go worker.Run(ctx)
	log.Info("notification service started", "consumer_group", cfg.KafkaNotificationConsumerGroup)
	return processor.Run(ctx)
}

func runAPI(ctx context.Context, cfg config.Config, repo *postgres.Repository, cipher *notification.Cipher, log *slog.Logger) error {
	hub := realtime.NewHub()
	go hub.Run(ctx)
	go realtime.NewListener(repo.Pool, repo, hub, log).Run(ctx)

	var adminHandler http.Handler
	if cfg.AdminEnabled {
		adminService := administration.New(repo, clock.Real{}.Now)
		if err := adminService.BootstrapOwners(ctx, cfg.AdminBootstrapOwners); err != nil {
			return fmt.Errorf("bootstrap administration owners: %w", err)
		}
		var err error
		adminHandler, err = httpadmin.New(adminService, log, httpadmin.Config{
			Host:             cfg.AdminHost,
			TeamDomain:       cfg.CloudflareAccessTeamDomain,
			Audience:         cfg.CloudflareAccessAudience,
			DevelopmentEmail: cfg.AdminDevelopmentEmail,
			GrafanaBaseURL:   cfg.GrafanaBaseURL,
			CSRFKey:          cfg.CursorHMACKey,
			Now:              clock.Real{}.Now,
		})
		if err != nil {
			return fmt.Errorf("initialize administration interface: %w", err)
		}
	}
	handler := httpapi.New(
		repo,
		log,
		clock.Real{},
		observability.NewMetrics(prometheus.DefaultRegisterer),
		cfg.AdminAPIKey,
		cfg.CursorHMACKey,
		cipher,
		cfg.Environment == "production",
		cfg.MaxSearchRadiusKM,
		hub,
		adminHandler,
	)
	server := &http.Server{
		Addr:              cfg.HTTPAddress,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       90 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		log.Info("HTTP server started", "address", cfg.HTTPAddress, "version", cfg.Version)
		errCh <- server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func runWorker(ctx context.Context, cfg config.Config, usgsIngestion, emscIngestion, geofonIngestion, kndcIngestion *ingestion.Service, emscStream *emsc.Stream,
	repo *postgres.Repository, cipher *notification.Cipher, userAgent string, telegramClient *telegram.Client, log *slog.Logger,
) {
	if err := startWorkerMetrics(ctx, cfg.MetricsAddress, log); err != nil {
		log.Error("start worker metrics server", "error", err)
	}
	if from, to, err := usgsIngestion.RecoveryRange(ctx, cfg.RecoveryOverlapDuration); err != nil {
		log.Error("calculate recovery range", "error", err)
	} else if from != nil && to != nil {
		if err := usgsIngestion.RunBackfill(ctx, *from, *to, cfg.BackfillChunkDuration, "recovery"); err != nil {
			log.Error("recovery backfill failed", "from", from, "to", to, "error", err)
		}
	}
	if emscIngestion != nil {
		if from, to, err := emscIngestion.RecoveryRange(ctx, cfg.RecoveryOverlapDuration); err != nil {
			log.Error("calculate EMSC recovery range", "error", err)
		} else if from != nil && to != nil {
			if err := emscIngestion.RunBackfill(ctx, *from, *to, cfg.BackfillChunkDuration, "recovery"); err != nil {
				log.Error("EMSC recovery backfill failed", "from", from, "to", to, "error", err)
			}
		}
		go emscIngestion.Run(ctx, cfg.EMSCPollInterval)
		go emscStream.Run(ctx, emscIngestion.ApplyRealtime)
		log.Info("EMSC FDSN and WebSocket ingestion enabled")
	}
	startCatalogIngestion(ctx, geofonIngestion, cfg.GEOFONPollInterval, cfg.RecoveryOverlapDuration,
		cfg.BackfillChunkDuration, "GEOFON", log)
	startCatalogIngestion(ctx, kndcIngestion, cfg.KNDCPollInterval, cfg.RecoveryOverlapDuration,
		cfg.BackfillChunkDuration, "KNDC", log)

	workerID := "earthquake-service-" + uuid.NewString()
	if telegramClient != nil {
		go telegram.NewBot(repo, telegramClient, clock.Real{}, log).Run(ctx)
		log.Info("Telegram bot polling enabled")
	}
	deliveryWorker := notification.NewWorker(
		repo,
		cipher,
		clock.Real{},
		log,
		workerID,
		userAgent,
		cfg.NotificationBatchSize,
		cfg.NotificationMaxAttempts,
		cfg.NotificationLockTimeout,
		cfg.NotificationPollInterval,
		cfg.WebhookHTTPTimeout,
		cfg.WebhookMaxResponseBytes,
		cfg.WebhookAllowPrivate,
		telegramClient,
	)
	go deliveryWorker.Run(ctx)
	usgsIngestion.Run(ctx, cfg.USGSPollInterval)
}

func startCatalogIngestion(ctx context.Context, service *ingestion.Service, pollInterval, recoveryOverlap,
	backfillChunk time.Duration, providerName string, log *slog.Logger,
) {
	if service == nil {
		return
	}
	if from, to, err := service.RecoveryRange(ctx, recoveryOverlap); err != nil {
		log.Error("calculate provider recovery range", "provider", providerName, "error", err)
	} else if from != nil && to != nil {
		if err := service.RunBackfill(ctx, *from, *to, backfillChunk, "recovery"); err != nil {
			log.Error("provider recovery backfill failed", "provider", providerName, "from", from, "to", to, "error", err)
		}
	}
	go service.Run(ctx, pollInterval)
	log.Info("catalog ingestion enabled", "provider", providerName, "poll_interval", pollInterval)
}

func startWorkerMetrics(ctx context.Context, address string, log *slog.Logger) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	server := &http.Server{
		Handler:           promhttp.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Warn("shut down worker metrics server", "error", err)
		}
	}()
	go func() {
		log.Info("worker metrics server started", "address", address)
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("serve worker metrics", "error", err)
		}
	}()
	return nil
}

func runBackfill(ctx context.Context, cfg config.Config, service *ingestion.Service, args []string) error {
	flags := flag.NewFlagSet("backfill", flag.ContinueOnError)
	fromValue := flags.String("from", "", "inclusive RFC3339 start time")
	toValue := flags.String("to", "", "exclusive RFC3339 end time")
	if err := flags.Parse(args); err != nil {
		return err
	}
	from, err := time.Parse(time.RFC3339, *fromValue)
	if err != nil {
		return fmt.Errorf("parse --from: %w", err)
	}
	to, err := time.Parse(time.RFC3339, *toValue)
	if err != nil {
		return fmt.Errorf("parse --to: %w", err)
	}
	return service.RunBackfill(ctx, from, to, cfg.BackfillChunkDuration, "backfill")
}

func healthcheck(configPath string) error {
	address, err := config.HTTPAddress(configPath)
	if err != nil {
		return err
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("invalid app.http_address: %w", err)
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	client := &http.Client{Timeout: 3 * time.Second}
	response, err := client.Get("http://" + net.JoinHostPort(host, port) + "/health/ready")
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("readiness endpoint returned HTTP %d", response.StatusCode)
	}
	return nil
}

func newLogger(level string) *slog.Logger {
	var parsed slog.Level
	if err := parsed.UnmarshalText([]byte(strings.ToUpper(level))); err != nil {
		parsed = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: parsed}))
}

func fatal(message string, err error) {
	slog.Error(message, "error", err)
	os.Exit(1)
}
