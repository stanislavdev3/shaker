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
	"github.com/example/earthquake-service/internal/httpadmin"
	"github.com/example/earthquake-service/internal/httpapi"
	"github.com/example/earthquake-service/internal/ingestion"
	"github.com/example/earthquake-service/internal/notification"
	"github.com/example/earthquake-service/internal/observability"
	"github.com/example/earthquake-service/internal/provider/emsc"
	"github.com/example/earthquake-service/internal/provider/usgs"
	"github.com/example/earthquake-service/internal/realtime"
	"github.com/example/earthquake-service/internal/repository/postgres"
	"github.com/example/earthquake-service/internal/telegram"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		if err := healthcheck(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	role := ""
	if len(os.Args) > 1 {
		role = os.Args[1]
	}
	cfg, err := config.Load(role)
	if err != nil {
		fatal("load configuration", err)
	}
	log := newLogger(cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	repo, err := postgres.Open(ctx, cfg.DatabaseURL, cfg.DatabaseMinConnections, cfg.DatabaseMaxConnections)
	if err != nil {
		fatal("connect to database", err)
	}
	defer repo.Pool.Close()

	cipher, err := notification.NewCipher(cfg.EncryptionKey)
	if err != nil {
		fatal("initialize secrets cipher", err)
	}
	userAgent := "earthquake-service/" + cfg.Version
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
	var telegramClient *telegram.Client
	if (cfg.Role == "worker" || cfg.Role == "all") && cfg.TelegramBotToken != "" {
		telegramClient = telegram.NewClient(cfg.TelegramAPIURL, cfg.TelegramBotToken, cfg.TelegramPollTimeout, cfg.TelegramMaxResponseBytes)
		if cfg.TelegramGlobalChannel != "" {
			subscription, registerErr := telegram.RegisterGlobalChannel(ctx, repo, telegramClient, cfg.TelegramGlobalChannel, clock.Real{}.Now())
			if registerErr != nil {
				fatal("register Telegram global channel", registerErr)
			}
			log.Info("Telegram global channel enabled", "channel", cfg.TelegramGlobalChannel, "subscription_id", subscription.ID)
		}
	}

	switch cfg.Role {
	case "api":
		err = runAPI(ctx, cfg, repo, cipher, log)
	case "worker":
		runWorker(ctx, cfg, usgsIngestion, emscIngestion, emscStream, repo, cipher, userAgent, telegramClient, log)
	case "all":
		go runWorker(ctx, cfg, usgsIngestion, emscIngestion, emscStream, repo, cipher, userAgent, telegramClient, log)
		err = runAPI(ctx, cfg, repo, cipher, log)
	case "backfill":
		err = runBackfill(ctx, cfg, usgsIngestion, os.Args[2:])
	default:
		err = fmt.Errorf("unsupported role %q", cfg.Role)
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		fatal("service stopped", err)
	}
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

func runWorker(ctx context.Context, cfg config.Config, usgsIngestion, emscIngestion *ingestion.Service, emscStream *emsc.Stream,
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

func healthcheck() error {
	address := os.Getenv("HTTP_ADDRESS")
	if address == "" {
		address = ":8080"
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("invalid HTTP_ADDRESS: %w", err)
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
