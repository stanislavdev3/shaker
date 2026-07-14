package config

import (
	"encoding/base64"
	"testing"
)

func TestLoadEMSCConfiguration(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv("EMSC_ENABLED", "true")
	t.Setenv("EMSC_POLL_INTERVAL", "45s")

	configuration, err := Load("worker")
	if err != nil {
		t.Fatal(err)
	}
	if !configuration.EMSCEnabled || configuration.EMSCPollInterval.String() != "45s" {
		t.Fatalf("unexpected EMSC configuration: %#v", configuration)
	}
	if configuration.EMSCWebSocketURL != "wss://www.seismicportal.eu/standing_order/websocket" {
		t.Fatalf("websocket URL=%q", configuration.EMSCWebSocketURL)
	}
	if configuration.MetricsAddress != ":9090" {
		t.Fatalf("metrics address=%q", configuration.MetricsAddress)
	}
}

func TestLoadRejectsInvalidEMSCWebSocketURL(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv("EMSC_ENABLED", "true")
	t.Setenv("EMSC_WEBSOCKET_URL", "https://www.seismicportal.eu/standing_order/websocket")
	if _, err := Load("worker"); err == nil {
		t.Fatal("expected an invalid WebSocket URL error")
	}
}

func TestLoadAdministrationConfiguration(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv("ADMIN_ENABLED", "true")
	t.Setenv("ADMIN_HOST", "admin.example.com")
	t.Setenv("ADMIN_DEVELOPMENT_EMAIL", "admin@example.com")
	t.Setenv("ADMIN_BOOTSTRAP_OWNERS", " admin@example.com, operator@example.com ")

	configuration, err := Load("api")
	if err != nil {
		t.Fatal(err)
	}
	if !configuration.AdminEnabled || len(configuration.AdminBootstrapOwners) != 2 {
		t.Fatalf("unexpected administration configuration: %#v", configuration)
	}
}

func TestLoadRequiresCloudflareAccessOutsideDevelopment(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv("APP_ENV", "production")
	t.Setenv("ADMIN_ENABLED", "true")
	t.Setenv("ADMIN_HOST", "admin.example.com")
	t.Setenv("ADMIN_BOOTSTRAP_OWNERS", "admin@example.com")
	if _, err := Load("api"); err == nil {
		t.Fatal("expected missing Cloudflare Access configuration error")
	}
}

func TestLoadRejectsNonCloudflareAdministrationIssuer(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv("APP_ENV", "production")
	t.Setenv("ADMIN_ENABLED", "true")
	t.Setenv("ADMIN_HOST", "admin.example.com")
	t.Setenv("ADMIN_BOOTSTRAP_OWNERS", "admin@example.com")
	t.Setenv("CLOUDFLARE_ACCESS_TEAM_DOMAIN", "http://127.0.0.1:8080")
	t.Setenv("CLOUDFLARE_ACCESS_AUDIENCE", "audience")
	if _, err := Load("api"); err == nil {
		t.Fatal("expected invalid Cloudflare Access team domain error")
	}
}

func setRequiredEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("APP_ENV", "development")
	t.Setenv("DATABASE_URL", "postgres://earthquake:earthquake@localhost/earthquake")
	t.Setenv("ADMIN_API_KEY", "test-admin-key")
	t.Setenv("SECRETS_ENCRYPTION_KEY", base64.StdEncoding.EncodeToString(make([]byte, 32)))
	t.Setenv("TELEGRAM_GLOBAL_CHANNEL", "")
	t.Setenv("TELEGRAM_BOT_TOKEN", "")
}
