package config

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadTOMLConfigurationAndDefaults(t *testing.T) {
	path := writeConfig(t, baseConfig()+`
[providers.emsc]
enabled = true
poll_interval = "45s"
`)
	configuration, err := Load(path, "worker", "")
	if err != nil {
		t.Fatal(err)
	}
	if !configuration.EMSCEnabled || configuration.EMSCPollInterval != 45*time.Second {
		t.Fatalf("unexpected EMSC configuration: %#v", configuration)
	}
	if configuration.EMSCWebSocketURL != "wss://www.seismicportal.eu/standing_order/websocket" {
		t.Fatalf("websocket URL=%q", configuration.EMSCWebSocketURL)
	}
	if configuration.MetricsAddress != ":9090" {
		t.Fatalf("metrics address=%q", configuration.MetricsAddress)
	}
}

func TestExampleConfigurationIsValid(t *testing.T) {
	configuration, err := Load(filepath.Join("..", "..", "config.example.toml"), "provider-worker", "kndc")
	if err != nil {
		t.Fatal(err)
	}
	if configuration.ProviderName != "kndc" || configuration.ProviderStateFile == "" {
		t.Fatalf("configuration=%+v", configuration)
	}
}

func TestLoadRejectsUnknownTOMLField(t *testing.T) {
	path := writeConfig(t, baseConfig()+"\n[app]\nunknown_setting = true\n")
	if _, err := Load(path, "core", ""); err == nil {
		t.Fatal("expected unknown TOML field error")
	}
}

func TestLoadRejectsInvalidDuration(t *testing.T) {
	path := writeConfig(t, baseConfig()+"\n[providers.kndc]\npoll_interval = \"later\"\n")
	if _, err := Load(path, "worker", ""); err == nil {
		t.Fatal("expected invalid duration error")
	}
}

func TestLoadProviderWorkerDoesNotRequireCoreDatabaseOrSecrets(t *testing.T) {
	path := writeConfig(t, `
[kafka]
brokers = ["kafka-1:9092", "kafka-2:9092"]

[providers.kndc]
state_file = "/var/lib/shaker/kndc-state.json"
`)
	configuration, err := Load(path, "provider-worker", "kndc")
	if err != nil {
		t.Fatal(err)
	}
	if configuration.ProviderName != "kndc" || len(configuration.KafkaBrokers) != 2 ||
		configuration.KafkaClientID != "shaker-provider-worker-kndc" {
		t.Fatalf("configuration=%+v", configuration)
	}
}

func TestLoadCoreRequiresKafkaButNotNotificationSecrets(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://environment-must-not-win/ignored")
	t.Setenv("KAFKA_BROKERS", "environment-must-not-win:9092")
	path := writeConfig(t, `
[database]
url = "postgres://earthquake:earthquake@localhost/earthquake"

[kafka]
brokers = ["kafka:9092"]
`)
	configuration, err := Load(path, "core", "")
	if err != nil {
		t.Fatal(err)
	}
	if configuration.KafkaCoreConsumerGroup != "shaker-core-v1" {
		t.Fatalf("consumer group=%q", configuration.KafkaCoreConsumerGroup)
	}
	if configuration.DatabaseURL != "postgres://earthquake:earthquake@localhost/earthquake" ||
		len(configuration.KafkaBrokers) != 1 || configuration.KafkaBrokers[0] != "kafka:9092" {
		t.Fatalf("environment overrode TOML: %+v", configuration)
	}
}

func TestLoadNotificationRequiresKafka(t *testing.T) {
	path := writeConfig(t, strings.Replace(baseConfig(), `brokers = ["kafka:9092"]`, "brokers = []", 1))
	if _, err := Load(path, "notification", ""); err == nil {
		t.Fatal("expected missing Kafka brokers error")
	}
	configuration, err := Load(writeConfig(t, baseConfig()), "notification", "")
	if err != nil {
		t.Fatal(err)
	}
	if configuration.KafkaNotificationConsumerGroup != "shaker-notification-v1" {
		t.Fatalf("consumer group=%q", configuration.KafkaNotificationConsumerGroup)
	}
}

func TestLoadAdministrationConfiguration(t *testing.T) {
	path := writeConfig(t, baseConfig()+`
[administration]
enabled = true
host = "admin.example.com"
development_email = "admin@example.com"
bootstrap_owners = ["admin@example.com", "operator@example.com"]
`)
	configuration, err := Load(path, "api", "")
	if err != nil {
		t.Fatal(err)
	}
	if !configuration.AdminEnabled || len(configuration.AdminBootstrapOwners) != 2 {
		t.Fatalf("unexpected administration configuration: %#v", configuration)
	}
}

func TestLoadRequiresCloudflareAccessOutsideDevelopment(t *testing.T) {
	path := writeConfig(t, baseConfig()+`
[app]
environment = "production"

[administration]
enabled = true
host = "admin.example.com"
bootstrap_owners = ["admin@example.com"]
`)
	if _, err := Load(path, "api", ""); err == nil {
		t.Fatal("expected missing Cloudflare Access configuration error")
	}
}

func TestHTTPAddressUsesTOMLWithoutFullRoleValidation(t *testing.T) {
	path := writeConfig(t, "[app]\nhttp_address = \"127.0.0.1:8181\"\n")
	address, err := HTTPAddress(path)
	if err != nil {
		t.Fatal(err)
	}
	if address != "127.0.0.1:8181" {
		t.Fatalf("address=%q", address)
	}
}

func TestLoadRejectsOversizedFile(t *testing.T) {
	path := writeConfig(t, strings.Repeat("#", maxConfigBytes+1))
	if _, err := Load(path, "core", ""); err == nil || !strings.Contains(err.Error(), "exceeds 1 MiB") {
		t.Fatalf("error=%v", err)
	}
}

func baseConfig() string {
	key := base64.StdEncoding.EncodeToString(make([]byte, 32))
	return `
[database]
url = "postgres://earthquake:earthquake@localhost/earthquake"

[kafka]
brokers = ["kafka:9092"]

[api]
admin_api_key = "test-admin-key"

[security]
encryption_key = "` + key + `"
`
}

func writeConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "shaker.toml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
