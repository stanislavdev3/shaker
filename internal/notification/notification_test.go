package notification

import (
	"context"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestSignature(t *testing.T) {
	got := Signature([]byte("secret"), []byte(`{"x":1}`), 123)
	if !strings.HasPrefix(got, "sha256=") || len(got) != 71 {
		t.Fatalf("invalid signature %q", got)
	}
	if got != Signature([]byte("secret"), []byte(`{"x":1}`), 123) {
		t.Fatal("signature not deterministic")
	}
}
func TestUnsafeIP(t *testing.T) {
	for _, value := range []string{"127.0.0.1", "10.0.0.1", "169.254.169.254", "::1", "ff02::1"} {
		if !unsafeIP(net.ParseIP(value)) {
			t.Fatalf("%s accepted", value)
		}
	}
	if unsafeIP(net.ParseIP("8.8.8.8")) {
		t.Fatal("public IP rejected")
	}
}
func TestRetryDelayBounds(t *testing.T) {
	for i := 1; i < 20; i++ {
		d := retryDelay(i)
		if d < 2500*time.Millisecond || d > time.Hour {
			t.Fatalf("attempt %d delay %s", i, d)
		}
	}
}
func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 6, 29, 10, 0, 0, 0, time.UTC)
	if got := parseRetryAfter("120", now); got != 2*time.Minute {
		t.Fatalf("got %s", got)
	}
	if got := parseRetryAfter(now.Add(time.Minute).Format(http.TimeFormat), now); got != time.Minute {
		t.Fatalf("got %s", got)
	}
}
func TestValidateURLRejectsCredentialsAndScheme(t *testing.T) {
	for _, value := range []string{"ftp://example.com/x", "https://user:pass@example.com/x"} {
		if _, _, err := ValidateURL(context.Background(), value, false); err == nil {
			t.Fatalf("accepted %s", value)
		}
	}
}

func TestTelegramMessage(t *testing.T) {
	message, err := telegramMessage([]byte(`{
		"type":"new_event",
		"lifecycle":"preliminary",
		"sources":{"usgs":"https://example.com/usgs"},
		"language":"en",
		"shaking":{"mean_mmi":4.2,"lower_mmi":3.3,"upper_mmi":5.1},
		"earthquake":{"source":"emsc","magnitude":5.2,"depth_km":12.4,"distance_km":123.6,"latitude":42.8,"longitude":74.6,"place":"Test & region","occurred_at":"2026-07-13T10:00:00Z","detail_url":"https://example.com/emsc"}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"🟡 Preliminary earthquake", "Magnitude: <b>5.2</b>", "Expected at your location: <b>IV — light</b>", "Likely range: III–V", "Distance: 124 km", "Depth: 12.4 km", "Test &amp; region", `<tg-time unix="1783936800" format="wDt">`, `<a href="https://example.com/usgs">USGS</a> | <a href="https://example.com/emsc">EMSC</a>`} {
		if !strings.Contains(message.text, expected) {
			t.Fatalf("message %q does not contain %q", message.text, expected)
		}
	}
	if message.latitude == nil || message.longitude == nil || *message.latitude != 42.8 || *message.longitude != 74.6 {
		t.Fatalf("coordinates=%v,%v", message.latitude, message.longitude)
	}
	if message.locationButton != "🗺 Show location" {
		t.Fatalf("location button=%q", message.locationButton)
	}
}

func TestTelegramMessageRussian(t *testing.T) {
	message, err := telegramMessage([]byte(`{
		"type":"new_event","lifecycle":"confirmed","language":"ru",
		"shaking":{"mean_mmi":5.0,"lower_mmi":4.1,"upper_mmi":5.9},
		"earthquake":{"magnitude":6.1,"distance_km":80}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"✅ Подтверждённое землетрясение", "Магнитуда: <b>6.1</b>", "Ожидается у вас: <b>V — умеренные</b>", "Вероятный диапазон: IV–VI", "Расстояние: 80 км"} {
		if !strings.Contains(message.text, expected) {
			t.Fatalf("message %q does not contain %q", message.text, expected)
		}
	}
	if message.locationButton != "🗺 Показать место" {
		t.Fatalf("location button=%q", message.locationButton)
	}
}
