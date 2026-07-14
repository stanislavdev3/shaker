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
		"earthquake":{"magnitude":5.2,"depth_km":12.4,"distance_km":123.6,"place":"Test region","occurred_at":"2026-07-13T10:00:00Z","source_url":"https://example.com/event"}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"🟡 Preliminary earthquake", "Magnitude: 5.2", "Distance: 124 km", "Depth: 12.4 km", "Test region"} {
		if !strings.Contains(message, expected) {
			t.Fatalf("message %q does not contain %q", message, expected)
		}
	}
}
