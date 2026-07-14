package emsc

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/example/earthquake-service/internal/domain/earthquake"
)

type streamMetricsSpy struct {
	connected bool
	messages  int
}

func (m *streamMetricsSpy) SetConnected(connected bool) { m.connected = connected }
func (m *streamMetricsSpy) MessageHandled()             { m.messages++ }

func TestStreamListensAndIgnoresInvalidFrame(t *testing.T) {
	valid := fixture(t, "standing_order_insert.json")
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		defer func() { _ = connection.CloseNow() }()
		_ = connection.Write(request.Context(), websocket.MessageText, []byte(`{"action":"noop"}`))
		_ = connection.Write(request.Context(), websocket.MessageText, valid)
		<-request.Context().Done()
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	metrics := &streamMetricsSpy{}
	stream := NewStream("ws"+strings.TrimPrefix(server.URL, "http"), "test-agent", time.Second, time.Hour, 1<<20, slog.New(slog.NewTextHandler(io.Discard, nil)), metrics)
	var received earthquake.Event
	err := stream.listen(ctx, func(_ context.Context, event earthquake.Event) error {
		received = event
		cancel()
		return nil
	})
	if err == nil {
		t.Fatal("expected context cancellation after receiving the fixture")
	}
	if received.ExternalID != "20260714_0000123" {
		t.Fatalf("unexpected event: %#v", received)
	}
	if metrics.connected || metrics.messages != 1 {
		t.Fatalf("unexpected stream metrics: %#v", metrics)
	}
}

func TestReconnectDelayIsCapped(t *testing.T) {
	stream := NewStream("ws://example.test", "test-agent", time.Second, time.Second, 1024, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	stream.minBackoff = time.Second
	stream.maxBackoff = 8 * time.Second
	for range 100 {
		delay := stream.reconnectDelay(20)
		if delay < 4*time.Second || delay > 8*time.Second {
			t.Fatalf("delay=%s", delay)
		}
	}
}
