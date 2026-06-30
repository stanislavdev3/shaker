package httpapi

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
	"github.com/coder/websocket/wsjson"

	"github.com/example/earthquake-service/internal/realtime"
)

func TestWebSocketStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hub := realtime.NewHub()
	go hub.Run(ctx)
	server := &Server{realtime: hub, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		server.stream(&statusWriter{ResponseWriter: w, status: http.StatusOK}, r)
	}))
	defer httpServer.Close()
	connection, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(httpServer.URL, "http"), nil)
	if response != nil && response.Body != nil {
		defer func() { _ = response.Body.Close() }()
	}
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connection.CloseNow() }()
	var connected realtime.Message
	if err := wsjson.Read(ctx, connection, &connected); err != nil {
		t.Fatal(err)
	}
	if connected.Type != "connected" {
		t.Fatalf("unexpected initial message: %+v", connected)
	}
	hub.Publish(realtime.Message{Type: "earthquake_changed", Data: map[string]any{"version": 2}})
	readContext, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	var changed realtime.Message
	if err := wsjson.Read(readContext, connection, &changed); err != nil {
		t.Fatal(err)
	}
	if changed.Type != "earthquake_changed" {
		t.Fatalf("unexpected broadcast: %+v", changed)
	}
}

func TestEmbeddedFrontend(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	response := httptest.NewRecorder()
	frontendHandler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d", response.Code)
	}
	if !strings.Contains(response.Body.String(), "Earthquake Monitor") {
		t.Fatal("frontend content is missing")
	}
	if strings.Contains(response.Body.String(), "Delivery Lab") {
		t.Fatal("removed delivery lab is present")
	}
}
