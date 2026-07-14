package httpapi

import (
	"context"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/example/earthquake-service/internal/realtime"
)

func (s *Server) stream(w http.ResponseWriter, r *http.Request) {
	connection, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		CompressionMode: websocket.CompressionContextTakeover,
	})
	if err != nil {
		s.log.Warn("accept websocket", "error", err, "request_id", requestID(r))
		return
	}
	defer func() { _ = connection.CloseNow() }()
	connection.SetReadLimit(1024)
	ctx := connection.CloseRead(r.Context())
	messages := s.realtime.Subscribe(ctx)

	if err := writeRealtime(ctx, connection, realtime.Message{Type: "connected"}); err != nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case message, ok := <-messages:
			if !ok {
				return
			}
			if err := writeRealtime(ctx, connection, message); err != nil {
				return
			}
		}
	}
}

func writeRealtime(ctx context.Context, connection *websocket.Conn, message realtime.Message) error {
	writeContext, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return wsjson.Write(writeContext, connection, message)
}
