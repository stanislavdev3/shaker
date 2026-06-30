package httpapi

import (
	"context"
	"embed"
	"io/fs"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/example/earthquake-service/internal/realtime"
)

// The Vite production build is written here by `make frontend`; `all:` includes
// the committed .gitkeep so the package compiles before the first build.
//
//go:embed all:web/dist
var webAssets embed.FS

// frontendHandler serves the embedded SPA. Real asset requests are served from
// disk; any other path falls back to index.html so client-side routes such as
// /event/{id} resolve on a full-page load or refresh.
func frontendHandler() http.Handler {
	root, err := fs.Sub(webAssets, "web/dist")
	if err != nil {
		panic(err)
	}
	fileServer := http.FileServer(http.FS(root))
	index, _ := fs.ReadFile(root, "index.html")
	serveIndex := func(w http.ResponseWriter) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(index)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/")
		if name == "" {
			serveIndex(w)
			return
		}
		f, err := root.Open(name)
		if err != nil {
			serveIndex(w)
			return
		}
		_ = f.Close()
		fileServer.ServeHTTP(w, r)
	})
}

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
