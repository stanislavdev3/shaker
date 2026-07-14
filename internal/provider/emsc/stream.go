package emsc

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/coder/websocket"

	"github.com/example/earthquake-service/internal/domain/earthquake"
)

type EventHandler func(context.Context, earthquake.Event) error

type StreamMetrics interface {
	SetConnected(bool)
	MessageHandled()
}

type Stream struct {
	url, userAgent   string
	httpClient       *http.Client
	log              *slog.Logger
	handshakeTimeout time.Duration
	readLimit        int64
	pingInterval     time.Duration
	minBackoff       time.Duration
	maxBackoff       time.Duration
	now              func() time.Time
	metrics          StreamMetrics
}

func NewStream(url, userAgent string, httpTimeout, pingInterval time.Duration, readLimit int64, log *slog.Logger, metrics StreamMetrics) *Stream {
	return &Stream{
		url: url, userAgent: userAgent, httpClient: &http.Client{}, log: log, handshakeTimeout: httpTimeout,
		readLimit: readLimit, pingInterval: pingInterval, minBackoff: time.Second, maxBackoff: time.Minute, now: time.Now, metrics: metrics,
	}
}

func (s *Stream) Run(ctx context.Context, handler EventHandler) {
	attempt := 0
	for ctx.Err() == nil {
		connectedAt := s.now()
		err := s.listen(ctx, handler)
		if ctx.Err() != nil {
			return
		}
		if s.now().Sub(connectedAt) >= time.Minute {
			attempt = 0
		}
		delay := s.reconnectDelay(attempt)
		if attempt < 30 {
			attempt++
		}
		s.log.Warn("EMSC WebSocket disconnected", "error", err, "reconnect_in", delay)
		if err := wait(ctx, delay); err != nil {
			return
		}
	}
}

func (s *Stream) listen(ctx context.Context, handler EventHandler) error {
	headers := http.Header{}
	headers.Set("User-Agent", s.userAgent)
	dialCtx, cancelDial := context.WithTimeout(ctx, s.handshakeTimeout)
	connection, response, err := websocket.Dial(dialCtx, s.url, &websocket.DialOptions{HTTPClient: s.httpClient, HTTPHeader: headers})
	cancelDial()
	if err != nil {
		if response != nil {
			if response.Body != nil {
				_ = response.Body.Close()
			}
			return fmt.Errorf("connect to EMSC WebSocket: HTTP %d: %w", response.StatusCode, err)
		}
		return fmt.Errorf("connect to EMSC WebSocket: %w", err)
	}
	defer func() { _ = connection.CloseNow() }()
	if s.metrics != nil {
		s.metrics.SetConnected(true)
		defer s.metrics.SetConnected(false)
	}
	connection.SetReadLimit(s.readLimit)
	s.log.Info("EMSC WebSocket connected")

	connectionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	pingErrors := make(chan error, 1)
	go s.ping(connectionCtx, connection, pingErrors)
	for {
		messageType, data, err := connection.Read(connectionCtx)
		if err != nil {
			return err
		}
		select {
		case err := <-pingErrors:
			return err
		default:
		}
		if messageType != websocket.MessageText {
			s.log.Warn("ignore non-text EMSC WebSocket frame", "message_type", messageType)
			continue
		}
		event, err := ParseStandingOrder(data)
		if err != nil {
			s.log.Warn("ignore invalid EMSC WebSocket frame", "error", err)
			continue
		}
		if err := s.handle(connectionCtx, handler, event); err != nil {
			return fmt.Errorf("persist EMSC WebSocket event %s: %w", event.ExternalID, err)
		}
		if s.metrics != nil {
			s.metrics.MessageHandled()
		}
	}
}

func (s *Stream) ping(ctx context.Context, connection *websocket.Conn, errors chan<- error) {
	ticker := time.NewTicker(s.pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pingCtx, cancel := context.WithTimeout(ctx, s.pingInterval)
			err := connection.Ping(pingCtx)
			cancel()
			if err != nil {
				select {
				case errors <- fmt.Errorf("ping EMSC WebSocket: %w", err):
				default:
				}
				_ = connection.CloseNow()
				return
			}
		}
	}
}

func (s *Stream) handle(ctx context.Context, handler EventHandler, event earthquake.Event) error {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if err = handler(ctx, event); err == nil {
			return nil
		}
		if attempt < 2 {
			if waitErr := wait(ctx, time.Duration(attempt+1)*time.Second); waitErr != nil {
				return waitErr
			}
		}
	}
	return err
}

func (s *Stream) reconnectDelay(attempt int) time.Duration {
	delay := s.minBackoff
	for i := 0; i < attempt && delay < s.maxBackoff; i++ {
		delay *= 2
		if delay > s.maxBackoff {
			delay = s.maxBackoff
		}
	}
	return delay/2 + time.Duration(rand.Int64N(int64(delay/2)+1))
}
