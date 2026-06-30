package realtime

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/example/earthquake-service/internal/repository/postgres"
)

type Message struct {
	Type string `json:"type"`
	Data any    `json:"data,omitempty"`
}

type Hub struct {
	subscribe   chan chan Message
	unsubscribe chan chan Message
	publish     chan Message
}

func NewHub() *Hub {
	return &Hub{
		subscribe: make(chan chan Message), unsubscribe: make(chan chan Message),
		publish: make(chan Message, 256),
	}
}

func (h *Hub) Run(ctx context.Context) {
	clients := map[chan Message]struct{}{}
	for {
		select {
		case <-ctx.Done():
			for client := range clients {
				close(client)
			}
			return
		case client := <-h.subscribe:
			clients[client] = struct{}{}
		case client := <-h.unsubscribe:
			if _, ok := clients[client]; ok {
				delete(clients, client)
				close(client)
			}
		case message := <-h.publish:
			for client := range clients {
				select {
				case client <- message:
				default:
					// A slow browser receives a later resync signal instead of
					// applying unbounded backpressure to ingestion.
				}
			}
		}
	}
}

func (h *Hub) Subscribe(ctx context.Context) <-chan Message {
	client := make(chan Message, 32)
	select {
	case h.subscribe <- client:
	case <-ctx.Done():
		close(client)
		return client
	}
	go func() {
		<-ctx.Done()
		h.unsubscribe <- client
	}()
	return client
}

func (h *Hub) Publish(message Message) {
	select {
	case h.publish <- message:
	default:
	}
}

type Listener struct {
	pool *pgxpool.Pool
	repo *postgres.Repository
	hub  *Hub
	log  *slog.Logger
}

func NewListener(pool *pgxpool.Pool, repo *postgres.Repository, hub *Hub, log *slog.Logger) *Listener {
	return &Listener{pool: pool, repo: repo, hub: hub, log: log}
}

func (l *Listener) Run(ctx context.Context) {
	for ctx.Err() == nil {
		if err := l.listen(ctx); err != nil && ctx.Err() == nil {
			l.log.Error("realtime database listener failed", "error", err)
			timer := time.NewTimer(time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
			case <-timer.C:
			}
		}
	}
}

func (l *Listener) listen(ctx context.Context) error {
	conn, err := l.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `LISTEN earthquake_changes; LISTEN notification_delivery_changes`); err != nil {
		return err
	}
	for {
		notice, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			return err
		}
		switch notice.Channel {
		case "earthquake_changes":
			var event struct {
				Operation    string    `json:"operation"`
				EarthquakeID uuid.UUID `json:"earthquake_id"`
				Version      int64     `json:"version"`
			}
			if err := json.Unmarshal([]byte(notice.Payload), &event); err != nil {
				l.log.Warn("invalid earthquake notification payload", "error", err)
				continue
			}
			earthquake, err := l.repo.Get(ctx, event.EarthquakeID)
			if err != nil {
				l.log.Warn("load realtime earthquake", "earthquake_id", event.EarthquakeID, "error", err)
				continue
			}
			l.hub.Publish(Message{Type: "earthquake_changed", Data: map[string]any{
				"operation": event.Operation, "earthquake": earthquake,
			}})
		case "notification_delivery_changes":
			// The public stream contains only a resync hint. Delivery and
			// subscription details remain behind administrative authentication.
			l.hub.Publish(Message{Type: "notification_changed"})
		}
	}
}
