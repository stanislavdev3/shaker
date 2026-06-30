package realtime

import (
	"context"
	"testing"
	"time"
)

func TestHubPublishesToSubscriber(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hub := NewHub()
	go hub.Run(ctx)
	clientContext, disconnect := context.WithCancel(ctx)
	messages := hub.Subscribe(clientContext)
	hub.Publish(Message{Type: "earthquake_changed", Data: "event"})
	select {
	case message := <-messages:
		if message.Type != "earthquake_changed" || message.Data != "event" {
			t.Fatalf("unexpected message: %+v", message)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message")
	}
	disconnect()
}
