package httpapi

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestCursorRoundTripAndTamperDetection(t *testing.T) {
	key := []byte("01234567890123456789012345678901")
	now := time.Now().UTC().Truncate(time.Microsecond)
	value, err := encodeCursor(cursorPayload{Version: 1, Sort: "occurred_at_desc", OccurredAt: &now, ID: uuid.New()}, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeCursor(value, "occurred_at_desc", key); err != nil {
		t.Fatal(err)
	}
	if _, err := decodeCursor(value, "occurred_at_asc", key); err == nil {
		t.Fatal("accepted different sort")
	}
	b := []byte(value)
	b[len(b)-1] ^= 1
	if _, err := decodeCursor(string(b), "occurred_at_desc", key); err == nil {
		t.Fatal("accepted tampered cursor")
	}
}
