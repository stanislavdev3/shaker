package httpapi

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

type cursorPayload struct {
	Version    int        `json:"v"`
	Sort       string     `json:"sort"`
	OccurredAt *time.Time `json:"occurred_at,omitempty"`
	Magnitude  *float64   `json:"magnitude,omitempty"`
	ID         uuid.UUID  `json:"id"`
}

func encodeCursor(c cursorPayload, key []byte) (string, error) {
	body, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, key)
	mac.Write(body)
	out := append(body, mac.Sum(nil)...)
	return base64.RawURLEncoding.EncodeToString(out), nil
}
func decodeCursor(value, sort string, key []byte) (cursorPayload, error) {
	var c cursorPayload
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(raw) <= sha256.Size {
		return c, errors.New("invalid cursor")
	}
	body, sig := raw[:len(raw)-sha256.Size], raw[len(raw)-sha256.Size:]
	mac := hmac.New(sha256.New, key)
	mac.Write(body)
	if !hmac.Equal(sig, mac.Sum(nil)) {
		return c, errors.New("invalid cursor signature")
	}
	if err := json.Unmarshal(body, &c); err != nil || c.Version != 1 || c.Sort != sort || c.ID == uuid.Nil {
		return c, errors.New("invalid cursor")
	}
	return c, nil
}
