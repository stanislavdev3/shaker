package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type Client struct {
	baseURL   string
	http      *http.Client
	maxBytes  int64
	pollLimit time.Duration
}

type APIError struct {
	StatusCode  int
	Description string
	RetryDelay  time.Duration
}

func (e *APIError) Error() string {
	if e.Description != "" {
		return "telegram API error: " + e.Description
	}
	return fmt.Sprintf("telegram returned HTTP %d", e.StatusCode)
}

func (e *APIError) RetryAfter() time.Duration { return e.RetryDelay }

type Update struct {
	ID            int64          `json:"update_id"`
	Message       *Message       `json:"message"`
	CallbackQuery *CallbackQuery `json:"callback_query"`
}

type CallbackQuery struct {
	ID      string   `json:"id"`
	Data    string   `json:"data"`
	Message *Message `json:"message"`
}

type Message struct {
	ID       int64     `json:"message_id"`
	Text     string    `json:"text"`
	Chat     Chat      `json:"chat"`
	Location *Location `json:"location"`
}

type Chat struct {
	ID       int64  `json:"id"`
	Type     string `json:"type"`
	Username string `json:"username"`
}

type User struct {
	ID int64 `json:"id"`
}

type ChatMember struct {
	Status          string `json:"status"`
	CanPostMessages bool   `json:"can_post_messages"`
	CanEditMessages bool   `json:"can_edit_messages"`
}

type Location struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

func NewClient(apiURL, token string, pollLimit time.Duration, maxBytes int64) *Client {
	return &Client{
		baseURL:   strings.TrimRight(apiURL, "/") + "/bot" + token + "/",
		http:      &http.Client{Timeout: pollLimit + 5*time.Second},
		maxBytes:  maxBytes,
		pollLimit: pollLimit,
	}
}

func (c *Client) GetUpdates(ctx context.Context, offset int64) ([]Update, error) {
	var response struct {
		Result []Update `json:"result"`
	}
	err := c.call(ctx, "getUpdates", map[string]any{
		"offset":          offset,
		"timeout":         int(c.pollLimit.Seconds()),
		"allowed_updates": []string{"message", "callback_query"},
	}, &response)
	return response.Result, err
}

func (c *Client) SendMessage(ctx context.Context, chatID int64, text string) error {
	return c.call(ctx, "sendMessage", map[string]any{"chat_id": chatID, "text": text}, nil)
}

func (c *Client) SendAlertMessage(ctx context.Context, chatID int64, text string, latitude, longitude *float64, locationButton string) (int64, error) {
	var response struct {
		Result Message `json:"result"`
	}
	request := map[string]any{
		"chat_id": chatID, "text": text, "parse_mode": "HTML",
		"link_preview_options": map[string]any{"is_disabled": true},
	}
	if keyboard := alertLocationKeyboard(chatID, latitude, longitude, locationButton); keyboard != nil {
		request["reply_markup"] = keyboard
	}
	err := c.call(ctx, "sendMessage", request, &response)
	if err != nil {
		return 0, err
	}
	if response.Result.ID <= 0 {
		return 0, errors.New("telegram sendMessage response has no message_id")
	}
	return response.Result.ID, nil
}

func alertLocationKeyboard(chatID int64, latitude, longitude *float64, label string) any {
	if chatID <= 0 || latitude == nil || longitude == nil {
		return nil
	}
	if label == "" {
		label = "🗺 Show location"
	}
	return map[string]any{"inline_keyboard": [][]map[string]any{{{
		"text": label, "callback_data": fmt.Sprintf("loc:%.6f:%.6f", *latitude, *longitude),
	}}}}
}

func (c *Client) AnswerCallbackQuery(ctx context.Context, callbackID, text string) error {
	request := map[string]any{"callback_query_id": callbackID}
	if text != "" {
		request["text"] = text
	}
	return c.call(ctx, "answerCallbackQuery", request, nil)
}

func (c *Client) SendLocation(ctx context.Context, chatID, replyToMessageID int64, latitude, longitude float64) error {
	return c.call(ctx, "sendLocation", map[string]any{
		"chat_id": chatID, "latitude": latitude, "longitude": longitude,
		"reply_parameters": map[string]any{"message_id": replyToMessageID, "allow_sending_without_reply": true},
	}, nil)
}

func (c *Client) EditAlertMessage(ctx context.Context, chatID, messageID int64, text string, latitude, longitude *float64, locationButton string) error {
	request := map[string]any{
		"chat_id": chatID, "message_id": messageID, "text": text, "parse_mode": "HTML",
		"link_preview_options": map[string]any{"is_disabled": true},
	}
	if keyboard := alertLocationKeyboard(chatID, latitude, longitude, locationButton); keyboard != nil {
		request["reply_markup"] = keyboard
	}
	err := c.call(ctx, "editMessageText", request, nil)
	if err != nil && strings.Contains(strings.ToLower(err.Error()), "message is not modified") {
		return nil
	}
	return err
}

func (c *Client) GetChat(ctx context.Context, username string) (Chat, error) {
	var response struct {
		Result Chat `json:"result"`
	}
	err := c.call(ctx, "getChat", map[string]any{"chat_id": username}, &response)
	return response.Result, err
}

func (c *Client) GetMe(ctx context.Context) (User, error) {
	var response struct {
		Result User `json:"result"`
	}
	err := c.call(ctx, "getMe", map[string]any{}, &response)
	return response.Result, err
}

func (c *Client) GetChatMember(ctx context.Context, chatID, userID int64) (ChatMember, error) {
	var response struct {
		Result ChatMember `json:"result"`
	}
	err := c.call(ctx, "getChatMember", map[string]any{"chat_id": chatID, "user_id": userID}, &response)
	return response.Result, err
}

func (c *Client) SendMessageRemovingKeyboard(ctx context.Context, chatID int64, text string) error {
	return c.call(ctx, "sendMessage", map[string]any{
		"chat_id":      chatID,
		"text":         text,
		"reply_markup": map[string]any{"remove_keyboard": true},
	}, nil)
}

func (c *Client) RequestLocation(ctx context.Context, chatID int64, text string) error {
	return c.call(ctx, "sendMessage", map[string]any{
		"chat_id": chatID,
		"text":    text,
		"reply_markup": map[string]any{
			"keyboard":          [][]map[string]any{{{"text": "Share location", "request_location": true}}},
			"resize_keyboard":   true,
			"one_time_keyboard": true,
		},
	}, nil)
}

func (c *Client) RequestChoice(ctx context.Context, chatID int64, text string, choices []string) error {
	buttons := make([][]map[string]any, 0, (len(choices)+1)/2)
	for i := 0; i < len(choices); i += 2 {
		row := []map[string]any{{"text": choices[i]}}
		if i+1 < len(choices) {
			row = append(row, map[string]any{"text": choices[i+1]})
		}
		buttons = append(buttons, row)
	}
	return c.call(ctx, "sendMessage", map[string]any{
		"chat_id": chatID,
		"text":    text,
		"reply_markup": map[string]any{
			"keyboard":          buttons,
			"resize_keyboard":   true,
			"one_time_keyboard": true,
		},
	}, nil)
}

func (c *Client) call(ctx context.Context, method string, request any, target any) error {
	body, err := json.Marshal(request)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+method, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(response.Body, c.maxBytes+1))
	if err != nil {
		return err
	}
	if int64(len(data)) > c.maxBytes {
		return errors.New("telegram response exceeds configured maximum")
	}
	var envelope struct {
		OK          bool            `json:"ok"`
		Description string          `json:"description"`
		Result      json.RawMessage `json:"result"`
		Parameters  struct {
			RetryAfter int `json:"retry_after"`
		} `json:"parameters"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return &APIError{StatusCode: response.StatusCode}
		}
		return fmt.Errorf("decode Telegram response: %w", err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 || !envelope.OK {
		return &APIError{StatusCode: response.StatusCode, Description: envelope.Description,
			RetryDelay: time.Duration(envelope.Parameters.RetryAfter) * time.Second}
	}
	if target == nil || len(envelope.Result) == 0 {
		return nil
	}
	wrapper, err := json.Marshal(map[string]json.RawMessage{"result": envelope.Result})
	if err != nil {
		return err
	}
	return json.Unmarshal(wrapper, target)
}
