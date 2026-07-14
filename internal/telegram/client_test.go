package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestClientGetUpdatesAndSendMessage(t *testing.T) {
	var sentChatID int64
	var keyboardRemoved bool
	var editedMessageID int64
	var alertParseMode string
	var alertCallbackData string
	var locationReplyID int64
	var answeredCallbackID string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/bottest/getUpdates":
			_, _ = w.Write([]byte(`{"ok":true,"result":[{"update_id":7,"message":{"chat":{"id":42},"text":"4.5"}}]}`))
		case "/bottest/sendMessage":
			var request struct {
				ChatID      int64  `json:"chat_id"`
				ParseMode   string `json:"parse_mode"`
				ReplyMarkup struct {
					RemoveKeyboard bool `json:"remove_keyboard"`
					InlineKeyboard [][]struct {
						CallbackData string `json:"callback_data"`
					} `json:"inline_keyboard"`
				} `json:"reply_markup"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatal(err)
			}
			sentChatID = request.ChatID
			keyboardRemoved = request.ReplyMarkup.RemoveKeyboard
			if request.ParseMode != "" {
				alertParseMode = request.ParseMode
				alertCallbackData = request.ReplyMarkup.InlineKeyboard[0][0].CallbackData
			}
			_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":99,"chat":{"id":42}}}`))
		case "/bottest/sendLocation":
			var request struct {
				Latitude        float64 `json:"latitude"`
				Longitude       float64 `json:"longitude"`
				ReplyParameters struct {
					MessageID int64 `json:"message_id"`
				} `json:"reply_parameters"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatal(err)
			}
			if request.Latitude != 42.8 || request.Longitude != 74.6 {
				t.Errorf("location=%v,%v", request.Latitude, request.Longitude)
			}
			locationReplyID = request.ReplyParameters.MessageID
			_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":100}}`))
		case "/bottest/answerCallbackQuery":
			var request struct {
				CallbackID string `json:"callback_query_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatal(err)
			}
			answeredCallbackID = request.CallbackID
			_, _ = w.Write([]byte(`{"ok":true,"result":true}`))
		case "/bottest/editMessageText":
			var request struct {
				MessageID int64 `json:"message_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatal(err)
			}
			editedMessageID = request.MessageID
			_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":99}}`))
		case "/bottest/getChat":
			_, _ = w.Write([]byte(`{"ok":true,"result":{"id":-10042,"type":"channel","username":"eqmonitor"}}`))
		case "/bottest/getMe":
			_, _ = w.Write([]byte(`{"ok":true,"result":{"id":7}}`))
		case "/bottest/getChatMember":
			_, _ = w.Write([]byte(`{"ok":true,"result":{"status":"administrator","can_post_messages":true,"can_edit_messages":true}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := NewClient(server.URL, "test", time.Second, 1024)
	updates, err := client.GetUpdates(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) != 1 || updates[0].ID != 7 || updates[0].Message.Chat.ID != 42 {
		t.Fatalf("unexpected updates: %+v", updates)
	}
	if err := client.SendMessage(context.Background(), 42, "hello"); err != nil {
		t.Fatal(err)
	}
	if sentChatID != 42 {
		t.Fatalf("sent chat ID = %d", sentChatID)
	}
	if err := client.SendMessageRemovingKeyboard(context.Background(), 42, "location saved"); err != nil {
		t.Fatal(err)
	}
	if !keyboardRemoved {
		t.Fatal("remove_keyboard was not sent")
	}
	latitude, longitude := 42.8, 74.6
	messageID, err := client.SendAlertMessage(context.Background(), 42, "<b>alert</b>", &latitude, &longitude)
	if err != nil || messageID != 99 {
		t.Fatalf("messageID=%d err=%v", messageID, err)
	}
	if alertParseMode != "HTML" || alertCallbackData != "loc:42.800000:74.600000" {
		t.Fatalf("parse mode=%q callback=%q", alertParseMode, alertCallbackData)
	}
	if err := client.AnswerCallbackQuery(context.Background(), "callback-1", ""); err != nil {
		t.Fatal(err)
	}
	if err := client.SendLocation(context.Background(), 42, messageID, latitude, longitude); err != nil {
		t.Fatal(err)
	}
	if answeredCallbackID != "callback-1" || locationReplyID != messageID {
		t.Fatalf("answered callback=%q location reply=%d", answeredCallbackID, locationReplyID)
	}
	if err := client.EditAlertMessage(context.Background(), 42, messageID, "updated", &latitude, &longitude); err != nil {
		t.Fatal(err)
	}
	if editedMessageID != 99 {
		t.Fatalf("edited message ID=%d", editedMessageID)
	}
	chat, err := client.GetChat(context.Background(), "@eqmonitor")
	if err != nil || chat.ID != -10042 || chat.Type != "channel" {
		t.Fatalf("chat=%+v err=%v", chat, err)
	}
	bot, err := client.GetMe(context.Background())
	if err != nil || bot.ID != 7 {
		t.Fatalf("bot=%+v err=%v", bot, err)
	}
	member, err := client.GetChatMember(context.Background(), chat.ID, bot.ID)
	if err != nil || !member.CanPostMessages || !member.CanEditMessages {
		t.Fatalf("member=%+v err=%v", member, err)
	}
}

func TestAlertLocationKeyboardIsPrivateOnly(t *testing.T) {
	latitude, longitude := 42.8, 74.6
	if alertLocationKeyboard(-10042, &latitude, &longitude) != nil {
		t.Fatal("channel alert must not have a location button")
	}
}

func TestClientPreservesTelegramRetryAfter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"ok":false,"description":"Too Many Requests","parameters":{"retry_after":17}}`))
	}))
	defer server.Close()
	client := NewClient(server.URL, "test", time.Second, 1024)
	_, err := client.SendAlertMessage(context.Background(), 42, "alert", nil, nil)
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.RetryAfter() != 17*time.Second {
		t.Fatalf("error=%v", err)
	}
}
