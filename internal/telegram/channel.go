package telegram

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/example/earthquake-service/internal/domain/notification"
)

type GlobalChannelRepository interface {
	UpsertGlobalTelegramChannel(context.Context, int64, string, time.Time) (notification.Subscription, error)
}

type GlobalChannelAPI interface {
	GetChat(context.Context, string) (Chat, error)
	GetMe(context.Context) (User, error)
	GetChatMember(context.Context, int64, int64) (ChatMember, error)
}

func RegisterGlobalChannel(ctx context.Context, repo GlobalChannelRepository, api GlobalChannelAPI,
	username string, now time.Time) (notification.Subscription, error) {
	if username == "" {
		return notification.Subscription{}, errors.New("global channel username is empty")
	}
	chat, err := api.GetChat(ctx, username)
	if err != nil {
		return notification.Subscription{}, fmt.Errorf("resolve global channel: %w", err)
	}
	if chat.Type != "channel" || chat.ID == 0 {
		return notification.Subscription{}, fmt.Errorf("%s is not a Telegram channel", username)
	}
	bot, err := api.GetMe(ctx)
	if err != nil {
		return notification.Subscription{}, fmt.Errorf("resolve Telegram bot identity: %w", err)
	}
	if bot.ID == 0 {
		return notification.Subscription{}, errors.New("telegram bot identity has no user ID")
	}
	member, err := api.GetChatMember(ctx, chat.ID, bot.ID)
	if err != nil {
		return notification.Subscription{}, fmt.Errorf("inspect global channel permissions: %w", err)
	}
	if member.Status != "creator" && member.Status != "administrator" {
		return notification.Subscription{}, errors.New("telegram bot is not a global channel administrator")
	}
	if member.Status == "administrator" && (!member.CanPostMessages || !member.CanEditMessages) {
		return notification.Subscription{}, errors.New("telegram bot requires post and edit message permissions in the global channel")
	}
	resolvedUsername := username
	if chat.Username != "" {
		resolvedUsername = "@" + strings.TrimPrefix(chat.Username, "@")
	}
	subscription, err := repo.UpsertGlobalTelegramChannel(ctx, chat.ID, resolvedUsername, now)
	if err != nil {
		return notification.Subscription{}, fmt.Errorf("persist global channel subscription: %w", err)
	}
	return subscription, nil
}
