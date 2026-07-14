package telegram

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/example/earthquake-service/internal/domain/notification"
)

type channelRepositoryStub struct {
	chatID   int64
	username string
}

func (r *channelRepositoryStub) UpsertGlobalTelegramChannel(_ context.Context, chatID int64, username string, _ time.Time) (notification.Subscription, error) {
	r.chatID, r.username = chatID, username
	return notification.Subscription{Name: "Global earthquake channel"}, nil
}

type channelAPIStub struct {
	chat   Chat
	user   User
	member ChatMember
	err    error
}

func (a channelAPIStub) GetChat(context.Context, string) (Chat, error) { return a.chat, a.err }
func (a channelAPIStub) GetMe(context.Context) (User, error)           { return a.user, a.err }
func (a channelAPIStub) GetChatMember(context.Context, int64, int64) (ChatMember, error) {
	return a.member, a.err
}

func TestRegisterGlobalChannel(t *testing.T) {
	repo := &channelRepositoryStub{}
	api := channelAPIStub{
		chat: Chat{ID: -10042, Type: "channel", Username: "eqmonitor"}, user: User{ID: 7},
		member: ChatMember{Status: "administrator", CanPostMessages: true, CanEditMessages: true},
	}
	if _, err := RegisterGlobalChannel(context.Background(), repo, api, "@eqmonitor", time.Now()); err != nil {
		t.Fatal(err)
	}
	if repo.chatID != -10042 || repo.username != "@eqmonitor" {
		t.Fatalf("chatID=%d username=%q", repo.chatID, repo.username)
	}
}

func TestRegisterGlobalChannelRequiresEditPermission(t *testing.T) {
	api := channelAPIStub{
		chat: Chat{ID: -10042, Type: "channel"}, user: User{ID: 7},
		member: ChatMember{Status: "administrator", CanPostMessages: true},
	}
	_, err := RegisterGlobalChannel(context.Background(), &channelRepositoryStub{}, api, "@eqmonitor", time.Now())
	if err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
}
