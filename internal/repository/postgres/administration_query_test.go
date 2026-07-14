package postgres

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/example/earthquake-service/internal/administration"
)

func TestAdminNotificationQueryFiltersClassAndCursor(t *testing.T) {
	before := time.Unix(100, 0).UTC()
	id := uuid.New()
	query, args := adminNotificationQuery(administration.NotificationFilter{
		PageFilter:    administration.PageFilter{BeforeAt: &before, BeforeID: &id, Limit: 25},
		DeliveryClass: "telegram_private",
	})
	for _, expected := range []string{"delivery_class=$1", "(created_at,id)<($2,$3)", "LIMIT $4"} {
		if !strings.Contains(query, expected) {
			t.Fatalf("query is missing %q: %s", expected, query)
		}
	}
	if len(args) != 4 || args[0] != "telegram_private" || args[1] != before || args[2] != id || args[3] != 25 {
		t.Fatalf("unexpected arguments: %#v", args)
	}
}
