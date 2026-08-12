package gatekeep

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedisAllow_BurstThenDeny(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.Close()

	ctx := context.Background()

	l := NewRedisLimiter(client, 5, 1) // redis takes a client, so three args

	currentTime := time.Now()
	l.now = func() time.Time { return currentTime }

	id := t.Name()
	client.Del(ctx, "gatekeep:"+id)

	for i := range 5 {
		allowed, _, err := l.Allow(ctx, id)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !allowed {
			t.Fatalf("request %d: expected allowed, got rejected", i+1)
		}
	}

	allowed, retryAfter, err := l.Allow(ctx, id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)

	}
	if allowed {
		t.Fatalf("6th request: expected denial, got approved")
	}

	if retryAfter <= 0 || retryAfter > time.Second {
		t.Fatalf("retryAfter = %v, want (0, 1s]", retryAfter)
	}
}
