package gatekeep

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func newTestRedisLimiter(t *testing.T, capacity, rate float64) (context.Context, *RedisLimiter, string) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	ctx := context.Background()
	id := t.Name()
	client.Del(ctx, "gatekeep:"+id)

	t.Cleanup(func() { client.Close() })

	return ctx, NewRedisLimiter(client, capacity, rate), id
}

func TestRedisAllow_BurstThenDeny(t *testing.T) {
	ctx, l, id := newTestRedisLimiter(t, 5, 1)

	currentTime := time.Now()
	l.now = func() time.Time { return currentTime }

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

func TestRedisAllow_RefillOverTime(t *testing.T) {
	ctx, l, id := newTestRedisLimiter(t, 5, 1)

	currentTime := time.Now()
	l.now = func() time.Time { return currentTime }

	// drain all tokens
	for range 5 {
		_, _, err := l.Allow(ctx, id)
		if err != nil {
			t.Fatalf("unexpected error occurred: %v", err)
		}
	}

	allowed, _, err := l.Allow(ctx, id)
	if err != nil {
		t.Fatalf("unexpected error occured: %v", err)
	}
	if allowed {
		t.Fatalf("6th request: expected denial, instead got allowed")
	}

	//move time 3 seconds ahead
	currentTime = currentTime.Add(3 * time.Second)
	for i := range 3 {
		allowed, _, err := l.Allow(ctx, id)
		if err != nil {
			t.Fatalf("unexpected error occured: %v", err)
		}
		if !allowed {
			t.Fatalf("post-refill request %d: expected allowed", i+1)
		}
	}

	// 4th req should fail after refill
	allowed, _, err = l.Allow(ctx, id)
	if err != nil {
		t.Fatalf("unexpected error occured: %v", err)
	}
	if allowed {
		t.Fatal("expected denial after consuming 3 refilled tokens")
	}
}
