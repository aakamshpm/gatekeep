package gatekeep

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAllow_BurstThenDeny(t *testing.T) {
	// capactity: 5, refill rate: 1 token/sec
	l := NewLimiter(5, 1)

	currentTime := time.Now()
	l.now = func() time.Time { return currentTime }

	// 5 requests to consume capacity of 5 tokens
	for i := range 5 {
		allowed, _ := l.Allow("user1")
		if !allowed {
			t.Fatalf("request %d: expected allowed, got denied", i+1)
		}
	}

	// 6th request, should fail
	allowed, retryAfter := l.Allow("user1")
	if allowed {
		t.Fatalf("6th request: expected denied, got allowed")
	}

	if retryAfter <= 0 || retryAfter > time.Second {
		t.Fatalf("retryAfter = %v, want (0, 1s]", retryAfter)
	}

}

func TestAllow_RefillOverTime(t *testing.T) {
	l := NewLimiter(5, 1)

	currentTime := time.Now()
	l.now = func() time.Time { return currentTime }

	// drain all 5 tokens
	for range 5 {
		l.Allow("user1")
	}

	if allowed, _ := l.Allow("user1"); allowed {
		t.Fatalf("expected denial after draining bucket")
	}

	// advance time by 3 seconds
	currentTime = currentTime.Add(3 * time.Second)
	for i := range 3 {
		if allowed, _ := l.Allow("user1"); !allowed {
			t.Fatalf("post-refill request %d: expected allowed", i+1)
		}
	}

	// 4th request must fail after refill
	if allowed, _ := l.Allow("user1"); allowed {
		t.Fatal("expected denial after consuming 3 refilled tokens")
	}
}

func TestAllow_CapsAtCapacity(t *testing.T) {
	l := NewLimiter(5, 1)

	currentTime := time.Now()
	l.now = func() time.Time { return currentTime }

	// drain the bucket.
	for range 5 {
		l.Allow("user1")
	}

	// idle for 1 hour
	currentTime = currentTime.Add(time.Hour)

	// Only 5 requests should pass — burst is bounded by capacity.
	for i := range 5 {
		if allowed, _ := l.Allow("user1"); !allowed {
			t.Fatalf("request %d after idle: expected allowed", i+1)
		}
	}

	// The 6th fails — capacity is the ceiling, no matter how long idle.
	if allowed, _ := l.Allow("user1"); allowed {
		t.Fatal("expected denial: burst must be capped at capacity")
	}
}

func TestAllow_ConcurrentNoOverAdmission(t *testing.T) {
	const capacity = 100
	l := NewLimiter(capacity, 1)

	currentTime := time.Now()
	l.now = func() time.Time { return currentTime }

	const goroutines = 1000
	var allowed atomic.Int64 // atomic: many goroutines increment it concurrently

	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			if ok, _ := l.Allow("user1"); ok {
				allowed.Add(1)
			}
		})
	}
	wg.Wait()

	if got := allowed.Load(); got != capacity {
		t.Fatalf("allowed = %d, want exactly %d (over-admission under concurrency)", got, capacity)
	}
}
