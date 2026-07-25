package gatekeep

import (
	"sync"
	"time"
)

type bucket struct {
	tokens     float64   // current token count
	lastRefill time.Time // timestamp of when we last computed tokens
}

type Limiter struct {
	mu       sync.Mutex
	buckets  map[string]*bucket
	capacity float64
	rate     float64
	now      func() time.Time
}

func NewLimiter(capacity, ratePerSec float64) *Limiter {
	return &Limiter{
		buckets:  make(map[string]*bucket),
		capacity: capacity,
		rate:     ratePerSec,
		now:      time.Now,
	}
}

func (l *Limiter) Allow(id string) (allowed bool, retryAfter time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()

	// fetch or lazily create this user's buckets
	b, ok := l.buckets[id]
	if !ok {
		b = &bucket{tokens: l.capacity, lastRefill: now}
		l.buckets[id] = b
	}

	// lazy refill: add tokens for the time elapsed since we last looked
	elapsed := now.Sub(b.lastRefill).Seconds()
	b.tokens = min(l.capacity, b.tokens+elapsed*l.rate)
	b.lastRefill = now

	// decision on approval
	if b.tokens >= 1 {
		b.tokens -= 1
		return true, 0
	}

	// denied: compute how long untill one full token is available again
	needed := 1 - b.tokens
	wait := time.Duration(needed / l.rate * float64(time.Second))
	return false, wait
}
