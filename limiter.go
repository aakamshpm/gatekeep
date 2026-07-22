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
