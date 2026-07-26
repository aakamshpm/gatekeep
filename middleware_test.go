package gatekeep

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestMiddleware_DeniedRequestNeverReachesHandler(t *testing.T) {
	l := NewLimiter(1, 1)

	currentTime := time.Now()
	l.now = func() time.Time { return currentTime }

	backendCalls := 0
	backend := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendCalls++
		w.WriteHeader(http.StatusOK)
	})

	identity := func(r *http.Request) string { return r.Header.Get("X-API-Key") }
	handler := l.Middleware(identity)(backend)

	newReq := func() *http.Request {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("X-API-Key", "key1")
		return req
	}

	// First request: allowed, backend runs.
	w1 := httptest.NewRecorder()
	handler.ServeHTTP(w1, newReq())
	if w1.Code != http.StatusOK {
		t.Fatalf("first request status = %d, want %d", w1.Code, http.StatusOK)
	}
	if backendCalls != 1 {
		t.Fatalf("backendCalls = %d, want 1", backendCalls)
	}

	// Second request: denied. Backend must NOT run.
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, newReq())
	if w2.Code != http.StatusTooManyRequests {
		t.Fatalf("second request status = %d, want %d", w2.Code, http.StatusTooManyRequests)
	}
	if backendCalls != 1 {
		t.Fatalf("backendCalls = %d, want 1 — denied request reached the backend", backendCalls)
	}
	if got := w2.Header().Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After = %q, want %q", got, "1")
	}
}

func TestMiddleware_IdentitiesAreIsolated(t *testing.T) {
	l := NewLimiter(1, 1)

	currentTime := time.Now()
	l.now = func() time.Time { return currentTime }

	backend := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	identity := func(r *http.Request) string { return r.Header.Get("X-API-Key") }
	handler := l.Middleware(identity)(backend)

	do := func(key string) int {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("X-API-Key", key)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		return w.Code
	}

	// key1 spends its single token.
	if code := do("key1"); code != http.StatusOK {
		t.Fatalf("key1 first request = %d, want 200", code)
	}
	if code := do("key1"); code != http.StatusTooManyRequests {
		t.Fatalf("key1 second request = %d, want 429", code)
	}

	// key2 has its own bucket -> must still be allowed.
	if code := do("key2"); code != http.StatusOK {
		t.Fatalf("key2 first request = %d, want 200 — buckets are not isolated", code)
	}
}
