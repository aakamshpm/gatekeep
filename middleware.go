package gatekeep

import (
	"math"
	"net/http"
	"strconv"
)

// Middleware returns an Http Middleware that rate limits the requests
// identifyFn -> extracts the rate limit key from the request (eg. API key, userID, or IP)
// if request is denied, they get 429 and never move to next handler
func (l *Limiter) Middleware(identifyFn func(*http.Request) string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := identifyFn(r)

			allowed, retryAfter := l.Allow(id)

			if !allowed {
				// round the wait up to whole seconds (rounding using ceil)
				sec := int(math.Ceil(retryAfter.Seconds()))

				// if less than 1, round to 1 (user have to wait atleat one second)
				if sec < 1 {
					sec = 1
				}

				// modify Header before writing http Error, otherwise the Header will not be included in response
				w.Header().Set("Retry-After", strconv.Itoa(sec))
				http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
				return // stops here; next.ServeHTTP is never reached
			}

			next.ServeHTTP(w, r) // allowed -> forward request to real handler
		})
	}
}
