package middleware

import (
	"context"
	"expvar"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"sync"

	"golang.org/x/time/rate"
)

type ctxKey string

// CtxKeyRequestID is the context key for the per-request ID.
const CtxKeyRequestID ctxKey = "request_id"

// RequestID attaches a random 16-char hex ID to each request, sets the
// X-Request-ID response header, and logs the incoming request with slog.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := fmt.Sprintf("%016x", rand.Uint64())
		ctx := context.WithValue(r.Context(), CtxKeyRequestID, id)
		w.Header().Set("X-Request-ID", id)
		slog.Info("req", "method", r.Method, "path", r.URL.Path, "id", id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// BearerAuth rejects requests whose Authorization header does not match
// "Bearer <token>". Pass an empty string to disable authentication.
func BearerAuth(token string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if token == "" {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer "+token {
				http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// RateLimiter applies a per-IP token-bucket rate limit.
// Each unique remote IP gets its own *rate.Limiter.
type RateLimiter struct {
	mu       sync.Mutex
	limiters map[string]*rate.Limiter
	limit    rate.Limit
	burst    int
}

func NewRateLimiter(rps float64, burst int) *RateLimiter {
	return &RateLimiter{
		limiters: make(map[string]*rate.Limiter),
		limit:    rate.Limit(rps),
		burst:    burst,
	}
}

func (rl *RateLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			ip = r.RemoteAddr
		}
		if !rl.get(ip).Allow() {
			http.Error(w, `{"error":"rate limit exceeded"}`, http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (rl *RateLimiter) get(ip string) *rate.Limiter {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	l, ok := rl.limiters[ip]
	if !ok {
		l = rate.NewLimiter(rl.limit, rl.burst)
		rl.limiters[ip] = l
	}
	return l
}

// RequestCount wraps a handler and increments the named expvar counter on
// every request. Use to track endpoint-level traffic without a metrics lib.
func RequestCount(name string, next http.Handler) http.Handler {
	c := expvar.NewInt(name)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Add(1)
		next.ServeHTTP(w, r)
	})
}
