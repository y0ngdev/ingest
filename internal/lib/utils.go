package lib

import (
	"context"
	"os"
	"time"
)

func Getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// Retry calls fn up to attempts times. After each failure it waits base×2ⁱ
// before the next attempt (exponential backoff). Returns the last error if
// all attempts fail, or ctx.Err() if the context is cancelled while waiting.
func Retry(ctx context.Context, attempts int, base time.Duration, fn func() error) error {
	var err error
	for i := range attempts {
		if err = fn(); err == nil {
			return nil
		}
		if i < attempts-1 {
			select {
			case <-time.After(base << i):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return err
}
