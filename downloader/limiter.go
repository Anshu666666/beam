package downloader

import (
	"io"
	"sync"
	"time"
)

// RateLimiter implements a thread-safe token-bucket bandwidth throttle.
type RateLimiter struct {
	bytesPerSec int64
	mu          sync.Mutex
	last        time.Time
	tokens      float64
	capacity    float64
}

// NewRateLimiter creates a new rate limiter enforcing maximum bytes/second across all callers.
// If bytesPerSec <= 0, returns nil (unthrottled).
func NewRateLimiter(bytesPerSec int64) *RateLimiter {
	if bytesPerSec <= 0 {
		return nil
	}
	return &RateLimiter{
		bytesPerSec: bytesPerSec,
		last:        time.Now(),
		tokens:      float64(bytesPerSec) * 0.25, // 250ms initial burst
		capacity:    float64(bytesPerSec),        // max 1s burst buffer
	}
}

// Take deducts n bytes from the token bucket, sleeping if necessary to maintain the rate.
func (rl *RateLimiter) Take(n int64) {
	if rl == nil || rl.bytesPerSec <= 0 || n <= 0 {
		return
	}
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(rl.last).Seconds()
	rl.last = now

	// Refill tokens based on elapsed wall time
	rl.tokens += elapsed * float64(rl.bytesPerSec)
	if rl.tokens > rl.capacity {
		rl.tokens = rl.capacity
	}

	rl.tokens -= float64(n)

	// If in deficit, pause goroutine to respect speed cap
	if rl.tokens < 0 {
		deficit := -rl.tokens
		sleepSecs := deficit / float64(rl.bytesPerSec)
		sleepDur := time.Duration(sleepSecs * float64(time.Second))
		if sleepDur > 0 {
			time.Sleep(sleepDur)
			rl.last = time.Now()
			rl.tokens = 0
		}
	}
}

// ThrottledReader wraps an io.Reader and slows reading according to the given RateLimiter.
type ThrottledReader struct {
	r       io.Reader
	limiter *RateLimiter
}

// NewThrottledReader wraps r with bandwidth pacing.
func NewThrottledReader(r io.Reader, limiter *RateLimiter) io.Reader {
	if limiter == nil {
		return r
	}
	return &ThrottledReader{r: r, limiter: limiter}
}

func (tr *ThrottledReader) Read(p []byte) (int, error) {
	n, err := tr.r.Read(p)
	if n > 0 && tr.limiter != nil {
		tr.limiter.Take(int64(n))
	}
	return n, err
}