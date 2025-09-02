// Package circuitbreaker implements sliding-window statistics for the circuit breaker.
package circuitbreaker

import (
	"sync"
	"sync/atomic"
	"time"
)

// windowCounts holds the counts for a time window
type windowCounts struct {
	requests             uint64
	successes            uint64
	failures             uint64
	consecutiveSuccesses uint64
	consecutiveFailures  uint64
}

// bucket represents a time bucket in the sliding window
type bucket struct {
	start     int64 // Unix nano
	requests  atomic.Uint64
	successes atomic.Uint64
	failures  atomic.Uint64
}

// window implements a sliding time window for tracking circuit breaker statistics
type window struct {
	buckets      []bucket
	size         int
	bucketTime   int64        // Nanoseconds
	lastRotation atomic.Int64 // Unix nano
	mu           sync.Mutex

	// Consecutive counters
	consecutiveSuccesses atomic.Uint64
	consecutiveFailures  atomic.Uint64
}

// newWindow creates a new sliding window
func newWindow(size int, duration time.Duration) *window {
	if size <= 0 {
		size = 10 // Default size
	}
	bucketTime := int64(duration) / int64(size)
	now := time.Now().UnixNano()

	w := &window{
		buckets:    make([]bucket, size),
		size:       size,
		bucketTime: bucketTime,
	}

	w.lastRotation.Store(now)

	// Initialize buckets
	for i := range w.buckets {
		w.buckets[i].start = now
	}

	return w
}

// success records a successful request
func (w *window) success() {
	b := w.getCurrentBucket()
	b.requests.Add(1)
	b.successes.Add(1)
	w.consecutiveSuccesses.Add(1)
	w.consecutiveFailures.Store(0)
}

// failure records a failed request
func (w *window) failure() {
	b := w.getCurrentBucket()
	b.requests.Add(1)
	b.failures.Add(1)
	w.consecutiveFailures.Add(1)
	w.consecutiveSuccesses.Store(0)
}

// sum returns the sum of all counts in the current time window
func (w *window) sum() windowCounts {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.rotate(time.Now().UnixNano())

	counts := windowCounts{
		consecutiveSuccesses: w.consecutiveSuccesses.Load(),
		consecutiveFailures:  w.consecutiveFailures.Load(),
	}

	for i := range w.buckets {
		counts.requests += w.buckets[i].requests.Load()
		counts.successes += w.buckets[i].successes.Load()
		counts.failures += w.buckets[i].failures.Load()
	}

	return counts
}

// reset clears all counters
func (w *window) reset() {
	w.mu.Lock()
	defer w.mu.Unlock()

	now := time.Now().UnixNano()
	w.lastRotation.Store(now)

	for i := range w.buckets {
		w.buckets[i].start = now
		w.buckets[i].requests.Store(0)
		w.buckets[i].successes.Store(0)
		w.buckets[i].failures.Store(0)
	}

	w.consecutiveSuccesses.Store(0)
	w.consecutiveFailures.Store(0)
}

// getCurrentBucket returns the current bucket, rotating the window if necessary
func (w *window) getCurrentBucket() *bucket {
	w.mu.Lock()
	defer w.mu.Unlock()

	now := time.Now().UnixNano()
	w.rotate(now)
	idx := int((now / w.bucketTime) % int64(w.size))
	return &w.buckets[idx]
}

// rotate moves old buckets out of the window
func (w *window) rotate(now int64) {
	lastRotation := w.lastRotation.Load()
	if now-lastRotation < w.bucketTime {
		return
	}

	// Update lastRotation before proceeding
	w.lastRotation.Store(now)

	// Calculate how many buckets to clear
	numExpired := (now - lastRotation) / w.bucketTime
	if numExpired >= int64(w.size) {
		// All buckets are expired, reset them all
		for i := 0; i < w.size; i++ {
			w.buckets[i].start = now
			w.buckets[i].requests.Store(0)
			w.buckets[i].successes.Store(0)
			w.buckets[i].failures.Store(0)
		}
		return
	}

	// Clear the expired buckets
	startIdx := int((lastRotation / w.bucketTime) % int64(w.size))
	for i := int64(1); i <= numExpired; i++ {
		idx := (startIdx + int(i)) % w.size
		w.buckets[idx].start = now
		w.buckets[idx].requests.Store(0)
		w.buckets[idx].successes.Store(0)
		w.buckets[idx].failures.Store(0)
	}
}
