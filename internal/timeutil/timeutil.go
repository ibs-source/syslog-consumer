// Package timeutil provides helpers for constructing time.Duration values
// from integer counts without performing duration-by-duration arithmetic,
// which is flagged by linters like durationcheck.
package timeutil

import "time"

// FromMillis converts a non-negative millisecond count to time.Duration
// without multiplying two durations (avoids durationcheck).
// Negative inputs return 0.
func FromMillis(ms int64) time.Duration {
	if ms <= 0 {
		return 0
	}
	// 1 millisecond = 1e6 nanoseconds. Avoid duration * duration.
	return time.Duration(ms * int64(time.Millisecond))
}
