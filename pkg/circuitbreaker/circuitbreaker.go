// Package circuitbreaker implements a sliding-window circuit breaker with atomic state.
package circuitbreaker

import (
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/ibs-source/syslog/consumer/golang/internal/ports"
)

// State represents the state of the circuit breaker
type State int32

const (
	// StateClosed means the circuit breaker is allowing requests
	StateClosed State = iota
	// StateOpen means the circuit breaker is blocking requests
	StateOpen
	// StateHalfOpen means the circuit breaker is testing if the service has recovered
	StateHalfOpen
)

// String returns the string representation of the state
func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// ErrOpenState is returned when the circuit breaker is open
var ErrOpenState = errors.New("circuit breaker is open")

// ErrTooManyConcurrentRequests is returned when max concurrent requests is exceeded
var ErrTooManyConcurrentRequests = errors.New("too many concurrent requests")

// Safe conversion helpers to satisfy gosec G115 without nolint or config changes.
func nonNegIntToUint64(v int) uint64 {
	if v <= 0 {
		return 0
	}
	return uint64(v)
}

func clampIntToInt32(v int) int32 {
	if v < 0 {
		return 0
	}
	if v > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(v)
}

// CircuitBreaker implements the circuit breaker pattern with adaptive behavior
type CircuitBreaker struct {
	name string

	// Configuration
	errorThreshold          float64
	successThresholdU       uint64
	timeout                 time.Duration
	maxConcurrentRequests   int32
	requestVolumeThresholdU uint64

	// State management
	state         atomic.Int32
	lastStateTime atomic.Int64
	generation    atomic.Uint64

	// Statistics
	counts *window

	// Concurrency control
	activeRequests atomic.Int32
}

// New creates a new circuit breaker
func New(
	name string,
	errorThreshold float64,
	successThreshold int,
	timeout time.Duration,
	maxConcurrent int,
	volumeThreshold int,
) *CircuitBreaker {
	// Guard int -> int32 conversions to satisfy gosec G115
	s := successThreshold
	if s < 0 {
		s = 0
	} else if s > math.MaxInt32 {
		s = math.MaxInt32
	}
	mc := maxConcurrent
	if mc < 0 {
		mc = 0
	} else if mc > math.MaxInt32 {
		mc = math.MaxInt32
	}
	vt := volumeThreshold
	if vt < 0 {
		vt = 0
	} else if vt > math.MaxInt32 {
		vt = math.MaxInt32
	}

	cb := &CircuitBreaker{
		name:                    name,
		errorThreshold:          errorThreshold,
		successThresholdU:       nonNegIntToUint64(s),
		timeout:                 timeout,
		maxConcurrentRequests:   clampIntToInt32(mc),
		requestVolumeThresholdU: nonNegIntToUint64(vt),
		counts:                  newWindow(10, time.Minute), // 10 buckets over 1 minute
	}

	cb.state.Store(int32(StateClosed))
	cb.lastStateTime.Store(time.Now().UnixNano())

	return cb
}

// Execute runs the given function if the circuit breaker allows it
func (cb *CircuitBreaker) Execute(fn func() error) (err error) {
	if fn == nil {
		return errors.New("function cannot be nil")
	}

	generation, err := cb.beforeRequest()
	if err != nil {
		return err
	}

	// Panic recovery
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
			cb.afterRequest(generation, err)
		}
	}()

	err = fn()
	cb.afterRequest(generation, err)
	return err
}

// GetState returns the current state of the circuit breaker
func (cb *CircuitBreaker) GetState() string {
	return State(cb.state.Load()).String()
}

// GetStats returns the current statistics
func (cb *CircuitBreaker) GetStats() ports.CircuitBreakerStats {
	counts := cb.counts.sum()
	return ports.CircuitBreakerStats{
		Requests:            counts.requests,
		TotalSuccess:        counts.successes,
		TotalFailure:        counts.failures,
		ConsecutiveFailures: counts.consecutiveFailures,
		State:               cb.GetState(),
	}
}

// beforeRequest is called before executing a request
func (cb *CircuitBreaker) beforeRequest() (uint64, error) {
	// Check concurrent requests limit
	active := cb.activeRequests.Add(1)
	if cb.maxConcurrentRequests > 0 && active > cb.maxConcurrentRequests {
		cb.activeRequests.Add(-1)
		return 0, ErrTooManyConcurrentRequests
	}

	state := State(cb.state.Load())
	generation := cb.generation.Load()

	if state == StateOpen {
		// If the timeout has passed, try to transition to half-open
		lastStateTime := cb.lastStateTime.Load()
		if time.Now().UnixNano()-lastStateTime > cb.timeout.Nanoseconds() {
			if cb.state.CompareAndSwap(int32(StateOpen), int32(StateHalfOpen)) {
				cb.toHalfOpen()
			}
		}
		// Re-check state after potential transition
		if State(cb.state.Load()) == StateOpen {
			cb.activeRequests.Add(-1) // Decrement since we are not proceeding
			return 0, ErrOpenState
		}
	}

	return generation, nil
}

// afterRequest is called after executing a request
func (cb *CircuitBreaker) afterRequest(generation uint64, err error) {
	cb.activeRequests.Add(-1)

	// If generation has changed, ignore this result as it's from a previous state
	if generation != cb.generation.Load() {
		return
	}

	if err == nil {
		cb.onSuccess()
	} else {
		cb.onFailure()
	}
}

// onSuccess handles successful request
func (cb *CircuitBreaker) onSuccess() {
	cb.counts.success()

	if State(cb.state.Load()) == StateHalfOpen {
		counts := cb.counts.sum()
		if counts.consecutiveSuccesses >= cb.successThresholdU {
			cb.toClosed()
		}
	}
}

// onFailure handles failed request
func (cb *CircuitBreaker) onFailure() {
	cb.counts.failure()

	state := State(cb.state.Load())
	switch state {
	case StateClosed:
		if cb.shouldOpen() {
			cb.toOpen()
		}
	case StateHalfOpen:
		cb.toOpen()
	}
}

// shouldOpen checks if the circuit should be opened
func (cb *CircuitBreaker) shouldOpen() bool {
	counts := cb.counts.sum()

	// Not enough requests to make a decision
	if counts.requests < cb.requestVolumeThresholdU {
		return false
	}

	// Calculate error rate
	errorRate := float64(counts.failures) / float64(counts.requests) * 100
	return errorRate >= cb.errorThreshold
}

// State transition methods

func (cb *CircuitBreaker) toOpen() {
	swapped := cb.state.CompareAndSwap(int32(StateClosed), int32(StateOpen))
	if !swapped {
		swapped = cb.state.CompareAndSwap(int32(StateHalfOpen), int32(StateOpen))
	}
	if swapped {
		cb.lastStateTime.Store(time.Now().UnixNano())
		cb.generation.Add(1)
	}
}

func (cb *CircuitBreaker) toHalfOpen() {
	// This is called after a CAS in beforeRequest, so no need for another CAS here.
	cb.lastStateTime.Store(time.Now().UnixNano())
	cb.generation.Add(1)
	cb.counts.reset()
}

func (cb *CircuitBreaker) toClosed() {
	if cb.state.CompareAndSwap(int32(StateHalfOpen), int32(StateClosed)) {
		cb.lastStateTime.Store(time.Now().UnixNano())
		cb.generation.Add(1)
		cb.counts.reset()
	}
}
