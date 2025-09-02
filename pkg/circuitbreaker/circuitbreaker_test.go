package circuitbreaker

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func isAllowedError(err error, expected error) bool {
	if err == nil {
		return true
	}
	return errors.Is(err, expected) || errors.Is(err, ErrOpenState) || errors.Is(err, ErrTooManyConcurrentRequests)
}

func TestNewCircuitBreaker(t *testing.T) {
	t.Run("create with valid config", func(t *testing.T) {
		cb := New("test-breaker", 50.0, 5, 10*time.Second, 100, 10)

		assert.NotNil(t, cb)
		assert.Equal(t, "test-breaker", cb.name)
		assert.Equal(t, 50.0, cb.errorThreshold)
		assert.Equal(t, uint64(5), cb.successThresholdU)
		assert.Equal(t, 10*time.Second, cb.timeout)
		assert.Equal(t, int32(100), cb.maxConcurrentRequests)
		assert.Equal(t, uint64(10), cb.requestVolumeThresholdU)
		assert.NotNil(t, cb.counts)
		assert.Equal(t, int32(StateClosed), cb.state.Load())
	})

	t.Run("create with edge case values", func(t *testing.T) {
		cb := New("test", 0.0, 0, 0, 0, 0)
		assert.NotNil(t, cb)
		assert.Equal(t, 0.0, cb.errorThreshold)
		assert.Equal(t, uint64(0), cb.successThresholdU)
	})
}

func TestCircuitBreakerExecute_SuccessWhenClosed(t *testing.T) {
	cb := New("test", 50.0, 3, 1*time.Second, 10, 5)

	executed := false
	err := cb.Execute(func() error {
		executed = true
		return nil
	})

	assert.NoError(t, err)
	assert.True(t, executed)
	assert.Equal(t, StateClosed.String(), cb.GetState())
}

func TestCircuitBreakerExecute_FailWhenClosed(t *testing.T) {
	cb := New("test", 50.0, 3, 1*time.Second, 10, 5)

	expectedErr := errors.New("test error")
	err := cb.Execute(func() error {
		return expectedErr
	})

	assert.Equal(t, expectedErr, err)
	assert.Equal(t, StateClosed.String(), cb.GetState())
}

func TestCircuitBreakerExecute_BlockedWhenOpen(t *testing.T) {
	cb := New("test", 50.0, 3, 1*time.Second, 10, 5)
	cb.state.Store(int32(StateOpen))

	executed := false
	err := cb.Execute(func() error {
		executed = true
		return nil
	})

	assert.Equal(t, ErrOpenState, err)
	assert.False(t, executed)
}

func TestCircuitBreakerExecute_ConcurrentLimit(t *testing.T) {
	cb := New("test", 50.0, 3, 1*time.Second, 2, 5) // Max 2 concurrent

	// Hold 2 requests
	var wg sync.WaitGroup
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			_ = cb.Execute(func() error {
				time.Sleep(100 * time.Millisecond)
				return nil
			})
		}()
	}

	// Give time for goroutines to start
	time.Sleep(10 * time.Millisecond)

	// Third request should be rejected
	err := cb.Execute(func() error {
		return nil
	})

	assert.Equal(t, ErrTooManyConcurrentRequests, err)
	wg.Wait()
}

func TestCircuitBreakerExecute_PanicRecovery(t *testing.T) {
	cb := New("test", 50.0, 3, 1*time.Second, 10, 5)

	err := cb.Execute(func() error {
		panic("test panic")
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "panic: test panic")
}

func TestCircuitBreakerStateTransitions(t *testing.T) {
	t.Run("closed to open on error threshold", func(t *testing.T) {
		cb := New("test", 50.0, 3, 1*time.Second, 10, 5)

		// Need at least 5 requests (volume threshold)
		// With 50% error threshold, 3 errors should open the circuit

		// 2 successes
		for i := 0; i < 2; i++ {
			_ = cb.Execute(func() error { return nil })
		}

		// 3 failures
		for i := 0; i < 3; i++ {
			_ = cb.Execute(func() error { return errors.New("error") })
		}

		assert.Equal(t, StateOpen.String(), cb.GetState())
	})

	t.Run("open to half-open after timeout", func(t *testing.T) {
		cb := New("test", 50.0, 3, 100*time.Millisecond, 10, 5)

		// Force to open state
		cb.state.Store(int32(StateOpen))
		cb.lastStateTime.Store(time.Now().UnixNano())

		// Wait for timeout
		time.Sleep(150 * time.Millisecond)

		// Next execution should move to half-open
		executed := false
		_ = cb.Execute(func() error {
			executed = true
			return nil
		})

		assert.True(t, executed)
		assert.Equal(t, StateHalfOpen.String(), cb.GetState())
	})

	t.Run("half-open to closed on success threshold", func(t *testing.T) {
		cb := New("test", 50.0, 3, 1*time.Second, 10, 5)

		// Force to half-open state
		cb.state.Store(int32(StateHalfOpen))
		cb.counts.reset()

		// 3 consecutive successes should close the circuit
		for i := 0; i < 3; i++ {
			err := cb.Execute(func() error { return nil })
			assert.NoError(t, err)
		}

		assert.Equal(t, StateClosed.String(), cb.GetState())
	})

	t.Run("half-open to open on any failure", func(t *testing.T) {
		cb := New("test", 50.0, 3, 1*time.Second, 10, 5)

		// Force to half-open state
		cb.state.Store(int32(StateHalfOpen))

		// Any failure should open the circuit
		_ = cb.Execute(func() error { return errors.New("error") })

		assert.Equal(t, StateOpen.String(), cb.GetState())
	})
}

func TestCircuitBreakerGetStats(t *testing.T) {
	cb := New("test", 50.0, 3, 1*time.Second, 10, 5)

	// Execute some requests
	for i := 0; i < 3; i++ {
		_ = cb.Execute(func() error { return nil })
	}
	for i := 0; i < 2; i++ {
		_ = cb.Execute(func() error { return errors.New("error") })
	}

	stats := cb.GetStats()

	assert.Equal(t, uint64(5), stats.Requests)
	assert.Equal(t, uint64(3), stats.TotalSuccess)
	assert.Equal(t, uint64(2), stats.TotalFailure)
	assert.Equal(t, StateClosed.String(), stats.State)
}

func TestCircuitBreakerVolumeThreshold(t *testing.T) {
	t.Run("no state change below volume threshold", func(t *testing.T) {
		cb := New("test", 50.0, 3, 1*time.Second, 10, 10) // Need 10 requests

		// 4 failures out of 4 requests (100% error rate)
		// But below volume threshold
		for i := 0; i < 4; i++ {
			_ = cb.Execute(func() error { return errors.New("error") })
		}

		// Should still be closed
		assert.Equal(t, StateClosed.String(), cb.GetState())
	})

	t.Run("state change when volume threshold met", func(t *testing.T) {
		cb := New("test", 50.0, 3, 1*time.Second, 10, 10)

		// 10 requests with 60% error rate
		for i := 0; i < 4; i++ {
			_ = cb.Execute(func() error { return nil })
		}
		for i := 0; i < 6; i++ {
			_ = cb.Execute(func() error { return errors.New("error") })
		}

		// Should be open
		assert.Equal(t, StateOpen.String(), cb.GetState())
	})
}

func TestCircuitBreakerConcurrency_Executions(t *testing.T) {
	cb := New("test", 50.0, 3, 1*time.Second, 100, 10)

	expectedErr := errors.New("error")
	var successCount, errorCount atomic.Int32
	var wg sync.WaitGroup

	// Run many concurrent requests
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			err := cb.Execute(func() error {
				if id%2 == 0 {
					return nil
				}
				return expectedErr
			})
			if !isAllowedError(err, expectedErr) {
				t.Errorf("unexpected error: %v", err)
			}
			if err == nil {
				successCount.Add(1)
			} else if errors.Is(err, expectedErr) {
				errorCount.Add(1)
			}
		}(i)
	}

	wg.Wait()

	// Should have processed some requests
	assert.Greater(t, successCount.Load()+errorCount.Load(), int32(0))
}

func TestCircuitBreakerConcurrency_StateTransitions(t *testing.T) {
	cb := New("test", 20.0, 3, 100*time.Millisecond, 100, 5)

	var wg sync.WaitGroup

	// Goroutine causing failures
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			_ = cb.Execute(func() error { return errors.New("error") })
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// Goroutine causing successes
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			_ = cb.Execute(func() error { return nil })
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// Goroutine checking state
	wg.Add(1)
	go func() {
		defer wg.Done()
		states := make(map[string]bool)
		for i := 0; i < 40; i++ {
			state := cb.GetState()
			states[state] = true
			time.Sleep(5 * time.Millisecond)
		}
		// Should have seen state changes
		assert.Greater(t, len(states), 1)
	}()

	wg.Wait()
}

// Benchmark tests
func BenchmarkCircuitBreakerClosed(b *testing.B) {
	cb := New("bench", 50.0, 3, 1*time.Second, 1000, 10)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = cb.Execute(func() error { return nil })
		}
	})
}

func BenchmarkCircuitBreakerOpen(b *testing.B) {
	cb := New("bench", 50.0, 3, 1*time.Second, 1000, 10)
	cb.state.Store(int32(StateOpen))

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = cb.Execute(func() error { return nil })
		}
	})
}

func BenchmarkCircuitBreakerMixed(b *testing.B) {
	cb := New("bench", 50.0, 3, 1*time.Second, 1000, 10)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_ = cb.Execute(func() error {
				i++
				if i%10 == 0 {
					return errors.New("error")
				}
				return nil
			})
		}
	})
}

// Test for edge cases
func TestCircuitBreakerEdgeCases(t *testing.T) {
	t.Run("generation change ignores old results", func(t *testing.T) {
		cb := New("test", 50.0, 3, 1*time.Second, 10, 5)

		// Get current generation
		gen := cb.generation.Load()

		// Force generation change
		cb.generation.Add(1)

		// Execute with old generation should be ignored
		cb.afterRequest(gen, errors.New("error"))

		// Stats should be empty
		stats := cb.GetStats()
		assert.Equal(t, uint64(0), stats.Requests)
	})

	t.Run("rapid timeout checks", func(t *testing.T) {
		cb := New("test", 50.0, 3, 100*time.Millisecond, 10, 5)

		// Open the circuit by failing requests
		for i := 0; i < 10; i++ {
			_ = cb.Execute(func() error {
				return errors.New("error")
			})
		}

		assert.Equal(t, "open", cb.GetState())

		// Wait for timeout period
		time.Sleep(150 * time.Millisecond)

		// Multiple goroutines checking if circuit is half-open
		var wg sync.WaitGroup
		transitioned := atomic.Bool{}
		successCount := atomic.Int32{}

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := cb.Execute(func() error {
					return nil
				})
				if err == nil {
					transitioned.Store(true)
					successCount.Add(1)
				}
			}()
		}

		wg.Wait()

		// At least one request should have succeeded
		assert.True(t, transitioned.Load(), "Circuit should have transitioned to half-open")
		assert.Greater(t, successCount.Load(), int32(0), "At least one request should have succeeded")
	})

	t.Run("nil function execution", func(t *testing.T) {
		cb := New("test", 50.0, 3, 1*time.Second, 10, 5)

		// Should handle nil function gracefully
		err := cb.Execute(nil)
		assert.Error(t, err)
	})
}

type rwCounters struct {
	total   atomic.Int64
	success atomic.Int64
	fail    atomic.Int64
	reject  atomic.Int64
}

func makeServiceCall(serviceHealthy *atomic.Bool) func() error {
	return func() error {
		if !serviceHealthy.Load() {
			return errors.New("service unavailable")
		}
		// Simulate network latency
		time.Sleep(5 * time.Millisecond)
		return nil
	}
}

func startTraffic(cb *CircuitBreaker, call func() error, wg *sync.WaitGroup, stopChan <-chan struct{}, c *rwCounters) {
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopChan:
					return
				default:
					c.total.Add(1)
					err := cb.Execute(call)
					switch {
					case err == nil:
						c.success.Add(1)
					case errors.Is(err, ErrOpenState), errors.Is(err, ErrTooManyConcurrentRequests):
						c.reject.Add(1)
					default:
						c.fail.Add(1)
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
		}()
	}
}

func logAndAssertRealWorld(t *testing.T, c *rwCounters) {
	t.Helper()
	t.Logf("Total requests: %d", c.total.Load())
	t.Logf("Successful calls: %d", c.success.Load())
	t.Logf("Failed calls: %d", c.fail.Load())
	t.Logf("Rejected calls: %d", c.reject.Load())

	assert.Greater(t, c.total.Load(), int64(0))
	assert.Greater(t, c.success.Load(), int64(0))
	assert.Greater(t, c.fail.Load(), int64(0))
	assert.Greater(t, c.reject.Load(), int64(0))
}

// Integration test simulating real-world usage
func TestCircuitBreakerRealWorldScenario(t *testing.T) {
	cb := New("service-api", 30.0, 5, 2*time.Second, 50, 20)

	// Simulate a service that has intermittent failures
	serviceHealthy := atomic.Bool{}
	serviceHealthy.Store(true)
	callService := makeServiceCall(&serviceHealthy)

	counters := &rwCounters{}
	var wg sync.WaitGroup
	stopChan := make(chan struct{})

	startTraffic(cb, callService, &wg, stopChan, counters)

	// Simulate service outage after 200ms
	time.Sleep(200 * time.Millisecond)
	serviceHealthy.Store(false)

	// Service down for 3 seconds
	time.Sleep(3 * time.Second)
	serviceHealthy.Store(true)

	// Continue for another 2 seconds
	time.Sleep(2 * time.Second)

	// Stop traffic and wait
	close(stopChan)
	wg.Wait()

	// Verify behavior
	logAndAssertRealWorld(t, counters)

	// Circuit should have opened during the outage
	assert.Greater(t, counters.reject.Load(), int64(10))
}

// Test State String representation
func TestStateString(t *testing.T) {
	tests := []struct {
		state    State
		expected string
	}{
		{StateClosed, "closed"},
		{StateOpen, "open"},
		{StateHalfOpen, "half-open"},
		{State(99), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.state.String())
		})
	}
}
