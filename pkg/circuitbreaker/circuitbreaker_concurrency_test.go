package circuitbreaker

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrency verifies that the CircuitBreaker is safe for concurrent use.
func TestConcurrency(t *testing.T) {
	cb := New(
		"test",
		50,                   // errorThreshold
		5,                    // successThreshold
		time.Millisecond*100, // timeout
		100,                  // maxConcurrentCalls
		10,                   // requestVolumeThreshold
	)

	numGoroutines := 100
	numRequestsPerG := 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	var totalRequests, totalErrors, totalSuccesses int64

	// Simulate concurrent requests
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < numRequestsPerG; j++ {
				atomic.AddInt64(&totalRequests, 1)
				err := cb.Execute(func() error {
					// Simulate some work
					time.Sleep(time.Millisecond)
					// Simulate a failing operation
					return errors.New("simulated error")
				})

				if err != nil {
					atomic.AddInt64(&totalErrors, 1)
				} else {
					atomic.AddInt64(&totalSuccesses, 1)
				}
			}
		}()
	}

	wg.Wait()

	t.Logf("Total Requests: %d, Successes: %d, Errors: %d", totalRequests, totalSuccesses, totalErrors)

	// We expect some requests to be successful before the circuit opens,
	// and some to fail due to the open state.
	if totalSuccesses > 0 {
		t.Errorf("Expected no successes for a constantly failing function, got %d", totalSuccesses)
	}
	if totalErrors != int64(numGoroutines*numRequestsPerG) {
		t.Errorf("Expected all requests to result in an error, but got %d errors", totalErrors)
	}
}

// TestStateTransitionsWithConcurrency tests that state transitions are correct under load.
func TestStateTransitionsWithConcurrency(t *testing.T) {
	cb := New(
		"test_transitions",
		20,                  // errorThreshold (20%)
		5,                   // successThreshold
		time.Millisecond*50, // timeout
		50,                  // maxConcurrentCalls
		10,                  // requestVolumeThreshold
	)

	var wg sync.WaitGroup
	numGoroutines := 50

	// 1. Force the circuit to OPEN
	t.Run("ForceOpen", func(t *testing.T) {
		wg.Add(numGoroutines)
		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer wg.Done()
				for j := 0; j < 5; j++ {
					_ = cb.Execute(func() error { return errors.New("fail") })
				}
			}()
		}
		wg.Wait()

		if State(cb.state.Load()) != StateOpen {
			t.Fatalf("Expected state to be OPEN, but got %s", State(cb.state.Load()))
		}
	})

	// 2. Wait for timeout and transition to HALF-OPEN
	t.Run("TransitionToHalfOpen", func(t *testing.T) {
		time.Sleep(cb.timeout + time.Millisecond*10)

		// One successful request should be enough to move to half-open and then test for closed
		_ = cb.Execute(func() error { return nil })

		if State(cb.state.Load()) != StateHalfOpen {
			t.Fatalf("Expected state to be HALF-OPEN, but got %s", State(cb.state.Load()))
		}
	})

	// 3. Force the circuit to CLOSE
	t.Run("ForceClose", func(t *testing.T) {
		wg.Add(numGoroutines)
		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer wg.Done()
				for j := uint64(0); j < cb.successThresholdU; j++ {
					_ = cb.Execute(func() error { return nil })
				}
			}()
		}
		wg.Wait()

		// It might take a moment for all successes to be registered and the state to flip
		time.Sleep(time.Millisecond * 10)

		if State(cb.state.Load()) != StateClosed {
			t.Fatalf("Expected state to be CLOSED, but got %s", State(cb.state.Load()))
		}
	})
}
