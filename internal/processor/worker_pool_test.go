package processor

import (
	"sync/atomic"
	"testing"
	"time"
)

func eventually(t *testing.T, d time.Duration, fn func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("eventually failed: %s", msg)
}

func TestWorkerPool_StartSubmitSubmitWait_Stop(t *testing.T) {
	wp := NewWorkerPool(1, 4)
	if wp.GetWorkerCount() != 0 {
		t.Fatalf("expected 0 workers before Start, got %d", wp.GetWorkerCount())
	}

	wp.Start()
	eventually(t, 200*time.Millisecond, func() bool {
		return wp.GetWorkerCount() >= 1
	}, "expected at least 1 worker after Start")

	var cnt atomic.Int32
	// Submit a few tasks
	for i := 0; i < 5; i++ {
		if err := wp.Submit(func() { cnt.Add(1) }); err != nil {
			t.Fatalf("submit error: %v", err)
		}
	}

	// SubmitWait should block until done
	if err := wp.SubmitWait(func() { cnt.Add(1) }); err != nil {
		t.Fatalf("submitwait error: %v", err)
	}

	// Wait counters to reflect 6 tasks
	eventually(t, 500*time.Millisecond, func() bool {
		return cnt.Load() == 6
	}, "expected 6 tasks executed")

	// Increase workers and observe growth
	wp.SetWorkerCount(3)
	eventually(t, 500*time.Millisecond, func() bool {
		return wp.GetWorkerCount() >= 3
	}, "expected worker count to reach at least 3")

	// Stop should gracefully shutdown and reject new tasks
	wp.Stop()

	if err := wp.Submit(func() {}); err == nil {
		t.Fatalf("expected error submitting after Stop")
	}
}

func TestWorkerPool_IdempotentStartAndStop(t *testing.T) {
	wp := NewWorkerPool(2, 2)
	wp.Start()
	first := wp.GetWorkerCount()

	// Second start should be idempotent
	wp.Start()
	second := wp.GetWorkerCount()
	if second != first {
		t.Fatalf("expected idempotent Start, counts %d vs %d", first, second)
	}

	// Stop twice should be safe
	wp.Stop()
	wp.Stop()
}
