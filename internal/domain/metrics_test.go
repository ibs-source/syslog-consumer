package domain

import (
	"math"
	"testing"
	"time"
)

func approxEqual(a, b, tol float64) bool {
	return math.Abs(a-b) <= tol
}

func TestMetricsRatesAndAverages(t *testing.T) {
	m := NewMetrics()
	// Pretend we've been running for exactly 10 seconds
	m.StartTime = time.Now().Add(-10 * time.Second)

	m.MessagesReceived.Store(100)
	m.MessagesPublished.Store(50)
	m.RedisErrors.Store(3)
	m.MQTTErrors.Store(2)
	m.ProcessingErrors.Store(5)

	// Totals to compute averages from
	m.ProcessingTimeNs.Store(1_000_000_000) // 1s total across 100 messages => 10ms avg
	m.PublishLatencyNs.Store(500_000_000)   // 0.5s total across 50 messages => 10ms avg

	if rate := m.GetThroughputRate(); !approxEqual(rate, 10.0, 0.5) {
		t.Fatalf("throughput rate expected ~10, got %f", rate)
	}
	if rate := m.GetPublishRate(); !approxEqual(rate, 5.0, 0.5) {
		t.Fatalf("publish rate expected ~5, got %f", rate)
	}
	if rate := m.GetErrorRate(); !approxEqual(rate, 1.0, 0.5) {
		// 3 + 2 + 5 = 10 errors over 10s => 1 err/sec
		t.Fatalf("error rate expected ~1, got %f", rate)
	}

	if avg := m.GetAverageProcessingTime(); !approxEqual(avg/1_000_000, 10.0, 1.0) {
		// in ms
		t.Fatalf("avg processing time expected ~10ms, got %fms", avg/1_000_000)
	}
	if avg := m.GetAveragePublishLatency(); !approxEqual(avg/1_000_000, 10.0, 1.0) {
		// in ms
		t.Fatalf("avg publish latency expected ~10ms, got %fms", avg/1_000_000)
	}
}

func TestMetricsSnapshot(t *testing.T) {
	m := NewMetrics()
	m.MessagesReceived.Store(7)
	m.MessagesPublished.Store(5)
	m.MessagesAcked.Store(3)
	m.MessagesDropped.Store(2)
	m.ActiveWorkers.Store(4)
	m.QueueDepth.Store(9)
	m.CPUPercent.Store(25)
	m.MemoryUsedBytes.Store(128 * 1024 * 1024) // 128MB

	s := m.Snapshot()

	if s.MessagesReceived != 7 || s.MessagesPublished != 5 || s.MessagesAcked != 3 || s.MessagesDropped != 2 {
		t.Fatalf("unexpected counters in snapshot: %#v", s)
	}
	if s.ActiveWorkers != 4 || s.QueueDepth != 9 {
		t.Fatalf("unexpected resource numbers: %#v", s)
	}
	if s.CPUPercent != 25 || s.MemoryMB != 128 {
		t.Fatalf("unexpected CPU/Memory: %#v", s)
	}
	if s.Timestamp.IsZero() {
		t.Fatalf("snapshot timestamp should be set")
	}
}
