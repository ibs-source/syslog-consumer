package domain

import (
	"sync/atomic"
	"time"
)

// Metrics holds atomic performance metrics
type Metrics struct {
	// Throughput metrics
	MessagesReceived  atomic.Uint64
	MessagesPublished atomic.Uint64
	MessagesAcked     atomic.Uint64
	MessagesDropped   atomic.Uint64

	// Performance metrics
	ProcessingTimeNs atomic.Uint64
	PublishLatencyNs atomic.Uint64
	AckLatencyNs     atomic.Uint64

	// Resource metrics
	ActiveWorkers   atomic.Int32
	QueueDepth      atomic.Int32
	MemoryUsedBytes atomic.Uint64
	CPUPercent      atomic.Uint64

	// Error metrics
	RedisErrors      atomic.Uint64
	MQTTErrors       atomic.Uint64
	ProcessingErrors atomic.Uint64

	// Backpressure metrics
	BackpressureDropped atomic.Uint64
	BufferUtilization   atomic.Uint64

	// Start time for rate calculations
	StartTime time.Time
}

// NewMetrics creates a new metrics instance
func NewMetrics() *Metrics {
	return &Metrics{
		StartTime: time.Now(),
	}
}

// GetThroughputRate returns messages per second
func (m *Metrics) GetThroughputRate() float64 {
	elapsed := time.Since(m.StartTime).Seconds()
	if elapsed == 0 {
		return 0
	}
	return float64(m.MessagesReceived.Load()) / elapsed
}

// GetPublishRate returns published messages per second
func (m *Metrics) GetPublishRate() float64 {
	elapsed := time.Since(m.StartTime).Seconds()
	if elapsed == 0 {
		return 0
	}
	return float64(m.MessagesPublished.Load()) / elapsed
}

// GetErrorRate returns errors per second
func (m *Metrics) GetErrorRate() float64 {
	elapsed := time.Since(m.StartTime).Seconds()
	if elapsed == 0 {
		return 0
	}
	totalErrors := m.RedisErrors.Load() + m.MQTTErrors.Load() + m.ProcessingErrors.Load()
	return float64(totalErrors) / elapsed
}

// GetAverageProcessingTime returns average processing time in nanoseconds
func (m *Metrics) GetAverageProcessingTime() float64 {
	received := m.MessagesReceived.Load()
	if received == 0 {
		return 0
	}
	return float64(m.ProcessingTimeNs.Load()) / float64(received)
}

// GetAveragePublishLatency returns average publish latency in nanoseconds
func (m *Metrics) GetAveragePublishLatency() float64 {
	published := m.MessagesPublished.Load()
	if published == 0 {
		return 0
	}
	return float64(m.PublishLatencyNs.Load()) / float64(published)
}

// MetricsSnapshot represents a point-in-time metrics snapshot
type MetricsSnapshot struct {
	Timestamp           time.Time
	MessagesReceived    uint64
	MessagesPublished   uint64
	MessagesAcked       uint64
	MessagesDropped     uint64
	ThroughputRate      float64
	PublishRate         float64
	ErrorRate           float64
	AvgProcessingTimeMs float64
	AvgPublishLatencyMs float64
	ActiveWorkers       int32
	QueueDepth          int32
	CPUPercent          uint64
	MemoryMB            uint64
}

// Snapshot creates a point-in-time snapshot of metrics
func (m *Metrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		Timestamp:           time.Now(),
		MessagesReceived:    m.MessagesReceived.Load(),
		MessagesPublished:   m.MessagesPublished.Load(),
		MessagesAcked:       m.MessagesAcked.Load(),
		MessagesDropped:     m.MessagesDropped.Load(),
		ThroughputRate:      m.GetThroughputRate(),
		PublishRate:         m.GetPublishRate(),
		ErrorRate:           m.GetErrorRate(),
		AvgProcessingTimeMs: m.GetAverageProcessingTime() / 1_000_000,
		AvgPublishLatencyMs: m.GetAveragePublishLatency() / 1_000_000,
		ActiveWorkers:       m.ActiveWorkers.Load(),
		QueueDepth:          m.QueueDepth.Load(),
		CPUPercent:          m.CPUPercent.Load(),
		MemoryMB:            m.MemoryUsedBytes.Load() / 1024 / 1024,
	}
}
