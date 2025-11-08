package hotpath

import (
	"testing"
	"time"

	"github.com/ibs-source/syslog-consumer/internal/config"
	"github.com/ibs-source/syslog-consumer/internal/log"
)

func TestNew(t *testing.T) {
	// Create mock config
	cfg := &config.Config{
		Redis: config.RedisConfig{
			ClaimIdle:           30 * time.Second,
			CleanupInterval:     1 * time.Minute,
			ConsumerIdleTimeout: 5 * time.Minute,
		},
		Pipeline: config.PipelineConfig{
			BufferCapacity: 100,
			ErrorBackoff:   1 * time.Second,
			AckTimeout:     5 * time.Second,
		},
	}

	logger := log.New()

	hp := New(nil, nil, cfg, logger)

	if hp == nil {
		t.Fatal("New() returned nil")
	}
	if hp.msgChan == nil {
		t.Error("msgChan not initialized")
	}
	if hp.claimTicker == nil {
		t.Error("claimTicker not initialized")
	}
	if hp.cleanupTicker == nil {
		t.Error("cleanupTicker not initialized")
	}
	if hp.log == nil {
		t.Error("logger not set")
	}

	// Cleanup
	_ = hp.Close()
}

func TestBuildPayload(t *testing.T) {
	cfg := &config.Config{
		Redis: config.RedisConfig{
			ClaimIdle:           30 * time.Second,
			CleanupInterval:     1 * time.Minute,
			ConsumerIdleTimeout: 5 * time.Minute,
		},
		Pipeline: config.PipelineConfig{
			BufferCapacity: 100,
			ErrorBackoff:   1 * time.Second,
			AckTimeout:     5 * time.Second,
		},
	}

	logger := log.New()
	hp := New(nil, nil, cfg, logger)
	defer func() { _ = hp.Close() }()

	tests := []struct {
		name     string
		id       string
		stream   string
		data     []byte
		expected string
	}{
		{
			name:   "simple payload with stream",
			id:     "msg-123",
			stream: "syslog-stream",
			data:   []byte(`{"key":"value"}`),
			expected: `{"message":{"payload":{"key":"value"}},` +
				`"redis":{"payload":{"id":"msg-123","stream":"syslog-stream","ack":true}}}`,
		},
		{
			name:     "empty payload with stream",
			id:       "msg-456",
			stream:   "test-stream",
			data:     []byte(``),
			expected: `{"message":{"payload":},"redis":{"payload":{"id":"msg-456","stream":"test-stream","ack":true}}}`,
		},
		{
			name:   "multi-stream payload",
			id:     "1234567890-0",
			stream: "stream-tenant-A",
			data:   []byte(`raw syslog data`),
			expected: `{"message":{"payload":raw syslog data},` +
				`"redis":{"payload":{"id":"1234567890-0","stream":"stream-tenant-A","ack":true}}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hp.buildPayload(tt.id, tt.stream, tt.data)
			if string(result) != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, string(result))
			}
		})
	}
}

func TestClose(t *testing.T) {
	cfg := &config.Config{
		Redis: config.RedisConfig{
			ClaimIdle:           30 * time.Second,
			CleanupInterval:     1 * time.Minute,
			ConsumerIdleTimeout: 5 * time.Minute,
		},
		Pipeline: config.PipelineConfig{
			BufferCapacity: 100,
			ErrorBackoff:   1 * time.Second,
			AckTimeout:     5 * time.Second,
		},
	}

	logger := log.New()
	hp := New(nil, nil, cfg, logger)

	err := hp.Close()
	if err != nil {
		t.Errorf("Close() returned error: %v", err)
	}
}

// Note: handleAck requires a live Redis connection and cannot be easily tested
// without mocking or using a real Redis instance. The function calls
// redis.AckAndDelete which requires an active connection.
// This is best tested through integration tests.
