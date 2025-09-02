package processor

import (
	"testing"

	"github.com/ibs-source/syslog/consumer/golang/internal/domain"
	"github.com/ibs-source/syslog/consumer/golang/internal/logger"
)

// These tests target simple branches that were previously uncovered to boost coverage on critical processor paths.

func TestGetMetrics_ReturnsSameInstance(t *testing.T) {
	t.Helper()
	log, _ := logger.NewLogrusLogger("error", "json")
	metrics := domain.NewMetrics()
	cfg := minimalConfig()
	p := NewStreamProcessor(cfg, &ackFakeRedis{}, &noopMQTT{}, log, metrics, &passCB{}, nil)

	if p.GetMetrics() != metrics {
		t.Fatalf("expected GetMetrics to return the same metrics instance")
	}
}

func TestDrainUnassignedMessages_DisabledNoOp(t *testing.T) {
	t.Helper()
	log, _ := logger.NewLogrusLogger("error", "json")
	cfg := minimalConfig()
	cfg.Redis.DrainEnabled = false
	p := NewStreamProcessor(cfg, &ackFakeRedis{}, &noopMQTT{}, log, domain.NewMetrics(), &passCB{}, nil)
	// Should early return without starting ticker
	p.drainUnassignedMessages()
}

func TestCleanupIdleConsumers_DisabledNoOp(t *testing.T) {
	t.Helper()
	log, _ := logger.NewLogrusLogger("error", "json")
	cfg := minimalConfig()
	cfg.Redis.ConsumerCleanupEnabled = false
	p := NewStreamProcessor(cfg, &ackFakeRedis{}, &noopMQTT{}, log, domain.NewMetrics(), &passCB{}, nil)
	// Should early return without starting ticker
	p.cleanupIdleConsumers()
}

func TestClaimStaleMessages_CtxDoneQuickExit(t *testing.T) {
	t.Helper()
	log, _ := logger.NewLogrusLogger("error", "json")
	cfg := minimalConfig()
	// Make intervals small to avoid long waits if misconfigured
	cfg.Redis.ClaimInterval = 1
	p := NewStreamProcessor(cfg, &ackFakeRedis{}, &noopMQTT{}, log, domain.NewMetrics(), &passCB{}, nil)

	// Cancel context before calling to exercise ctx.Done() branch
	p.cancel()
	// Direct call should exit quickly
	p.claimStaleMessages()
}
