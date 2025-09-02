package processor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ibs-source/syslog/consumer/golang/internal/domain"
	"github.com/ibs-source/syslog/consumer/golang/internal/logger"
	"github.com/ibs-source/syslog/consumer/golang/internal/ports"
)

// Fakes focused on error branches and loop tickers

const ackTopicConst = "ack/topic"

type redisErrAcks struct {
	ackErr    bool
	deleteErr bool
}

func (r *redisErrAcks) CreateConsumerGroup(
	_ context.Context, _ string, _ string, _ string,
) error {
	return nil
}
func (r *redisErrAcks) ReadMessages(
	_ context.Context, _ string, _ string, _ string, _ int64, _ time.Duration,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (r *redisErrAcks) AckMessages(_ context.Context, _ string, _ string, _ ...string) error {
	if r.ackErr {
		return errors.New("ack error")
	}
	return nil
}
func (r *redisErrAcks) DeleteMessages(_ context.Context, _ string, _ ...string) error {
	if r.deleteErr {
		return errors.New("del error")
	}
	return nil
}
func (r *redisErrAcks) ClaimPendingMessages(
	_ context.Context, _ string, _ string, _ string, _ time.Duration, _ int64,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (r *redisErrAcks) GetPendingMessages(
	_ context.Context, _ string, _ string, _ string, _ string, _ int64,
) ([]ports.PendingMessage, error) {
	return []ports.PendingMessage{}, nil
}
func (r *redisErrAcks) GetConsumers(_ context.Context, _ string, _ string) ([]ports.ConsumerInfo, error) {
	return []ports.ConsumerInfo{}, nil
}
func (r *redisErrAcks) RemoveConsumer(_ context.Context, _ string, _ string, _ string) error {
	return nil
}
func (r *redisErrAcks) ReadStreamMessages(
	_ context.Context, _ string, _ string, _ int64,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (r *redisErrAcks) GetStreamInfo(_ context.Context, _ string) (*ports.StreamInfo, error) {
	return &ports.StreamInfo{Length: 0}, nil
}
func (r *redisErrAcks) GetConsumerGroupInfo(
	_ context.Context, _ string, group string,
) (*ports.ConsumerGroupInfo, error) {
	return &ports.ConsumerGroupInfo{Name: group, LastDeliveredID: "0-0"}, nil
}
func (r *redisErrAcks) GetConsumerName() string      { return "ack-err" }
func (r *redisErrAcks) Ping(_ context.Context) error { return nil }
func (r *redisErrAcks) Close() error                 { return nil }

type mqttNoop struct{}

func (m *mqttNoop) Connect(_ context.Context) error { return nil }
func (m *mqttNoop) Disconnect(_ time.Duration)      {}
func (m *mqttNoop) IsConnected() bool               { return true }
func (m *mqttNoop) GetUserPrefix() string           { return "" }
func (m *mqttNoop) Publish(_ context.Context, _ string, _ byte, _ bool, _ []byte) error {
	return nil
}
func (m *mqttNoop) Subscribe(_ context.Context, _ string, _ byte, _ ports.MessageHandler) error {
	return nil
}
func (m *mqttNoop) Unsubscribe(_ context.Context, _ ...string) error { return nil }

type cbPass2 struct{}

func (cb *cbPass2) Execute(fn func() error) error { return fn() }
func (cb *cbPass2) GetState() string              { return cbClosedState }
func (cb *cbPass2) GetStats() ports.CircuitBreakerStats {
	return ports.CircuitBreakerStats{State: "closed"}
}

func TestHandleAckMessage_ErrorBranches(t *testing.T) {
	t.Helper()
	log, _ := logger.NewLogrusLogger("error", "json")
	cfg := minimalConfig()
	cfg.MQTT.Topics.SubscribeTopic = ackTopicConst
	r := &redisErrAcks{}
	p := NewStreamProcessor(cfg, r, &mqttNoop{}, log, domain.NewMetrics(), &cbPass2{}, nil)

	// malformed json
	p.handleAckMessage(cfg.MQTT.Topics.SubscribeTopic, []byte("{"))

	// missing id
	p.handleAckMessage(cfg.MQTT.Topics.SubscribeTopic, []byte(`{"ack":true}`))

	// ack error path
	r.ackErr = true
	p.handleAckMessage(cfg.MQTT.Topics.SubscribeTopic, []byte(`{"id":"1-1","ack":true}`))
	r.ackErr = false

	// delete error path
	r.deleteErr = true
	p.handleAckMessage(cfg.MQTT.Topics.SubscribeTopic, []byte(`{"id":"1-2","ack":true}`))
}

type redisErrClaim struct {
	errOnce bool
}

func (r *redisErrClaim) CreateConsumerGroup(
	_ context.Context, _ string, _ string, _ string,
) error {
	return nil
}
func (r *redisErrClaim) ReadMessages(
	_ context.Context, _ string, _ string, _ string, _ int64, _ time.Duration,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (r *redisErrClaim) AckMessages(_ context.Context, _ string, _ string, _ ...string) error {
	return nil
}
func (r *redisErrClaim) DeleteMessages(_ context.Context, _ string, _ ...string) error {
	return nil
}
func (r *redisErrClaim) ClaimPendingMessages(
	_ context.Context, _ string, _ string, _ string, _ time.Duration, _ int64,
) ([]*domain.Message, error) {
	if r.errOnce {
		r.errOnce = false
		return nil, errors.New("claim error")
	}
	return []*domain.Message{}, nil
}
func (r *redisErrClaim) GetPendingMessages(
	_ context.Context, _ string, _ string, _ string, _ string, _ int64,
) ([]ports.PendingMessage, error) {
	return []ports.PendingMessage{}, nil
}
func (r *redisErrClaim) GetConsumers(_ context.Context, _ string, _ string) ([]ports.ConsumerInfo, error) {
	return []ports.ConsumerInfo{}, nil
}
func (r *redisErrClaim) RemoveConsumer(_ context.Context, _ string, _ string, _ string) error {
	return nil
}
func (r *redisErrClaim) ReadStreamMessages(
	_ context.Context, _ string, _ string, _ int64,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (r *redisErrClaim) GetStreamInfo(_ context.Context, _ string) (*ports.StreamInfo, error) {
	return &ports.StreamInfo{Length: 0}, nil
}
func (r *redisErrClaim) GetConsumerGroupInfo(
	_ context.Context, _ string, group string,
) (*ports.ConsumerGroupInfo, error) {
	return &ports.ConsumerGroupInfo{Name: group, LastDeliveredID: "0-0"}, nil
}
func (r *redisErrClaim) GetConsumerName() string      { return "claim-err" }
func (r *redisErrClaim) Ping(_ context.Context) error { return nil }
func (r *redisErrClaim) Close() error                 { return nil }

func TestPerformSingleClaim_Error(t *testing.T) {
	t.Helper()
	log, _ := logger.NewLogrusLogger("error", "json")
	cfg := minimalConfig()
	r := &redisErrClaim{errOnce: true}
	p := NewStreamProcessor(cfg, r, &mqttNoop{}, log, domain.NewMetrics(), &cbPass2{}, nil)
	p.performSingleClaim() // should handle error branch gracefully
}

func TestPerformAggressiveClaim_ErrorBreaks(t *testing.T) {
	t.Helper()
	log, _ := logger.NewLogrusLogger("error", "json")
	cfg := minimalConfig()
	cfg.Redis.AggressiveClaim = true
	r := &redisErrClaim{errOnce: true}
	p := NewStreamProcessor(cfg, r, &mqttNoop{}, log, domain.NewMetrics(), &cbPass2{}, nil)
	p.performAggressiveClaim() // should hit error and break
}

type redisLoopTick struct {
	consumers []ports.ConsumerInfo
}

func (r *redisLoopTick) CreateConsumerGroup(
	_ context.Context, _ string, _ string, _ string,
) error {
	return nil
}
func (r *redisLoopTick) ReadMessages(
	_ context.Context, _ string, _ string, _ string, _ int64, _ time.Duration,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (r *redisLoopTick) AckMessages(_ context.Context, _ string, _ string, _ ...string) error {
	return nil
}
func (r *redisLoopTick) DeleteMessages(_ context.Context, _ string, _ ...string) error {
	return nil
}
func (r *redisLoopTick) ClaimPendingMessages(
	_ context.Context, _ string, _ string, _ string, _ time.Duration, _ int64,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (r *redisLoopTick) GetPendingMessages(
	_ context.Context, _ string, _ string, _ string, _ string, _ int64,
) ([]ports.PendingMessage, error) {
	return []ports.PendingMessage{}, nil
}
func (r *redisLoopTick) GetConsumers(_ context.Context, _ string, _ string) ([]ports.ConsumerInfo, error) {
	return r.consumers, nil
}
func (r *redisLoopTick) RemoveConsumer(_ context.Context, _ string, _ string, _ string) error {
	return nil
}
func (r *redisLoopTick) ReadStreamMessages(
	_ context.Context, _ string, _ string, _ int64,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (r *redisLoopTick) GetStreamInfo(_ context.Context, _ string) (*ports.StreamInfo, error) {
	return &ports.StreamInfo{Length: 0}, nil
}
func (r *redisLoopTick) GetConsumerGroupInfo(
	_ context.Context, _ string, group string,
) (*ports.ConsumerGroupInfo, error) {
	return &ports.ConsumerGroupInfo{Name: group, LastDeliveredID: "0-0"}, nil
}
func (r *redisLoopTick) GetConsumerName() string      { return "loop-tick" }
func (r *redisLoopTick) Ping(_ context.Context) error { return nil }
func (r *redisLoopTick) Close() error                 { return nil }

func TestDrainAndCleanupLoops_TickOnce(t *testing.T) {
	t.Helper()
	log, _ := logger.NewLogrusLogger("error", "json")
	cfg := minimalConfig()
	cfg.Redis.DrainEnabled = true
	cfg.Redis.DrainInterval = 10 * time.Millisecond
	cfg.Redis.ConsumerCleanupEnabled = true
	cfg.Redis.ConsumerCleanupInterval = 10 * time.Millisecond
	r := &redisLoopTick{
		consumers: []ports.ConsumerInfo{
			{Name: "self", Pending: 0, Idle: 0},
			{Name: "idle", Pending: 0, Idle: 1 * time.Hour},
		},
	}
	p := NewStreamProcessor(cfg, r, &mqttNoop{}, log, domain.NewMetrics(), &cbPass2{}, nil)

	// Run both loops and stop shortly after one tick
	go p.drainUnassignedMessages()
	go p.cleanupIdleConsumers()
	time.Sleep(25 * time.Millisecond)
	p.cancel()
}
