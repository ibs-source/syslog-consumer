// Package processor orchestrates the lock-free, zero-copy processing pipeline.
package processor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ibs-source/syslog/consumer/golang/pkg/jsonx"

	"github.com/ibs-source/syslog/consumer/golang/internal/config"
	"github.com/ibs-source/syslog/consumer/golang/internal/domain"
	"github.com/ibs-source/syslog/consumer/golang/internal/ports"
	"github.com/ibs-source/syslog/consumer/golang/pkg/ringbuffer"
)

// State represents the state of the processor
type State int32

// State values for the processor lifecycle.
const (
	StateIdle State = iota
	StateRunning
	StatePaused
	StateStopping
	StateStopped
)

// State string representations
const (
	StateIdleStr     = "idle"
	StateRunningStr  = "running"
	StatePausedStr   = "paused"
	StateStoppingStr = "stopping"
	StateStoppedStr  = "stopped"
)

// Drop policy string constants to avoid repeated literals
const (
	DropOldest = "oldest"
	DropNewest = "newest"
	DropNone   = "none"
)

// nonNegInt64ToUint64 converts int64 to uint64 with non-negative clamp to avoid risky casts (gosec G115).
func nonNegInt64ToUint64(v int64) uint64 {
	if v <= 0 {
		return 0
	}
	return uint64(v)
}

// nonNegIntToUint32 converts int to uint32 with bounds clamping to avoid risky casts (gosec G115).
func nonNegIntToUint32(v int) uint32 {
	if v <= 0 {
		return 0
	}
	if v > int(math.MaxUint32) {
		return math.MaxUint32
	}
	return uint32(v)
}

// Nominative sub-structures (embedded) to keep memory layout flat while grouping concerns.

// Deps groups external dependencies and configuration (passed by reference)
type Deps struct {
	cfg         *config.Config
	redisClient ports.RedisClient
	mqttClient  ports.MQTTClient
	logger      ports.Logger
	metrics     *domain.Metrics
	publishCB   ports.CircuitBreaker
}

// Lifecycle groups lifecycle state and worker resources
type Lifecycle struct {
	state      atomic.Int32
	workerPool *WorkerPool
	bgWg       sync.WaitGroup
}

// Buffer holds the buffering structure
type Buffer struct {
	buffer *ringbuffer.RingBuffer[domain.Message]
}

// Control holds control channels and context
type Control struct {
	ctx      context.Context
	cancel   context.CancelFunc
	pauseCh  chan struct{}
	resumeCh chan struct{}
}

// Stats holds processing statistics
type Stats struct {
	processedCount atomic.Uint64
	errorCount     atomic.Uint64
	lastError      atomic.Value
}

// Hooks holds optional testing hooks/channels
type Hooks struct {
	messageProcessedCh chan struct{}
}

// StreamProcessor composes nominative sub-structures (embedded)
type StreamProcessor struct {
	Deps
	Lifecycle
	Buffer
	Control
	Stats
	Hooks
}

// NewStreamProcessor creates a new stream processor (dependencies by reference; minimal allocations)
func NewStreamProcessor(
	cfg *config.Config,
	redis ports.RedisClient,
	mqtt ports.MQTTClient,
	logger ports.Logger,
	metrics *domain.Metrics,
	publishCB ports.CircuitBreaker,
	messageProcessedCh chan struct{},
) *StreamProcessor {
	ctx, cancel := context.WithCancel(context.Background())
	capInt := cfg.Pipeline.BufferSize
	if capInt < 0 {
		capInt = 0
	}
	if capInt > int(math.MaxUint32) {
		capInt = int(math.MaxUint32)
	}
	buf := ringbuffer.New[domain.Message](nonNegIntToUint32(capInt))

	return &StreamProcessor{
		Deps: Deps{
			cfg:         cfg,
			redisClient: redis,
			mqttClient:  mqtt,
			logger:      logger.WithFields(ports.Field{Key: "component", Value: "stream-processor"}),
			metrics:     metrics,
			publishCB:   publishCB,
		},
		Lifecycle: Lifecycle{
			state:      atomic.Int32{},
			workerPool: nil,
		},
		Buffer: Buffer{
			buffer: buf,
		},
		Control: Control{
			ctx:      ctx,
			cancel:   cancel,
			pauseCh:  make(chan struct{}),
			resumeCh: make(chan struct{}),
		},
		Stats: Stats{
			processedCount: atomic.Uint64{},
			errorCount:     atomic.Uint64{},
			lastError:      atomic.Value{},
		},
		Hooks: Hooks{
			messageProcessedCh: messageProcessedCh,
		},
	}
}

func (p *StreamProcessor) initWorkerPool() {
	p.workerPool = NewWorkerPoolWithParent(p.ctx, p.cfg.Resource.MinWorkers, p.cfg.Resource.MaxWorkers)

	// Configure lock-free fast-path queue capacity from config (must be set before Start)
	capU := nonNegIntToUint32(p.cfg.Pipeline.WorkerQueueSize)
	if capU > 0 {
		p.workerPool.SetMsgQueueCapacity(capU)
	}

	// Set lock-free message handler fast-path
	p.workerPool.SetMsgHandler(func(m *domain.Message) {
		if m == nil {
			return
		}
		p.logger.Debug("Executing message task", ports.Field{Key: "messageID", Value: m.ID})
		p.processMessage(m)
	})
	p.workerPool.Start()
}

func (p *StreamProcessor) launchBackgroundTasks() {
	// Start background tasks
	p.bgWg.Add(1)
	go func() { defer p.bgWg.Done(); p.consumeMessages() }()
	p.bgWg.Add(1)
	go func() { defer p.bgWg.Done(); p.processMessages() }()
	p.bgWg.Add(1)
	go func() { defer p.bgWg.Done(); p.claimStaleMessages() }()
	p.bgWg.Add(1)
	go func() { defer p.bgWg.Done(); p.monitorBackpressure() }()

	// Start advanced claim and drain tasks if enabled
	if p.cfg.Redis.DrainEnabled {
		p.bgWg.Add(1)
		go func() { defer p.bgWg.Done(); p.drainUnassignedMessages() }()
	}
	if p.cfg.Redis.ConsumerCleanupEnabled {
		p.bgWg.Add(1)
		go func() { defer p.bgWg.Done(); p.cleanupIdleConsumers() }()
	}
}

// Start starts the stream processor
func (p *StreamProcessor) Start(ctx context.Context) error {
	if !p.state.CompareAndSwap(int32(StateIdle), int32(StateRunning)) {
		return errors.New("processor already running")
	}

	p.logger.Info("Starting stream processor")

	// Create consumer group
	err := p.redisClient.CreateConsumerGroup(
		ctx,
		p.cfg.Redis.StreamName,
		p.cfg.Redis.ConsumerGroup,
		"0-0",
	)
	if err != nil {
		p.state.Store(int32(StateIdle))
		return fmt.Errorf("failed to create consumer group: %w", err)
	}

	// Subscribe to acknowledgment topic
	ackTopic := p.cfg.MQTT.Topics.SubscribeTopic
	err = p.mqttClient.Subscribe(ctx, ackTopic, p.cfg.MQTT.QoS, p.handleAckMessage)
	if err != nil {
		p.state.Store(int32(StateIdle))
		return fmt.Errorf("failed to subscribe to ack topic: %w", err)
	}

	// Initialize worker pool
	p.initWorkerPool()

	// Launch background tasks
	p.launchBackgroundTasks()

	p.logger.Info("Stream processor started successfully")
	return nil
}

// Stop stops the stream processor gracefully
func (p *StreamProcessor) Stop(ctx context.Context) error {
	if !p.state.CompareAndSwap(int32(StateRunning), int32(StateStopping)) &&
		!p.state.CompareAndSwap(int32(StatePaused), int32(StateStopping)) {
		return errors.New("processor not running")
	}

	p.logger.Info("Stopping stream processor")

	// Cancel context to signal shutdown
	p.cancel()

	// Wait for graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(ctx, p.cfg.App.ShutdownTimeout)
	defer shutdownCancel()

	// Stop worker pool with timeout to guarantee bounded shutdown
	if p.workerPool != nil {
		p.workerPool.StopWithTimeout(shutdownCtx)
	}

	// Unsubscribe from MQTT topics
	ackTopic := p.cfg.MQTT.Topics.SubscribeTopic
	_ = p.mqttClient.Unsubscribe(shutdownCtx, ackTopic)

	// Wait for background goroutines to exit or timeout
	done := make(chan struct{})
	go func() {
		p.bgWg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-shutdownCtx.Done():
		p.logger.Warn("Shutdown timeout reached while waiting for processor goroutines")
	}

	p.state.Store(int32(StateStopped))
	p.logger.Info("Stream processor stopped")
	return nil
}

// Pause pauses the stream processor
func (p *StreamProcessor) Pause() error {
	if !p.state.CompareAndSwap(int32(StateRunning), int32(StatePaused)) {
		return errors.New("processor not running")
	}

	p.logger.Info("Stream processor paused")
	return nil
}

// Resume resumes the stream processor
func (p *StreamProcessor) Resume() error {
	if !p.state.CompareAndSwap(int32(StatePaused), int32(StateRunning)) {
		return errors.New("processor not paused")
	}

	p.logger.Info("Stream processor resumed")
	return nil
}

// consumeMessages consumes messages from Redis stream
func (p *StreamProcessor) consumeMessages() {
	defer func() {
		if r := recover(); r != nil {
			p.logger.Error("Panic in consumeMessages", ports.Field{Key: "panic", Value: r})
		}
	}()

	p.logger.Debug("Starting consumeMessages goroutine")

	for {
		if p.ctx.Err() != nil {
			p.logger.Debug("consumeMessages: context done, exiting")
			return
		}
		if p.handlePauseState("consumeMessages") {
			return // context done while paused
		}
		p.readAndBufferMessages()
	}
}

// processMessages processes buffered messages
func (p *StreamProcessor) processMessages() {
	defer func() {
		if r := recover(); r != nil {
			p.logger.Error("Panic in processMessages", ports.Field{Key: "panic", Value: r})
		}
	}()

	batch := make([]*domain.Message, 0, p.cfg.Pipeline.BatchSize)
	ticker := time.NewTicker(p.cfg.Pipeline.FlushInterval)
	defer ticker.Stop()

	scratch := make([]*domain.Message, p.cfg.Pipeline.BatchSize)

	for {
		if p.ctx.Err() != nil {
			return
		}
		if p.handlePauseState("processMessages") {
			return // context done while paused
		}
		if p.processBatchCycle(&batch, scratch) {
			if p.ctx.Err() != nil {
				return
			}
			continue
		}
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.flushBatch(&batch)
		default:
			p.idleSleep()
		}
	}
}

// flushBatch processes the current batch if non-empty and clears it.
func (p *StreamProcessor) flushBatch(batch *[]*domain.Message) {
	if len(*batch) == 0 {
		return
	}
	p.processBatch(*batch)
	*batch = (*batch)[:0]
}

// fetchIntoBatch tries to fetch messages from the buffer into the batch.
// Returns true if any messages were fetched.
func (p *StreamProcessor) fetchIntoBatch(batch *[]*domain.Message, scratch []*domain.Message) bool {
	needed := p.cfg.Pipeline.BatchSize - len(*batch)
	if needed <= 0 {
		return false
	}
	n := p.buffer.TryGetBatch(scratch[:needed])
	if n <= 0 {
		return false
	}
	*batch = append(*batch, scratch[:n]...)
	return true
}

// processBatch processes a batch of messages
func (p *StreamProcessor) processBatch(messages []*domain.Message) {
	if len(messages) == 0 {
		return
	}

	// Fast-path: batch submit to worker pool's lock-free queue
	if p.workerPool != nil && p.workerPool.msgHandler != nil {
		inserted := p.workerPool.SubmitBatch(messages)
		// Drop any remainder to maintain lock-free, zero-copy behavior (no channel fallback)
		if inserted < len(messages) {
			dropped := len(messages) - inserted
			if dropped > 0 {
				p.metrics.MessagesDropped.Add(uint64(dropped))
			}
			p.logger.Warn("Worker queue full, dropping remainder",
				ports.Field{Key: "reason", Value: "worker-queue-full"},
				ports.Field{Key: "inserted", Value: inserted},
				ports.Field{Key: "dropped", Value: dropped},
			)
		}
		return
	}

	// Legacy path (should not normally happen)
	for _, msg := range messages {
		p.submitMessageTask(msg)
	}
}

func (p *StreamProcessor) shouldSkipSubmit() bool {
	st := State(p.state.Load())
	return p.ctx.Err() != nil || st == StateStopping || st == StateStopped
}

func (p *StreamProcessor) submitViaFastPath(msg *domain.Message) {
	err := p.workerPool.SubmitMsg(msg)
	if err == nil {
		return
	}
	if errors.Is(err, ErrQueueFull) {
		p.logger.Warn("Worker queue full, dropping message",
			ports.Field{Key: "messageID", Value: msg.ID})
		p.metrics.MessagesDropped.Add(1)
		return
	}
	p.logger.Error("Failed to submit message task",
		ports.Field{Key: "messageID", Value: msg.ID},
		ports.Field{Key: "error", Value: err})
	p.metrics.MessagesDropped.Add(1)
}

// submitMessageTask submits a message processing task to the worker pool
func (p *StreamProcessor) submitMessageTask(msg *domain.Message) {
	if p.shouldSkipSubmit() {
		return
	}

	p.logger.Debug("Submitting message task", ports.Field{Key: "messageID", Value: msg.ID})

	wp := p.workerPool
	if wp == nil {
		// No worker pool (should not normally happen)
		go p.processMessage(msg)
		return
	}

	if wp.msgHandler != nil {
		p.submitViaFastPath(msg)
		return
	}

	// Legacy path when worker pool exists but no fast-path handler set: drop to maintain lock-free semantics
	p.logger.Error("Worker pool message handler not configured, dropping message",
		ports.Field{Key: "messageID", Value: msg.ID})
	p.metrics.MessagesDropped.Add(1)
}

// ensureCapacityForIncoming ensures capacity for n incoming messages by dropping oldest if configured.
func (p *StreamProcessor) ensureCapacityForIncoming(n int, reason string) int {
	if p.cfg.Pipeline.DropPolicy != DropOldest || n <= 0 {
		return 0
	}
	dropped := p.buffer.EnsureCapacityOrDropOldest(n, func(_ *domain.Message) {
		p.metrics.MessagesDropped.Add(1)
	})
	if dropped > 0 {
		p.logger.Warn("Dropped oldest to buffer messages",
			ports.Field{Key: "reason", Value: reason},
			ports.Field{Key: "dropped", Value: dropped},
			ports.Field{Key: "incoming", Value: n},
		)
	}
	return dropped
}

// bufferBatch buffers a batch of messages with unified drop/capacity handling.
func (p *StreamProcessor) bufferBatch(messages []*domain.Message, reason string) {
	if len(messages) == 0 {
		return
	}
	p.ensureCapacityForIncoming(len(messages), reason)

	inserted := p.buffer.TryPutBatch(messages)
	if inserted < len(messages) {
		dropped := len(messages) - inserted
		var uDropped uint64
		if dropped > 0 {
			uDropped = uint64(dropped)
		} else {
			uDropped = 0
		}
		p.metrics.MessagesDropped.Add(uDropped)
		p.logger.Warn("Buffer full, some messages dropped",
			ports.Field{Key: "reason", Value: reason},
			ports.Field{Key: "inserted", Value: inserted},
			ports.Field{Key: "dropped", Value: dropped},
		)
	}
}

// processMessage processes a single message
func (p *StreamProcessor) processMessage(msg *domain.Message) {
	start := time.Now()
	defer func() {
		duration := time.Since(start)
		dn := duration.Nanoseconds()
		if dn < 0 {
			dn = 0
		}
		p.metrics.ProcessingTimeNs.Add(nonNegInt64ToUint64(dn))
		p.logger.Debug("Message processing completed",
			ports.Field{Key: "messageID", Value: msg.ID},
			ports.Field{Key: "duration", Value: duration})
	}()

	p.logger.Trace("Starting message processing",
		ports.Field{Key: "messageID", Value: msg.ID},
		ports.Field{Key: "data", Value: string(msg.Data)},
		ports.Field{Key: "attempts", Value: msg.Attempts})

	payloadBytes := p.buildPublishPayload(msg)

	if err := p.publishToMQTT(payloadBytes); err != nil {
		p.logger.Debug("MQTT publish failed", ports.Field{Key: "error", Value: err})
		p.handleMessageError(msg, err)
		return
	}

	p.processedCount.Add(1)
	p.metrics.MessagesPublished.Add(1)
	p.logger.Debug("Message published successfully",
		ports.Field{Key: "messageID", Value: msg.ID},
		ports.Field{Key: "topic", Value: p.cfg.MQTT.Topics.PublishTopic},
		ports.Field{Key: "totalProcessed", Value: p.processedCount.Load()})

	// Signal for integration tests
	if p.messageProcessedCh != nil {
		p.logger.Debug("Signaling message processed channel")
		p.messageProcessedCh <- struct{}{}
	}
}

// buildPublishPayload constructs the MQTT payload with zero-copy techniques.
func (p *StreamProcessor) buildPublishPayload(msg *domain.Message) []byte {
	// Preserve original payload as-is; only decode/replace struct_data if it exists and is valid JSON
	payloadData := msg.Data
	if s, ok := jsonx.GetTopLevelString(payloadData, "struct_data"); ok && jsonx.IsLikelyJSONString(s) {
		if newPayload, replaced := jsonx.ReplaceTopLevelKey(payloadData, "struct_data", []byte(s)); replaced {
			payloadData = newPayload
		}
	}

	// {"message":{"payload":<payloadData>},"redis":{"payload":{"id":"<msg.ID>"}}}
	idQuoted, _ := jsonx.Marshal(msg.ID)

	buf := domain.BufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer func() {
		buf.Reset()
		domain.BufferPool.Put(buf)
	}()

	buf.WriteString(`{"message":{"payload":`)
	buf.Write(payloadData)
	buf.WriteString(`},"redis":{"payload":{"id":`)
	buf.Write(idQuoted)
	buf.WriteString(`}}}`)

	// Copy buffer to a compact slice for publishing (avoid retaining large capacity)
	payloadBytes := make([]byte, buf.Len())
	copy(payloadBytes, buf.Bytes())

	// Log publish intent
	topic := p.cfg.MQTT.Topics.PublishTopic
	p.logger.Debug("Publishing to MQTT",
		ports.Field{Key: "topic", Value: topic},
		ports.Field{Key: "qos", Value: p.cfg.MQTT.QoS},
		ports.Field{Key: "payload_bytes", Value: len(payloadBytes)})

	return payloadBytes
}

// publishToMQTT publishes the payload via the circuit breaker.
func (p *StreamProcessor) publishToMQTT(payloadBytes []byte) error {
	topic := p.cfg.MQTT.Topics.PublishTopic
	return p.publishCB.Execute(func() error {
		return p.mqttClient.Publish(
			p.ctx,
			topic,
			p.cfg.MQTT.QoS,
			false,
			payloadBytes,
		)
	})
}

// handleMessageError handles message processing errors
func (p *StreamProcessor) handleMessageError(msg *domain.Message, err error) {
	p.errorCount.Add(1)
	p.lastError.Store(err.Error())
	p.metrics.ProcessingErrors.Add(1)

	p.logger.Error("Failed to process message",
		ports.Field{Key: "messageID", Value: msg.ID},
		ports.Field{Key: "error", Value: err},
		ports.Field{Key: "attempts", Value: msg.Attempts},
	)

	// Increment attempts
	msg.Attempts++

	// Check for retry
	if p.cfg.Pipeline.Retry.Enabled && int(msg.Attempts) <= p.cfg.Pipeline.Retry.MaxAttempts {
		backoff := p.computeBackoff(int(msg.Attempts))
		p.logger.Warn("Retrying message",
			ports.Field{Key: "messageID", Value: msg.ID},
			ports.Field{Key: "attempts", Value: msg.Attempts},
			ports.Field{Key: "backoff", Value: backoff},
		)
		go p.retryMessageAfter(backoff, msg)
		return
	}

	// If retries exhausted or retry is disabled, send to DLQ or drop
	if p.cfg.Pipeline.DLQ.Enabled {
		p.publishToDLQ(msg, err)
	} else {
		p.logger.Error("Max retries reached, dropping message", ports.Field{Key: "messageID", Value: msg.ID})
		p.metrics.MessagesDropped.Add(1)
	}
}

// computeBackoff calculates exponential backoff with limits.
func (p *StreamProcessor) computeBackoff(attempts int) time.Duration {
	backoff := p.cfg.Pipeline.Retry.InitialBackoff
	for i := 1; i < attempts; i++ {
		backoff = time.Duration(float64(backoff) * p.cfg.Pipeline.Retry.Multiplier)
	}
	if backoff > p.cfg.Pipeline.Retry.MaxBackoff {
		backoff = p.cfg.Pipeline.Retry.MaxBackoff
	}
	return backoff
}

// publishToDLQ builds the DLQ payload and publishes it.
func (p *StreamProcessor) publishToDLQ(msg *domain.Message, origErr error) {
	p.logger.Error("Max retries reached, sending to DLQ", ports.Field{Key: "messageID", Value: msg.ID})

	dlqPayload := map[string]interface{}{
		"original_message": string(msg.Data),
		"error":            origErr.Error(),
		"timestamp":        time.Now().Format(time.RFC3339),
		"message_id":       msg.ID,
		"attempts":         msg.Attempts,
	}
	dlqPayloadBytes, marshalErr := jsonx.Marshal(dlqPayload)
	if marshalErr != nil {
		p.logger.Error("Failed to marshal DLQ payload", ports.Field{Key: "error", Value: marshalErr})
		p.metrics.MessagesDropped.Add(1)
		return
	}

	err := p.mqttClient.Publish(
		p.ctx,
		p.cfg.Pipeline.DLQ.Topic,
		p.cfg.MQTT.QoS,
		false,
		dlqPayloadBytes,
	)
	if err != nil {
		p.logger.Error("Failed to publish message to DLQ",
			ports.Field{Key: "messageID", Value: msg.ID},
			ports.Field{Key: "error", Value: err})
		p.metrics.MessagesDropped.Add(1)
	}
}

// retryMessageAfter waits backoff and attempts to re-queue the message (explicit function, early-return style)
func (p *StreamProcessor) retryMessageAfter(backoff time.Duration, msg *domain.Message) {
	timer := time.NewTimer(backoff)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-p.ctx.Done():
		return
	}

	// For "oldest" policy ensure capacity before requeue
	if p.cfg.Pipeline.DropPolicy == DropOldest {
		p.buffer.EnsureCapacityOrDropOldest(1, func(_ *domain.Message) {
			p.metrics.MessagesDropped.Add(1)
		})
	}

	if p.ctx.Err() != nil {
		return
	}
	if p.buffer.Put(msg) {
		return
	}

	p.logger.Error("Failed to re-buffer message for retry", ports.Field{Key: "messageID", Value: msg.ID})
	p.metrics.MessagesDropped.Add(1)
}

// handleAckMessage handles acknowledgment messages from MQTT
func (p *StreamProcessor) handleAckMessage(topic string, payload []byte) {
	p.logger.Debug("Received acknowledgment message",
		ports.Field{Key: "topic", Value: topic},
		ports.Field{Key: "payload_bytes", Value: len(payload)},
	)
	p.logger.Trace("Ack payload raw", ports.Field{Key: "payload", Value: string(payload)})

	var ack domain.AckMessage
	if err := jsonx.Unmarshal(payload, &ack); err != nil {
		p.logger.Error("Failed to unmarshal ack message",
			ports.Field{Key: "error", Value: err},
		)
		return
	}

	// Check if we have the required fields
	if ack.ID == "" {
		p.logger.Error("Ack message missing Redis ID")
		return
	}

	p.logger.Trace("ACK parsed",
		ports.Field{Key: "redisID", Value: ack.ID},
		ports.Field{Key: "ack", Value: ack.Ack},
	)

	if ack.Ack {
		p.logger.Trace("Acking in Redis",
			ports.Field{Key: "stream", Value: p.cfg.Redis.StreamName},
			ports.Field{Key: "group", Value: p.cfg.Redis.ConsumerGroup},
			ports.Field{Key: "redisID", Value: ack.ID},
		)

		// Acknowledge and delete the message from Redis
		err := p.redisClient.AckMessages(
			p.ctx,
			p.cfg.Redis.StreamName,
			p.cfg.Redis.ConsumerGroup,
			ack.ID,
		)
		if err != nil {
			p.logger.Error("Failed to ack message in Redis",
				ports.Field{Key: "redisID", Value: ack.ID},
				ports.Field{Key: "error", Value: err},
			)
			return
		}

		p.metrics.MessagesAcked.Add(1)
		p.logger.Debug("Redis XACK success", ports.Field{Key: "redisID", Value: ack.ID})

		// Delete the message from the stream
		if err := p.redisClient.DeleteMessages(p.ctx, p.cfg.Redis.StreamName, ack.ID); err != nil {
			p.logger.Error("Failed to delete message from Redis",
				ports.Field{Key: "redisID", Value: ack.ID},
				ports.Field{Key: "error", Value: err},
			)
		} else {
			p.logger.Debug("Redis XDEL success", ports.Field{Key: "redisID", Value: ack.ID})
		}
	} else {
		// Negative acknowledgment - message will be retried
		p.logger.Warn("Received negative acknowledgment", ports.Field{Key: "redisID", Value: ack.ID})
	}
}

// claimStaleMessages claims messages from dead consumers
func (p *StreamProcessor) claimStaleMessages() {
	ticker := time.NewTicker(p.cfg.Redis.ClaimInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			if p.cfg.Redis.AggressiveClaim {
				p.performAggressiveClaim()
			} else {
				p.performSingleClaim()
			}
		}
	}
}

// performSingleClaim performs a single claim operation
func (p *StreamProcessor) performSingleClaim() {
	messages, err := p.redisClient.ClaimPendingMessages(
		p.ctx,
		p.cfg.Redis.StreamName,
		p.cfg.Redis.ConsumerGroup,
		p.redisClient.GetConsumerName(),
		p.cfg.Redis.ClaimMinIdleTime,
		int64(p.cfg.Redis.ClaimBatchSize),
	)
	if err != nil {
		p.logger.Error("Failed to claim messages", ports.Field{Key: "error", Value: err})
		return
	}

	if len(messages) == 0 {
		return
	}

	p.logger.Info("Claimed stale messages", ports.Field{Key: "count", Value: len(messages)})
	if len(messages) > 0 {
		p.bufferBatch(messages, "claim")
	}
}

// performAggressiveClaim performs aggressive claiming until no more messages can be claimed
func (p *StreamProcessor) performAggressiveClaim() {
	totalClaimed := 0
	cycles := 0

	for {
		// Check if we should stop
		if p.ctx.Err() != nil {
			return
		}

		messages, err := p.redisClient.ClaimPendingMessages(
			p.ctx,
			p.cfg.Redis.StreamName,
			p.cfg.Redis.ConsumerGroup,
			p.redisClient.GetConsumerName(),
			p.cfg.Redis.ClaimMinIdleTime,
			int64(p.cfg.Redis.ClaimBatchSize),
		)
		if err != nil {
			p.logger.Error("Failed to claim messages in aggressive mode", ports.Field{Key: "error", Value: err})
			break
		}

		if len(messages) == 0 {
			// No more messages to claim
			if totalClaimed > 0 {
				p.logger.Info("Aggressive claim completed",
					ports.Field{Key: "totalClaimed", Value: totalClaimed},
					ports.Field{Key: "cycles", Value: cycles},
				)
			}
			break
		}

		// Buffer the claimed messages
		if len(messages) > 0 {
			p.bufferBatch(messages, "aggressive-claim")
		}

		totalClaimed += len(messages)
		cycles++

		// Add a small delay between cycles to avoid overwhelming the system
		if p.cfg.Redis.ClaimCycleDelay > 0 {
			select {
			case <-p.ctx.Done():
				return
			case <-time.After(p.cfg.Redis.ClaimCycleDelay):
			}
		}
	}
}

// monitorBackpressure monitors and handles backpressure
func (p *StreamProcessor) monitorBackpressure() {
	interval := p.cfg.Pipeline.BackpressurePollInterval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.backpressureTick()
		}
	}
}

// backpressureTick performs one backpressure/telemetry update cycle (split to reduce complexity).
func (p *StreamProcessor) backpressureTick() {
	usage := float64(p.buffer.Size()) / float64(p.buffer.Capacity())

	if usage > p.cfg.Pipeline.BackpressureThreshold && p.cfg.Pipeline.DropPolicy == DropNewest {
		// Accounting/log only; drops occur via ringbuffer write failures.
		p.metrics.BackpressureDropped.Add(1)
		p.logger.Warn("Backpressure threshold exceeded, dropping new messages",
			ports.Field{Key: "usage", Value: usage},
		)
	}

	// Update buffer utilization
	p.metrics.BufferUtilization.Store(uint64(usage * 100))

	// Export worker telemetry
	p.exportWorkerTelemetry()
}

// exportWorkerTelemetry reports worker count and queue depth with safe bounds (avoid G115).
func (p *StreamProcessor) exportWorkerTelemetry() {
	if p.workerPool == nil {
		return
	}

	wc := p.workerPool.GetWorkerCount()
	qd := p.workerPool.GetQueueDepth()

	if wc < 0 {
		wc = 0
	}
	if qd < 0 {
		qd = 0
	}

	if wc > int(math.MaxInt32) {
		p.metrics.ActiveWorkers.Store(int32(math.MaxInt32))
	} else {
		// #nosec G115 -- int32 cast is safe due to explicit guard above
		p.metrics.ActiveWorkers.Store(int32(wc))
	}
	if qd > int(math.MaxInt32) {
		p.metrics.QueueDepth.Store(int32(math.MaxInt32))
	} else {
		// #nosec G115 -- int32 cast is safe due to explicit guard above
		p.metrics.QueueDepth.Store(int32(qd))
	}
}

// GetMetrics returns current metrics
func (p *StreamProcessor) GetMetrics() *domain.Metrics {
	return p.metrics
}

// GetState returns the current processor state as string
func (p *StreamProcessor) GetState() string {
	state := State(p.state.Load())
	switch state {
	case StateIdle:
		return StateIdleStr
	case StateRunning:
		return StateRunningStr
	case StatePaused:
		return StatePausedStr
	case StateStopping:
		return StateStoppingStr
	case StateStopped:
		return StateStoppedStr
	default:
		return "unknown"
	}
}

// drainUnassignedMessages drains messages that haven't been assigned to any consumer
func (p *StreamProcessor) drainUnassignedMessages() {
	if !p.cfg.Redis.DrainEnabled {
		return
	}

	ticker := time.NewTicker(p.cfg.Redis.DrainInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.performDrain()
		}
	}
}

// performDrain performs the actual drain operation
func (p *StreamProcessor) performDrain() {
	// Get consumer group info
	groupInfo, err := p.redisClient.GetConsumerGroupInfo(
		p.ctx,
		p.cfg.Redis.StreamName,
		p.cfg.Redis.ConsumerGroup,
	)
	if err != nil {
		p.logger.Error("Failed to get consumer group info", ports.Field{Key: "error", Value: err})
		return
	}

	// Read messages that haven't been delivered to the consumer group
	// Start from the last delivered ID
	messages, err := p.redisClient.ReadStreamMessages(
		p.ctx,
		p.cfg.Redis.StreamName,
		groupInfo.LastDeliveredID,
		p.cfg.Redis.DrainBatchSize,
	)
	if err != nil {
		p.logger.Error("Failed to read unassigned messages", ports.Field{Key: "error", Value: err})
		return
	}

	if len(messages) == 0 {
		return
	}

	p.logger.Info("Draining unassigned messages", ports.Field{Key: "count", Value: len(messages)})
	if len(messages) > 0 {
		p.bufferBatch(messages, "drain")
	}
}

// handlePauseState handles pause state checking for both consume and process loops.
// Returns true if context was cancelled while paused (caller should exit).
func (p *StreamProcessor) handlePauseState(caller string) bool {
	if State(p.state.Load()) == StatePaused {
		p.logger.Debug(caller + ": paused, waiting for resume")
		for State(p.state.Load()) == StatePaused {
			select {
			case <-p.ctx.Done():
				p.logger.Debug(caller + ": context done while paused, exiting")
				return true
			default:
				s := p.cfg.Pipeline.IdlePollSleep
				if s <= 0 {
					s = time.Millisecond
				}
				time.Sleep(s)
			}
		}
		p.logger.Debug(caller + ": resumed")
	}
	return false
}

// readAndBufferMessages reads messages from Redis and buffers them.
func (p *StreamProcessor) readAndBufferMessages() {
	p.logger.Debug("Reading messages from Redis",
		ports.Field{Key: "group", Value: p.cfg.Redis.ConsumerGroup},
		ports.Field{Key: "consumer", Value: p.redisClient.GetConsumerName()},
		ports.Field{Key: "stream", Value: p.cfg.Redis.StreamName},
		ports.Field{Key: "batchSize", Value: p.cfg.Redis.BatchSize},
		ports.Field{Key: "blockTime", Value: p.cfg.Redis.BlockTime},
	)

	messages, err := p.redisClient.ReadMessages(
		p.ctx,
		p.cfg.Redis.ConsumerGroup,
		p.redisClient.GetConsumerName(),
		p.cfg.Redis.StreamName,
		p.cfg.Redis.BatchSize,
		p.cfg.Redis.BlockTime,
	)
	if err != nil {
		p.errorCount.Add(1)
		p.lastError.Store(err.Error())
		p.metrics.RedisErrors.Add(1)
		p.logger.Error("Failed to read messages", ports.Field{Key: "error", Value: err})
		select {
		case <-p.ctx.Done():
			return
		case <-time.After(p.cfg.Redis.RetryInterval):
			return
		}
	}

	p.logger.Debug("Received messages from Redis", ports.Field{Key: "count", Value: len(messages)})

	// Update metrics for received messages
	if len(messages) > 0 {
		p.metrics.MessagesReceived.Add(uint64(len(messages)))
		p.bufferBatch(messages, "consume")
	}
}

// processBatchCycle attempts to fetch and process messages in one cycle.
// Returns true if messages were fetched and batch should be flushed if full.
func (p *StreamProcessor) processBatchCycle(batch *[]*domain.Message, scratch []*domain.Message) bool {
	if p.fetchIntoBatch(batch, scratch) {
		if len(*batch) >= p.cfg.Pipeline.BatchSize {
			p.flushBatch(batch)
		}
		return true
	}
	return false
}

// idleSleep performs a configurable idle sleep when no work is available.
func (p *StreamProcessor) idleSleep() {
	s := p.cfg.Pipeline.IdlePollSleep
	if s <= 0 {
		s = time.Millisecond
	}
	time.Sleep(s)
}

// cleanupIdleConsumers removes idle consumers from the consumer group
func (p *StreamProcessor) cleanupIdleConsumers() {
	if !p.cfg.Redis.ConsumerCleanupEnabled {
		return
	}

	ticker := time.NewTicker(p.cfg.Redis.ConsumerCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.performConsumerCleanup()
		}
	}
}

// performConsumerCleanup performs the actual consumer cleanup
func (p *StreamProcessor) performConsumerCleanup() {
	// Get all consumers in the group
	consumers, err := p.redisClient.GetConsumers(
		p.ctx,
		p.cfg.Redis.StreamName,
		p.cfg.Redis.ConsumerGroup,
	)
	if err != nil {
		p.logger.Error("Failed to get consumers", ports.Field{Key: "error", Value: err})
		return
	}

	currentConsumer := p.redisClient.GetConsumerName()

	for _, consumer := range consumers {
		// Don't remove ourselves
		if consumer.Name == currentConsumer {
			continue
		}

		// Check if consumer is idle
		if consumer.Idle > p.cfg.Redis.ConsumerIdleTimeout && consumer.Pending == 0 {
			err := p.redisClient.RemoveConsumer(
				p.ctx,
				p.cfg.Redis.StreamName,
				p.cfg.Redis.ConsumerGroup,
				consumer.Name,
			)
			if err != nil {
				p.logger.Error("Failed to remove idle consumer",
					ports.Field{Key: "consumer", Value: consumer.Name},
					ports.Field{Key: "error", Value: err},
				)
			} else {
				p.logger.Info("Removed idle consumer",
					ports.Field{Key: "consumer", Value: consumer.Name},
					ports.Field{Key: "idleTime", Value: consumer.Idle},
				)
			}
		}
	}
}
