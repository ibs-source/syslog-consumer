// Package hotpath coordinates the Redis to MQTT pipeline hot path.
package hotpath

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/ibs-source/syslog-consumer/internal/compress"
	"github.com/ubyte-source/go-jsonfast"

	"github.com/ibs-source/syslog-consumer/internal/config"
	"github.com/ibs-source/syslog-consumer/internal/log"
	"github.com/ibs-source/syslog-consumer/internal/message"
	"github.com/ibs-source/syslog-consumer/internal/metrics"
	"github.com/ibs-source/syslog-consumer/internal/mqtt"
	"github.com/ibs-source/syslog-consumer/internal/redis"
)

// HotPath orchestrates the Redis→MQTT pipeline
type HotPath struct {
	redis               redis.StreamClient
	mqtt                mqtt.Publisher
	lifecycleCtx        context.Context
	lifecycleCancel     context.CancelFunc
	msgChan             chan message.Batch
	claimTicker         *time.Ticker
	cleanupTicker       *time.Ticker
	refreshTicker       *time.Ticker
	log                 *log.Logger
	ackChans            []chan message.AckMessage // sharded by stream hash for batching
	ackWg               sync.WaitGroup
	consumerIdleTimeout time.Duration
	errorBackoff        time.Duration
	ackTimeout          time.Duration
	ackFlushInterval    time.Duration
	publishWorkers      int
	ackWorkers          int
	ackBatchSize        int
	singleStream        bool
}

// validateNewInputs checks all preconditions for New().
func validateNewInputs(
	redisClient redis.StreamClient,
	mqttPublisher mqtt.Publisher,
	cfg *config.Config,
	logger *log.Logger,
) error {
	if redisClient == nil {
		return errors.New("hotpath: redis client must not be nil")
	}
	if mqttPublisher == nil {
		return errors.New("hotpath: mqtt publisher must not be nil")
	}
	if cfg == nil {
		return errors.New("hotpath: config must not be nil")
	}
	if logger == nil {
		return errors.New("hotpath: logger must not be nil")
	}
	if cfg.Pipeline.PublishWorkers < 1 {
		return errors.New("hotpath: pipeline publish workers must be positive")
	}
	if cfg.Pipeline.AckWorkers < 1 {
		return errors.New("hotpath: pipeline ack workers must be positive")
	}
	if cfg.Pipeline.MessageQueueCapacity < 1 {
		return errors.New("hotpath: pipeline message queue capacity must be positive")
	}
	return nil
}

// New creates a new hot path orchestrator.
// mqttPublisher can be either *mqtt.Client or *mqtt.Pool.
func New(
	redisClient redis.StreamClient,
	mqttPublisher mqtt.Publisher,
	cfg *config.Config,
	logger *log.Logger,
) (*HotPath, error) {
	if err := validateNewInputs(redisClient, mqttPublisher, cfg, logger); err != nil {
		return nil, err
	}

	lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())

	singleStream := cfg.Redis.Stream != ""

	// Only create refreshTicker in multi-stream mode to avoid
	// a ticker firing uselessly in single-stream deployments.
	var refreshTicker *time.Ticker
	if !singleStream {
		refreshTicker = time.NewTicker(cfg.Pipeline.RefreshInterval)
	}

	// Per-worker ACK channels: each worker owns its channel, sharded by
	// stream name hash to concentrate ACKs for the same stream in the same
	// worker, maximizing batch sizes per flush.
	ackChans := make([]chan message.AckMessage, cfg.Pipeline.AckWorkers)
	chanCap := cfg.Pipeline.BufferCapacity / cfg.Pipeline.AckWorkers
	if chanCap < 64 {
		chanCap = 64
	}
	for i := range ackChans {
		ackChans[i] = make(chan message.AckMessage, chanCap)
	}

	return &HotPath{
		redis:               redisClient,
		mqtt:                mqttPublisher,
		msgChan:             make(chan message.Batch, cfg.Pipeline.MessageQueueCapacity),
		ackChans:            ackChans,
		lifecycleCtx:        lifecycleCtx,
		lifecycleCancel:     lifecycleCancel,
		claimTicker:         time.NewTicker(cfg.Redis.ClaimIdle),
		cleanupTicker:       time.NewTicker(cfg.Redis.CleanupInterval),
		refreshTicker:       refreshTicker,
		consumerIdleTimeout: cfg.Redis.ConsumerIdleTimeout,
		errorBackoff:        cfg.Pipeline.ErrorBackoff,
		ackTimeout:          cfg.Pipeline.AckTimeout,
		ackFlushInterval:    cfg.Pipeline.AckFlushInterval,
		ackBatchSize:        cfg.Pipeline.AckBatchSize,
		publishWorkers:      cfg.Pipeline.PublishWorkers,
		ackWorkers:          cfg.Pipeline.AckWorkers,
		singleStream:        singleStream,
		log:                 logger,
	}, nil
}

// startLoop starts a loop goroutine and reports non-canceled errors
func (hp *HotPath) startLoop(
	ctx context.Context,
	wg *sync.WaitGroup,
	name string,
	loop func(context.Context) error,
	errCh chan<- error,
) {
	wg.Go(func() {
		if err := loop(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- fmt.Errorf("%s loop error: %w", name, err)
		}
	})
}

// Run starts the hot path main loop
func (hp *HotPath) Run(ctx context.Context) error {
	hp.log.Info("Starting hot path orchestrator")

	// Subscribe to ACK messages
	if err := hp.mqtt.SubscribeAck(hp.handleAck); err != nil {
		return fmt.Errorf("failed to subscribe to ACK topic: %w", err)
	}

	// Start persistent ACK worker pool — one worker per shard channel.
	hp.log.Info("Starting %d ACK workers", hp.ackWorkers)
	for i := range hp.ackWorkers {
		ch := hp.ackChans[i]
		hp.ackWg.Go(func() { hp.ackWorker(ch) })
	}

	// Start processing goroutines
	var wg sync.WaitGroup
	// Buffer size for error channel to accommodate all loops
	numLoops := 4 + hp.publishWorkers // fetch, claim, cleanup, refresh + publish workers
	errCh := make(chan error, numLoops)

	hp.startLoop(ctx, &wg, "fetch", hp.fetchLoop, errCh)
	hp.startLoop(ctx, &wg, "claim", hp.claimLoop, errCh)
	hp.startLoop(ctx, &wg, "cleanup", hp.cleanupLoop, errCh)

	// Only start refresh loop in multi-stream mode (A7)
	if !hp.singleStream {
		hp.startLoop(ctx, &wg, "refresh", hp.refreshLoop, errCh)
	}

	// Start multiple concurrent publish workers for high throughput
	hp.log.Info("Starting %d publish workers", hp.publishWorkers)
	for i := range hp.publishWorkers {
		hp.startLoop(ctx, &wg, "publish-"+strconv.Itoa(i), hp.makePublishLoop(i), errCh)
	}

	// Wait for context cancellation or error
	shutdown := func() {
		hp.claimTicker.Stop()
		hp.cleanupTicker.Stop()
		if hp.refreshTicker != nil {
			hp.refreshTicker.Stop()
		}
		// DO NOT close(hp.msgChan) here.
		// Publish workers exit via ctx.Done() in their select.
		// fetch/claim loops also exit via ctx.Done().
		// After wg.Wait(), all goroutines have exited, so close is safe.
		wg.Wait()
		close(hp.msgChan) // Safe: no goroutine can write after wg.Wait() completes
		for _, ch := range hp.ackChans {
			close(ch) // Stop ACK workers
		}
		hp.ackWg.Wait() // Wait for ACK workers to drain
	}

	select {
	case <-ctx.Done():
		hp.log.Info("Shutting down hot path orchestrator")
		shutdown()
		return ctx.Err()
	case err := <-errCh:
		hp.log.Error("Hot path error: %v", err)
		shutdown()
		return err
	}
}

// fetchLoop continuously fetches batches from Redis
func (hp *HotPath) fetchLoop(ctx context.Context) error {
	// Reusable backoff timer — avoids allocation on every error.
	backoffTimer := time.NewTimer(hp.errorBackoff)
	backoffTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Fetch batch from Redis
		batch, err := hp.redis.ReadBatch(ctx)
		if err != nil {
			hp.log.Error("Failed to read batch from Redis: %v", err)
			metrics.FetchErrors.Add(1)
			// Context-aware backoff: exits immediately on shutdown.
			backoffTimer.Reset(hp.errorBackoff)
			select {
			case <-ctx.Done():
				backoffTimer.Stop()
				return ctx.Err()
			case <-backoffTimer.C:
			}
			continue
		}

		if len(batch.Items) == 0 {
			continue
		}

		if hp.log.DebugEnabled() {
			hp.log.Debug("Fetched %d messages from Redis", len(batch.Items))
		}
		metrics.MessagesFetched.Add(int64(len(batch.Items)))

		if err := hp.enqueueBatch(ctx, batch); err != nil {
			return err
		}
	}
}

// enqueueBatch sends a batch to the message channel,
// returning immediately if the context is canceled.
func (hp *HotPath) enqueueBatch(ctx context.Context, batch message.Batch) error {
	// Fast path: non-blocking send.
	select {
	case hp.msgChan <- batch:
		return nil
	default:
	}
	// Channel full — publish workers are slower than fetch. Track it.
	metrics.FetchBackpressure.Add(1)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case hp.msgChan <- batch:
	}
	return nil
}

// hintedPublisher is an optional extension that avoids shared-atomic
// contention by letting each worker supply its own routing hint.
type hintedPublisher interface {
	PublishFrom(ctx context.Context, payload message.Payload, hint uint64) error
}

// makePublishLoop returns a publishLoop closure bound to the given worker
// index. If the MQTT publisher supports PublishFrom, the closure uses a
// per-worker counter (zero contention); otherwise it falls back to Publish.
func (hp *HotPath) makePublishLoop(workerIdx int) func(context.Context) error {
	builder := jsonfast.New(4096)
	enc := compress.NewEncoder()
	bw := jsonfast.NewBatchWriter(4096)
	var compressed []byte

	// Resolve once at init: avoids repeated type assertion per batch.
	hinted, ok := hp.mqtt.(hintedPublisher)
	hint := uint64(max(workerIdx, 0))           // max elides gosec G115; workerIdx is always non-negative
	stride := uint64(max(hp.publishWorkers, 1)) // max elides gosec G115; publishWorkers is validated > 0

	publishFn := func(ctx context.Context, payload message.Payload) error {
		if ok {
			h := hint
			hint += stride
			return hinted.PublishFrom(ctx, payload, h)
		}
		return hp.mqtt.Publish(ctx, payload)
	}

	return func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				for {
					select {
					case batch := <-hp.msgChan:
						hp.publishBatch(builder, enc, batch.Items, bw, &compressed, publishFn)
						batch.Release()
					default:
						return ctx.Err()
					}
				}
			case batch := <-hp.msgChan:
				hp.publishBatch(builder, enc, batch.Items, bw, &compressed, publishFn)
				batch.Release()
			}
		}
	}
}

// publishBatch builds, compresses, and publishes a single NDJSON batch.
func (hp *HotPath) publishBatch(
	builder *jsonfast.Builder, enc *zstd.Encoder,
	batch []message.Redis, bw *jsonfast.BatchWriter, compressed *[]byte,
	publishFn func(context.Context, message.Payload) error,
) {
	bw.Reset()

	for i := range batch {
		msg := &batch[i]
		if msg.Object == "" && msg.Raw == "" {
			hp.log.Warn("Skipping message %s with empty body", msg.ID)
			continue
		}
		bw.Append(hp.buildPayload(builder, msg))
	}

	if bw.Count() == 0 {
		return
	}

	// Compress the NDJSON batch with zstd using the per-worker encoder.
	*compressed = compress.EncodeWith(enc, *compressed, bw.Bytes())

	if err := publishFn(hp.lifecycleCtx, *compressed); err != nil {
		hp.log.Error("Failed to publish batch of %d messages: %v",
			bw.Count(), err)
		metrics.PublishErrors.Add(int64(bw.Count()))
		return
	}

	if hp.log.DebugEnabled() {
		hp.log.Debug("Published compressed batch: %d messages, %d→%d bytes",
			bw.Count(), bw.Len(), len(*compressed))
	}
	metrics.MessagesPublished.Add(int64(bw.Count()))
}

// Pre-allocated key constants for bytes.Equal comparison,
// eliminating the string(key[1:len(key)-1]) allocation per field.
var (
	keyStructuredData = []byte("structured_data")
	keySeverity       = []byte("severity")
)

// Pre-computed FieldKey constants: eliminate per-call quoting overhead
// for fields written on every message.
var (
	fkSeverity = jsonfast.NewFieldKey("severity")
	fkRaw      = jsonfast.NewFieldKey("raw")
)

// buildPayload produces "id\tstream\t{event JSON}" by flattening
// structured_data, mapping severity→name, and passing through the rest.
// The returned slice is valid until the next call on the same builder.
func (hp *HotPath) buildPayload(builder *jsonfast.Builder, msg *message.Redis) []byte {
	builder.Reset()

	builder.AppendRawString(msg.ID)
	builder.AppendRawString("\t")
	builder.AppendRawString(msg.Stream)
	builder.AppendRawString("\t")

	builder.BeginObject()

	if msg.Object != "" {
		jsonfast.IterateFieldsString(msg.Object, func(key, value []byte) bool {
			name := key[1 : len(key)-1]
			switch len(name) {
			case 15: // "structured_data"
				if bytes.Equal(name, keyStructuredData) {
					jsonfast.FlattenObject(builder, value)
					return true
				}
			case 8: // "severity"
				if bytes.Equal(name, keySeverity) {
					builder.AddStringFieldKey(fkSeverity, severityName(value))
					return true
				}
			}
			builder.AddRawBytesField(name, value)
			return true
		})
	}

	if msg.Raw == "" {
		builder.AddStringFieldKey(fkRaw, "-")
	} else {
		builder.AddStringFieldKey(fkRaw, msg.Raw)
	}

	builder.EndObject()

	return builder.Bytes()
}

// claimLoop periodically claims idle messages from Redis
func (hp *HotPath) claimLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-hp.claimTicker.C:
			batch, err := hp.redis.ClaimIdle(ctx)
			if err != nil {
				hp.log.Error("Failed to claim idle messages: %v", err)
				continue
			}

			if len(batch.Items) > 0 {
				hp.log.Info("Claimed %d idle messages", len(batch.Items))
				metrics.MessagesClaimed.Add(int64(len(batch.Items)))

				if err := hp.enqueueBatch(ctx, batch); err != nil {
					return err
				}
			}
		}
	}
}

// cleanupLoop periodically removes dead consumers from the consumer group
func (hp *HotPath) cleanupLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-hp.cleanupTicker.C:
			if err := hp.redis.CleanupDeadConsumers(ctx, hp.consumerIdleTimeout); err != nil {
				hp.log.Error("Failed to cleanup dead consumers: %v", err)
			}
		}
	}
}

// refreshLoop periodically refreshes the list of streams (multi-stream mode only)
func (hp *HotPath) refreshLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-hp.refreshTicker.C:
			newCount, err := hp.redis.RefreshStreams(ctx)
			if err != nil {
				hp.log.Error("Failed to refresh streams: %v", err)
				continue
			}
			if newCount > 0 {
				hp.log.Info("Stream refresh discovered %d new streams", newCount)
			}
		}
	}
}

// handleAck enqueues ACK messages for processing by the sharded worker pool.
// ACKs are routed to a worker based on stream name hash so that all ACKs for
// the same stream accumulate in the same worker, maximizing batch sizes.
func (hp *HotPath) handleAck(ack message.AckMessage) {
	idx := streamShard(ack.Stream, len(hp.ackChans))
	select {
	case hp.ackChans[idx] <- ack:
		metrics.AckQueueDepth.Add(1)
	case <-hp.lifecycleCtx.Done():
		// Shutdown in progress, drop the ACK.
		// The message will be reclaimed by the claim loop on next startup.
		if hp.log.DebugEnabled() {
			hp.log.Debug("Dropping ACK for %v during shutdown", ack.IDs)
		}
	}
}

// streamShard returns a deterministic shard index for a stream name.
// Uses FNV-1a-inspired byte mixing — cheap, no imports, good distribution.
func streamShard(stream string, shards int) int {
	h := uint32(2166136261) // FNV offset basis
	for i := range len(stream) {
		h ^= uint32(stream[i])
		h *= 16777619 // FNV prime
	}
	return int(h) % shards
}

// ackWorker batches ACK messages per stream and flushes on timer or
// threshold. Each worker owns a sharded channel so same-stream ACKs
// coalesce here.
func (hp *HotPath) ackWorker(ch <-chan message.AckMessage) {
	if hp.singleStream {
		hp.ackWorkerSingle(ch)
		return
	}

	pending := make(map[string]*pendingACK, 4)
	timer := time.NewTimer(hp.ackFlushInterval)
	timer.Stop()
	armed := false

	flush := func() {
		for stream, p := range pending {
			hp.flushACKs(stream, p)
			delete(pending, stream)
			putPendingACK(p)
		}
		armed = false
	}

	for {
		select {
		case ack, ok := <-ch:
			if !ok {
				flush()
				timer.Stop()
				return
			}

			if hp.accumulateACK(pending, ack) {
				flush()
				continue
			}

			if !armed {
				timer.Reset(hp.ackFlushInterval)
				armed = true
			}

		case <-timer.C:
			flush()
		}
	}
}

// ackWorkerSingle is the single-stream fast path for ackWorker.
// It avoids map allocation and lookup entirely by keeping a single
// pendingACK and stream name.
func (hp *HotPath) ackWorkerSingle(ch <-chan message.AckMessage) {
	p := getPendingACK()
	var stream string
	timer := time.NewTimer(hp.ackFlushInterval)
	timer.Stop()
	armed := false

	flush := func() {
		if stream != "" {
			hp.flushACKs(stream, p)
			p.ackIDs = p.ackIDs[:0]
			p.nackCount = 0
		}
		armed = false
	}

	for {
		select {
		case ack, ok := <-ch:
			if !ok {
				flush()
				timer.Stop()
				putPendingACK(p)
				return
			}

			metrics.AckQueueDepth.Add(-1)
			stream = ack.Stream
			if ack.Ack {
				p.ackIDs = append(p.ackIDs, ack.IDs...)
			} else {
				p.nackCount += len(ack.IDs)
			}

			if len(p.ackIDs)+p.nackCount >= hp.ackBatchSize {
				flush()
				continue
			}

			if !armed {
				timer.Reset(hp.ackFlushInterval)
				armed = true
			}

		case <-timer.C:
			flush()
		}
	}
}

// accumulateACK adds an ACK message to the pending map and reports whether
// the batch for that stream has reached the flush threshold.
func (hp *HotPath) accumulateACK(pending map[string]*pendingACK, ack message.AckMessage) bool {
	metrics.AckQueueDepth.Add(-1)

	p := pending[ack.Stream]
	if p == nil {
		p = getPendingACK()
		pending[ack.Stream] = p
	}

	if ack.Ack {
		p.ackIDs = append(p.ackIDs, ack.IDs...)
	} else {
		p.nackCount += len(ack.IDs)
	}

	return len(p.ackIDs)+p.nackCount >= hp.ackBatchSize
}

// pendingACK accumulates ACK and NACK IDs for a single stream.
type pendingACK struct {
	ackIDs    []string
	nackCount int
}

var pendingACKPool = sync.Pool{
	New: func() any { return &pendingACK{} },
}

func getPendingACK() *pendingACK {
	p, ok := pendingACKPool.Get().(*pendingACK)
	if !ok {
		return &pendingACK{}
	}
	return p
}

func putPendingACK(p *pendingACK) {
	p.ackIDs = p.ackIDs[:0]
	p.nackCount = 0
	pendingACKPool.Put(p)
}

// flushACKs sends a single batched AckAndDeleteBatch for all accumulated IDs.
func (hp *HotPath) flushACKs(stream string, p *pendingACK) {
	if len(p.ackIDs) > 0 {
		ctx, cancel := context.WithTimeout(hp.lifecycleCtx, hp.ackTimeout)
		err := hp.redis.AckAndDeleteBatch(ctx, p.ackIDs, stream)
		cancel()

		if err != nil {
			hp.log.Error("Failed to ACK %d messages from stream %s: %v", len(p.ackIDs), stream, err)
			metrics.AckErrors.Add(1)
		} else {
			if hp.log.DebugEnabled() {
				hp.log.Debug("ACKed %d messages from stream %s", len(p.ackIDs), stream)
			}
			metrics.MessagesAcked.Add(int64(len(p.ackIDs)))
		}
	}

	if p.nackCount > 0 {
		metrics.MessagesNacked.Add(int64(p.nackCount))
		// Log only when enabled — avoids int/string boxing on the hot path.
		if hp.log.InfoEnabled() {
			hp.log.Info("%d messages from stream %s failed, will be reclaimed", p.nackCount, stream)
		}
	}
}

// Close cancels in-flight ACK goroutines and stops tickers. Safe to
// call even if Run never completed; ticker.Stop is idempotent.
func (hp *HotPath) Close() error {
	hp.lifecycleCancel()
	hp.claimTicker.Stop()
	hp.cleanupTicker.Stop()
	if hp.refreshTicker != nil {
		hp.refreshTicker.Stop()
	}
	return nil
}
