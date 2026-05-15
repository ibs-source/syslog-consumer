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

// HotPath orchestrates the Redis → MQTT pipeline: fetch, publish, ACK, and
// the maintenance loops (claim, cleanup, refresh).
type HotPath struct {
	redis               redis.StreamClient
	mqtt                mqtt.Publisher
	done                chan struct{}
	msgChan             chan message.Batch
	claimTicker         *time.Ticker
	cleanupTicker       *time.Ticker
	refreshTicker       *time.Ticker
	log                 *log.Logger
	ackChans            []chan message.AckMessage
	closeOnce           sync.Once
	singleStream        bool
	ackWg               sync.WaitGroup
	consumerIdleTimeout time.Duration
	errorBackoff        time.Duration
	ackTimeout          time.Duration
	ackFlushInterval    time.Duration
	publishWorkers      int
	ackWorkers          int
	ackBatchSize        int
}

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

// New accepts either *mqtt.Client or *mqtt.Pool as mqttPublisher.
func New(
	redisClient redis.StreamClient,
	mqttPublisher mqtt.Publisher,
	cfg *config.Config,
	logger *log.Logger,
) (*HotPath, error) {
	if err := validateNewInputs(redisClient, mqttPublisher, cfg, logger); err != nil {
		return nil, err
	}

	singleStream := cfg.Redis.Stream != ""

	var refreshTicker *time.Ticker
	if !singleStream {
		refreshTicker = time.NewTicker(cfg.Pipeline.RefreshInterval)
	}

	// ACK channels are sharded by stream-name hash so same-stream ACKs land
	// on the same worker, maximizing per-flush batch sizes.
	ackChans := make([]chan message.AckMessage, cfg.Pipeline.AckWorkers)
	chanCap := max(cfg.Pipeline.BufferCapacity/cfg.Pipeline.AckWorkers, 64)
	for i := range ackChans {
		ackChans[i] = make(chan message.AckMessage, chanCap)
	}

	return &HotPath{
		redis:               redisClient,
		mqtt:                mqttPublisher,
		msgChan:             make(chan message.Batch, cfg.Pipeline.MessageQueueCapacity),
		ackChans:            ackChans,
		done:                make(chan struct{}),
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

// Run blocks until ctx is canceled or a loop returns a fatal error. It
// returns ctx.Err() on graceful shutdown.
func (hp *HotPath) Run(ctx context.Context) error {
	hp.log.Infof(ctx, "Starting hot path orchestrator")

	// lifeCtx outlives ctx so ACK callbacks and the drain phase can still
	// complete after the orchestrator's loop context is canceled.
	lifeCtx, lifeCancel := context.WithCancel(context.WithoutCancel(ctx))
	defer lifeCancel()
	go func() {
		select {
		case <-hp.done:
			lifeCancel()
		case <-lifeCtx.Done():
		}
	}()

	if err := hp.mqtt.SubscribeAck(lifeCtx, hp.makeAckHandler(lifeCtx)); err != nil {
		return fmt.Errorf("failed to subscribe to ACK topic: %w", err)
	}

	hp.startAckWorkers(ctx, lifeCtx)

	wg, errCh := hp.startLoops(ctx, lifeCtx)

	select {
	case <-ctx.Done():
		hp.log.Infof(ctx, "Shutting down hot path orchestrator")
		hp.shutdown(wg)
		return ctx.Err()
	case err := <-errCh:
		hp.log.Errorf(ctx, "Hot path error: %v", err)
		hp.shutdown(wg)
		return err
	}
}

func (hp *HotPath) startAckWorkers(ctx, lifeCtx context.Context) {
	hp.log.Infof(ctx, "Starting %d ACK workers", hp.ackWorkers)
	for i := range hp.ackWorkers {
		ch := hp.ackChans[i]
		hp.ackWg.Go(func() { hp.ackWorker(lifeCtx, ch) })
	}
}

func (hp *HotPath) startLoops(ctx, lifeCtx context.Context) (wg *sync.WaitGroup, errCh <-chan error) {
	wg = &sync.WaitGroup{}
	numLoops := 4 + hp.publishWorkers
	ch := make(chan error, numLoops)

	hp.startLoop(ctx, wg, "fetch", hp.fetchLoop, ch)
	hp.startLoop(ctx, wg, "claim", hp.claimLoop, ch)
	hp.startLoop(ctx, wg, "cleanup", hp.cleanupLoop, ch)

	if !hp.singleStream {
		hp.startLoop(ctx, wg, "refresh", hp.refreshLoop, ch)
	}

	hp.log.Infof(ctx, "Starting %d publish workers", hp.publishWorkers)
	for i := range hp.publishWorkers {
		hp.startLoop(ctx, wg, "publish-"+strconv.Itoa(i), hp.makePublishLoop(lifeCtx, i), ch)
	}
	errCh = ch
	return wg, errCh
}

func (hp *HotPath) shutdown(wg *sync.WaitGroup) {
	hp.claimTicker.Stop()
	hp.cleanupTicker.Stop()
	if hp.refreshTicker != nil {
		hp.refreshTicker.Stop()
	}
	// wg.Wait() must precede the channel closes: workers may still send.
	wg.Wait()
	close(hp.msgChan)
	for _, ch := range hp.ackChans {
		close(ch)
	}
	hp.ackWg.Wait()
}

func (hp *HotPath) fetchLoop(ctx context.Context) error {
	backoffTimer := time.NewTimer(hp.errorBackoff)
	backoffTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		batch, err := hp.redis.ReadBatch(ctx)
		if err != nil {
			hp.log.Errorf(ctx, "Failed to read batch from Redis: %v", err)
			metrics.FetchErrors.Add(1)
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

		if hp.log.DebugEnabled(ctx) {
			hp.log.Debugf(ctx, "Fetched %d messages from Redis", len(batch.Items))
		}
		metrics.MessagesFetched.Add(int64(len(batch.Items)))

		if err := hp.enqueueBatch(ctx, batch); err != nil {
			return err
		}
	}
}

func (hp *HotPath) enqueueBatch(ctx context.Context, batch message.Batch) error {
	select {
	case hp.msgChan <- batch:
		return nil
	default:
	}
	metrics.FetchBackpressure.Add(1)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case hp.msgChan <- batch:
	}
	return nil
}

// hintedPublisher lets each worker supply a routing hint instead of contending
// on a shared atomic.
type hintedPublisher interface {
	PublishFrom(ctx context.Context, payload message.Payload, hint uint64) error
}

func (hp *HotPath) makePublishLoop(lifeCtx context.Context, workerIdx int) func(context.Context) error {
	builder := jsonfast.New(4096)
	enc := compress.NewEncoder()
	bw := jsonfast.NewBatchWriter(4096)
	var compressed []byte

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
						hp.publishBatch(lifeCtx, builder, enc, batch.Items, bw, &compressed, publishFn)
						batch.Release()
					default:
						return ctx.Err()
					}
				}
			case batch := <-hp.msgChan:
				hp.publishBatch(lifeCtx, builder, enc, batch.Items, bw, &compressed, publishFn)
				batch.Release()
			}
		}
	}
}

func (hp *HotPath) publishBatch(
	ctx context.Context,
	builder *jsonfast.Builder, enc *zstd.Encoder,
	batch []message.Redis, bw *jsonfast.BatchWriter, compressed *[]byte,
	publishFn func(context.Context, message.Payload) error,
) {
	bw.Reset()

	for i := range batch {
		msg := &batch[i]
		if msg.Object == "" && msg.Raw == "" {
			hp.log.Warnf(ctx, "Skipping message %s with empty body", msg.ID)
			continue
		}
		bw.Append(hp.buildPayload(builder, msg))
	}

	if bw.Count() == 0 {
		return
	}

	*compressed = compress.EncodeWith(enc, *compressed, bw.Bytes())

	if err := publishFn(ctx, *compressed); err != nil {
		hp.log.Errorf(ctx, "Failed to publish batch of %d messages: %v",
			bw.Count(), err)
		metrics.PublishErrors.Add(int64(bw.Count()))
		return
	}

	if hp.log.DebugEnabled(ctx) {
		hp.log.Debugf(ctx, "Published compressed batch: %d messages, %d→%d bytes",
			bw.Count(), bw.Len(), len(*compressed))
	}
	metrics.MessagesPublished.Add(int64(bw.Count()))
}

var (
	keyStructuredData = []byte("structured_data")
	keySeverity       = []byte("severity")
)

var (
	fkSeverity = jsonfast.NewFieldKey("severity")
	fkRaw      = jsonfast.NewFieldKey("raw")
)

// buildPayload returns a slice that is only valid until the next call on
// the same builder.
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
			case 15:
				if bytes.Equal(name, keyStructuredData) {
					jsonfast.FlattenObject(builder, value)
					return true
				}
			case 8:
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

func (hp *HotPath) claimLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-hp.claimTicker.C:
			batch, err := hp.redis.ClaimIdle(ctx)
			if err != nil {
				hp.log.Errorf(ctx, "Failed to claim idle messages: %v", err)
				continue
			}

			if len(batch.Items) > 0 {
				hp.log.Infof(ctx, "Claimed %d idle messages", len(batch.Items))
				metrics.MessagesClaimed.Add(int64(len(batch.Items)))

				if err := hp.enqueueBatch(ctx, batch); err != nil {
					return err
				}
			}
		}
	}
}

func (hp *HotPath) cleanupLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-hp.cleanupTicker.C:
			if err := hp.redis.CleanupDeadConsumers(ctx, hp.consumerIdleTimeout); err != nil {
				hp.log.Errorf(ctx, "Failed to cleanup dead consumers: %v", err)
			}
		}
	}
}

func (hp *HotPath) refreshLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-hp.refreshTicker.C:
			newCount, err := hp.redis.RefreshStreams(ctx)
			if err != nil {
				hp.log.Errorf(ctx, "Failed to refresh streams: %v", err)
				continue
			}
			if newCount > 0 {
				hp.log.Infof(ctx, "Stream refresh discovered %d new streams", newCount)
			}
		}
	}
}

// makeAckHandler routes ACKs to a worker by stream-name hash so that
// same-stream ACKs coalesce into the same flush batch. Dropped ACKs are
// safe: the claim loop reclaims them on the next start.
func (hp *HotPath) makeAckHandler(lifeCtx context.Context) func(message.AckMessage) {
	return func(ack message.AckMessage) {
		idx := streamShard(ack.Stream, len(hp.ackChans))
		select {
		case hp.ackChans[idx] <- ack:
			metrics.AckQueueDepth.Add(1)
		case <-lifeCtx.Done():
			if hp.log.DebugEnabled(lifeCtx) {
				hp.log.Debugf(lifeCtx, "Dropping ACK for %v during shutdown", ack.IDs)
			}
		}
	}
}

func streamShard(stream string, shards int) int {
	h := uint32(2166136261)
	for i := range len(stream) {
		h ^= uint32(stream[i])
		h *= 16777619
	}
	return int(h) % shards
}

func (hp *HotPath) ackWorker(ctx context.Context, ch <-chan message.AckMessage) {
	if hp.singleStream {
		hp.ackWorkerSingle(ctx, ch)
		return
	}

	pending := make(map[string]*pendingACK, 4)
	timer := time.NewTimer(hp.ackFlushInterval)
	timer.Stop()
	armed := false

	flush := func() {
		for stream, p := range pending {
			hp.flushACKs(ctx, stream, p)
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

func (hp *HotPath) ackWorkerSingle(ctx context.Context, ch <-chan message.AckMessage) {
	p := getPendingACK()
	var stream string
	timer := time.NewTimer(hp.ackFlushInterval)
	timer.Stop()
	armed := false

	flush := func() {
		if stream != "" {
			hp.flushACKs(ctx, stream, p)
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

func (hp *HotPath) flushACKs(parentCtx context.Context, stream string, p *pendingACK) {
	if len(p.ackIDs) > 0 {
		ctx, cancel := context.WithTimeout(parentCtx, hp.ackTimeout)
		err := hp.redis.AckAndDeleteBatch(ctx, p.ackIDs, stream)
		cancel()

		if err != nil {
			hp.log.Errorf(parentCtx, "Failed to ACK %d messages from stream %s: %v", len(p.ackIDs), stream, err)
			metrics.AckErrors.Add(1)
		} else {
			if hp.log.DebugEnabled(parentCtx) {
				hp.log.Debugf(parentCtx, "ACKed %d messages from stream %s", len(p.ackIDs), stream)
			}
			metrics.MessagesAcked.Add(int64(len(p.ackIDs)))
		}
	}

	if p.nackCount > 0 {
		metrics.MessagesNacked.Add(int64(p.nackCount))
		if hp.log.InfoEnabled(parentCtx) {
			hp.log.Infof(parentCtx, "%d messages from stream %s failed, will be reclaimed", p.nackCount, stream)
		}
	}
}

// Close is idempotent and safe to call even if Run never started.
func (hp *HotPath) Close() error {
	hp.closeOnce.Do(func() {
		close(hp.done)
	})
	hp.claimTicker.Stop()
	hp.cleanupTicker.Stop()
	if hp.refreshTicker != nil {
		hp.refreshTicker.Stop()
	}
	return nil
}
