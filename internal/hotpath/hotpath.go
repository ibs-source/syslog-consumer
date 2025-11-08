// Package hotpath coordinates the Redis to MQTT pipeline hot path.
package hotpath

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ibs-source/syslog-consumer/internal/config"
	"github.com/ibs-source/syslog-consumer/internal/log"
	"github.com/ibs-source/syslog-consumer/internal/message"
	"github.com/ibs-source/syslog-consumer/internal/mqtt"
	"github.com/ibs-source/syslog-consumer/internal/redis"
)

// HotPath orchestrates the Redis→MQTT pipeline
type HotPath struct {
	redis               *redis.Client
	mqtt                mqtt.Publisher
	msgChan             chan message.Redis[message.Payload]
	claimTicker         *time.Ticker
	cleanupTicker       *time.Ticker
	refreshTicker       *time.Ticker
	consumerIdleTimeout time.Duration
	errorBackoff        time.Duration
	ackTimeout          time.Duration
	publishWorkers      int
	log                 *log.Logger
}

// New creates a new hot path orchestrator
// mqttPublisher can be either *mqtt.Client or *mqtt.Pool
func New(redisClient *redis.Client, mqttPublisher mqtt.Publisher, cfg *config.Config, logger *log.Logger) *HotPath {
	return &HotPath{
		redis:               redisClient,
		mqtt:                mqttPublisher,
		msgChan:             make(chan message.Redis[message.Payload], cfg.Pipeline.BufferCapacity),
		claimTicker:         time.NewTicker(cfg.Redis.ClaimIdle),
		cleanupTicker:       time.NewTicker(cfg.Redis.CleanupInterval),
		refreshTicker:       time.NewTicker(cfg.Redis.CleanupInterval), // Reuse cleanup interval for stream refresh
		consumerIdleTimeout: cfg.Redis.ConsumerIdleTimeout,
		errorBackoff:        cfg.Pipeline.ErrorBackoff,
		ackTimeout:          cfg.Pipeline.AckTimeout,
		publishWorkers:      cfg.Pipeline.PublishWorkers,
		log:                 logger,
	}
}

// startLoop starts a loop goroutine and reports non-canceled errors
func (hp *HotPath) startLoop(
	ctx context.Context,
	wg *sync.WaitGroup,
	name string,
	loop func(context.Context) error,
	errCh chan<- error,
) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := loop(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- fmt.Errorf("%s loop error: %w", name, err)
		}
	}()
}

// Run starts the hot path main loop
func (hp *HotPath) Run(ctx context.Context) error {
	hp.log.Info("Starting hot path orchestrator")

	// Subscribe to ACK messages
	if err := hp.mqtt.SubscribeAck(hp.handleAck); err != nil {
		return fmt.Errorf("failed to subscribe to ACK topic: %w", err)
	}

	// Start processing goroutines
	var wg sync.WaitGroup
	// Buffer size for error channel to accommodate all loops
	numLoops := 4 + hp.publishWorkers // fetch, claim, cleanup, refresh + publish workers
	errCh := make(chan error, numLoops)

	hp.startLoop(ctx, &wg, "fetch", hp.fetchLoop, errCh)
	hp.startLoop(ctx, &wg, "claim", hp.claimLoop, errCh)
	hp.startLoop(ctx, &wg, "cleanup", hp.cleanupLoop, errCh)
	hp.startLoop(ctx, &wg, "refresh", hp.refreshLoop, errCh)

	// Start multiple concurrent publish workers for high throughput
	publishWorkers := hp.getPublishWorkers()
	hp.log.Info("Starting %d publish workers", publishWorkers)
	for i := 0; i < publishWorkers; i++ {
		hp.startLoop(ctx, &wg, fmt.Sprintf("publish-%d", i), hp.publishLoop, errCh)
	}

	// Wait for context cancellation or error
	select {
	case <-ctx.Done():
		hp.log.Info("Shutting down hot path orchestrator")
		hp.claimTicker.Stop()
		hp.cleanupTicker.Stop()
		hp.refreshTicker.Stop()
		close(hp.msgChan)
		wg.Wait()
		return ctx.Err()
	case err := <-errCh:
		hp.log.Error("Hot path error: %v", err)
		hp.claimTicker.Stop()
		hp.cleanupTicker.Stop()
		hp.refreshTicker.Stop()
		close(hp.msgChan)
		wg.Wait()
		return err
	}
}

// getPublishWorkers returns the configured number of publish workers
func (hp *HotPath) getPublishWorkers() int {
	return hp.publishWorkers
}

// fetchLoop continuously fetches batches from Redis
func (hp *HotPath) fetchLoop(ctx context.Context) error {
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
			time.Sleep(hp.errorBackoff) // Brief backoff on error
			continue
		}

		if len(batch.Items) == 0 {
			continue
		}

		hp.log.Debug("Fetched %d messages from Redis", len(batch.Items))

		// Enqueue messages to channel (thread-safe for multiple consumers)
		for i := range batch.Items {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case hp.msgChan <- batch.Items[i]:
				// Message enqueued successfully
			}
		}
	}
}

// publishLoop continuously publishes messages from channel to MQTT
func (hp *HotPath) publishLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-hp.msgChan:
			if !ok {
				// Channel closed, shutdown
				return nil
			}

			// Validate message body before processing
			if len(msg.Body) == 0 {
				hp.log.Warn("Skipping message %s with empty body", msg.ID)
				continue
			}

			// Build self-contained payload with stream info and ack:true preset
			payload := hp.buildPayload(msg.ID, msg.Stream, msg.Body)

			// Publish to MQTT
			if err := hp.mqtt.Publish(ctx, payload); err != nil {
				hp.log.Error("Failed to publish message %s: %v", msg.ID, err)
				// Don't retry here - let claim loop handle it
				continue
			}

			// Message published successfully
			// ACK handler will receive ACK and do XACK+XDEL independently
			hp.log.Debug("Published message %s to MQTT", msg.ID)
		}
	}
}

// buildPayload constructs self-contained message with stream info and ack:true preset
func (hp *HotPath) buildPayload(id string, stream string, data []byte) []byte {
	// Format: {"message":{"payload":<data>},"redis":{"payload":{"id":"<id>","stream":"<stream>","ack":true}}}
	// The ack:true is preset - remote system only needs to change to false on failure
	// Approximate size: len(data) + len(stream) + 100 bytes overhead
	result := make([]byte, 0, len(data)+len(stream)+100)
	result = append(result, `{"message":{"payload":`...)
	result = append(result, data...)
	result = append(result, `},"redis":{"payload":{"id":"`...)
	result = append(result, id...)
	result = append(result, `","stream":"`...)
	result = append(result, stream...)
	result = append(result, `","ack":true}}}`...)
	return result
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

				// Enqueue claimed messages to channel
				for i := range batch.Items {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case hp.msgChan <- batch.Items[i]:
						// Message enqueued successfully
					}
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

// handleAck processes ACK messages from MQTT
// ACK message is self-contained with all necessary info (id, stream, ack)
func (hp *HotPath) handleAck(ack message.AckMessage) {
	ctx, cancel := context.WithTimeout(context.Background(), hp.ackTimeout)
	defer cancel()

	if ack.Ack {
		// Success - ACK and delete from Redis
		// Stream name comes directly from the ACK message (no cache needed)
		msg := message.Redis[message.Payload]{
			ID:     ack.ID,
			Stream: ack.Stream,
		}

		if err := hp.redis.AckAndDelete(ctx, msg); err != nil {
			hp.log.Error("Failed to ACK message %s from stream %s in Redis: %v", ack.ID, ack.Stream, err)
			// Message stays in pending list, claim loop will recover it
		} else {
			hp.log.Debug("Successfully ACKed message %s from stream %s", ack.ID, ack.Stream)
		}
	} else {
		// Failure - don't ACK, let it become idle and get claimed for retry
		hp.log.Info("Message %s from stream %s failed processing, will be reclaimed", ack.ID, ack.Stream)
		// Claim loop will pick it up after ClaimIdle timeout
	}
}

// Close cleans up resources
func (hp *HotPath) Close() error {
	hp.claimTicker.Stop()
	hp.cleanupTicker.Stop()
	hp.refreshTicker.Stop()
	return nil
}
