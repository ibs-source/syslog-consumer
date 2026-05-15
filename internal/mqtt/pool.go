package mqtt

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"github.com/ibs-source/syslog-consumer/internal/config"
	"github.com/ibs-source/syslog-consumer/internal/log"
	"github.com/ibs-source/syslog-consumer/internal/message"
)

// Pool fans out publishes across several paho connections to raise broker
// throughput beyond what one TCP connection can sustain.
type Pool struct {
	log     *log.Logger
	clients []*Client
	next    atomic.Uint64
	size    uint
}

func closeClients(ctx context.Context, logger *log.Logger, clients []*Client, count int) {
	safe := clients[:min(count, len(clients))]
	for j, c := range safe {
		if c == nil {
			continue
		}
		if err := c.Close(); err != nil {
			logger.Warnf(ctx, "Error closing client %d during cleanup: %v", j, err)
		}
	}
}

// NewPool retries each connection until the broker responds or ctx is canceled.
func NewPool(ctx context.Context, cfg *config.MQTTConfig, poolSize int, logger *log.Logger) (*Pool, error) {
	if poolSize < 1 {
		return nil, errors.New("mqtt: pool size must be positive")
	}

	// Per-process suffix prevents Client ID collisions across instances.
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	pid := os.Getpid()
	baseClientID := fmt.Sprintf("%s-%s-%d", cfg.ClientID, hostname, pid)

	clients := make([]*Client, poolSize)

	g, gctx := errgroup.WithContext(ctx)
	for i := range poolSize {
		clientCfg := *cfg
		clientCfg.ClientID = fmt.Sprintf("%s-%d", baseClientID, i)

		client, err := NewClient(ctx, &clientCfg, logger)
		if err != nil {
			closeClients(ctx, logger, clients, poolSize)
			return nil, fmt.Errorf("failed to create client %d: %w", i, err)
		}
		clients[i] = client

		g.Go(func() error {
			if err := client.Connect(gctx); err != nil {
				return fmt.Errorf("failed to connect client %d: %w", i, err)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		closeClients(ctx, logger, clients, poolSize)
		return nil, err
	}

	return &Pool{
		clients: clients,
		size:    uint(poolSize),
		log:     logger,
	}, nil
}

// Publish skips disconnected clients and tries all pool members before failing.
func (p *Pool) Publish(ctx context.Context, payload message.Payload) error {
	start := p.next.Add(1) - 1
	sz := uint64(p.size)
	for i := range p.size {
		c := p.clients[(start+uint64(i))%sz]
		if !c.IsConnected() {
			continue
		}
		return c.Publish(ctx, payload)
	}
	return errNotConnected
}

// PublishFrom takes the round-robin hint from the caller to avoid contention
// on the shared atomic counter.
func (p *Pool) PublishFrom(ctx context.Context, payload message.Payload, hint uint64) error {
	sz := uint64(p.size)
	for i := range p.size {
		c := p.clients[(hint+uint64(i))%sz]
		if !c.IsConnected() {
			continue
		}
		return c.Publish(ctx, payload)
	}
	return errNotConnected
}

// SubscribeAck subscribes on every client because the broker may deliver
// ACK responses on any connection. The handler must be idempotent.
func (p *Pool) SubscribeAck(ctx context.Context, handler func(message.AckMessage)) error {
	for i, c := range p.clients {
		if err := c.SubscribeAck(ctx, handler); err != nil {
			return fmt.Errorf("failed to subscribe ACK on client %d: %w", i, err)
		}
	}
	return nil
}

// Close disconnects every pool member; returned errors are joined.
func (p *Pool) Close() error {
	var errs []error
	for i, client := range p.clients {
		if err := client.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close client %d: %w", i, err))
		}
	}
	return errors.Join(errs...)
}

// IsConnected reports whether at least one pool connection is open.
func (p *Pool) IsConnected() bool {
	for _, c := range p.clients {
		if c.IsConnected() {
			return true
		}
	}
	return false
}
