package mqtt

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/ibs-source/syslog-consumer/internal/config"
	"github.com/ibs-source/syslog-consumer/internal/log"
	"github.com/ibs-source/syslog-consumer/internal/message"
)

// Pool manages multiple MQTT client connections for high throughput
type Pool struct {
	clients []*Client
	next    atomic.Uint64
	size    int
	log     *log.Logger
}

// NewPool creates a new MQTT connection pool
func NewPool(cfg *config.MQTTConfig, poolSize int, logger *log.Logger) (*Pool, error) {
	if poolSize < 1 {
		poolSize = 1
	}

	// Generate a unique base Client ID for this process instance
	// This prevents collisions when multiple consumer instances run with the same config
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	pid := os.Getpid()
	baseClientID := fmt.Sprintf("%s-%s-%d", cfg.ClientID, hostname, pid)

	clients := make([]*Client, poolSize)

	for i := 0; i < poolSize; i++ {
		clientCfg := *cfg
		clientCfg.ClientID = fmt.Sprintf("%s-%d", baseClientID, i)

		client, err := NewClient(&clientCfg, logger)
		if err != nil {
			for j := 0; j < i; j++ {
				_ = clients[j].Close()
			}
			return nil, fmt.Errorf("failed to create client %d: %w", i, err)
		}

		clients[i] = client
	}

	return &Pool{
		clients: clients,
		size:    poolSize,
		log:     logger,
	}, nil
}

// Publish publishes a message using round-robin across connections
func (p *Pool) Publish(ctx context.Context, payload message.Payload) error {
	idx := p.next.Add(1) % uint64(p.size) // #nosec G115
	return p.clients[idx].Publish(ctx, payload)
}

// SubscribeAck subscribes to ACK topic
func (p *Pool) SubscribeAck(handler func(message.AckMessage)) error {
	return p.clients[0].SubscribeAck(handler)
}

// Close closes all connections in the pool
func (p *Pool) Close() error {
	var lastErr error
	for i, client := range p.clients {
		if err := client.Close(); err != nil {
			lastErr = fmt.Errorf("failed to close client %d: %w", i, err)
		}
	}
	return lastErr
}
