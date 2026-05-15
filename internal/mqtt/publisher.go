package mqtt

import (
	"context"

	"github.com/ibs-source/syslog-consumer/internal/message"
)

// Publisher is implemented by both Client and Pool.
type Publisher interface {
	Publish(ctx context.Context, payload message.Payload) error
	SubscribeAck(ctx context.Context, handler func(message.AckMessage)) error
	Close() error
}

var (
	_ Publisher = (*Client)(nil)
	_ Publisher = (*Pool)(nil)
)
