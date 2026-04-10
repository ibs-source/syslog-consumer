package mqtt

import (
	"context"

	"github.com/ibs-source/syslog-consumer/internal/message"
)

// Publisher is the interface for publishing messages to MQTT
// Can be implemented by either a single Client or a Pool
type Publisher interface {
	Publish(ctx context.Context, payload message.Payload) error
	SubscribeAck(handler func(message.AckMessage)) error
	Close() error
}

// Ensure Client implements Publisher
var _ Publisher = (*Client)(nil)

// Ensure Pool implements Publisher
var _ Publisher = (*Pool)(nil)
