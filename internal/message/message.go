// Package message provides shared data structures for Redis messages, MQTT acknowledgments, and batch processing.
package message

// Payload is the canonical alias for raw message body
type Payload = []byte

// Redis is a strongly typed representation of Redis stream entries
type Redis[T any] struct {
	ID     string
	Stream string // Stream name (required for multi-stream ACK/delete operations)
	Body   T
}

// Batch is an envelope returned by Redis fetchers
type Batch[T any] struct {
	Items []Redis[T]
}

// AckMessage is a decoded MQTT acknowledgment payload
type AckMessage struct {
	ID     string `json:"id"`
	Stream string `json:"stream"`
	Ack    bool   `json:"ack"`
}
