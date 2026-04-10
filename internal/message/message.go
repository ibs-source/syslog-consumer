// Package message provides shared data structures for Redis messages, MQTT acknowledgements, and batch processing.
package message

import "sync"

// Payload is the canonical alias for raw message body
type Payload = []byte

// Redis is a strongly typed representation of Redis stream entries.
// Object and Raw are extracted at the Redis boundary so the hot path
// accesses plain struct fields instead of map lookups (eliminates
// map hashing, which was 39% of CPU).
type Redis struct {
	Object string // "object" field: serialized JSON event
	Raw    string // "raw" field: original syslog line
	ID     string
	Stream string // Stream name (required for multi-stream ACK/delete operations)
}

// Batch is an envelope returned by Redis fetchers.
// When Items comes from a pooled slice, Release returns the backing array
// to the pool for reuse (avoiding a ~1.5 MB allocation per ReadBatch call).
type Batch struct {
	poolBuf *[]Redis   // non-nil when Items uses a pooled backing array
	pool    *sync.Pool // pool to return poolBuf to
	Items   []Redis
}

// NewPooledBatch creates a Batch whose backing slice will be returned
// to pool when Release is called. This is the only way to associate a
// pool with a Batch (the pool fields are unexported).
func NewPooledBatch(items []Redis, poolBuf *[]Redis, pool *sync.Pool) Batch {
	return Batch{Items: items, poolBuf: poolBuf, pool: pool}
}

// Release returns the batch's backing slice to the pool.
// Must be called after the consumer has finished processing Items.
// Safe to call on zero-value or already-released batches.
func (b *Batch) Release() {
	if b.poolBuf != nil && b.pool != nil {
		clear(*b.poolBuf) // zero struct fields so GC can collect strings
		*b.poolBuf = (*b.poolBuf)[:0]
		b.pool.Put(b.poolBuf)
		b.poolBuf = nil
		b.pool = nil
	}
}

// AckMessage is a decoded MQTT acknowledgement payload.
type AckMessage struct {
	Stream string   `json:"stream"`
	IDs    []string `json:"ids"`
	Ack    bool     `json:"ack"`
}
