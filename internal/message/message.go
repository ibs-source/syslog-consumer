// Package message provides shared data structures for Redis messages,
// MQTT acknowledgements, and batch processing.
package message

import "sync"

// Payload is the canonical alias for a raw, opaque message body.
type Payload = []byte

// Redis is the strongly-typed form of a Redis stream entry. Object and Raw
// are extracted at the Redis boundary so the hot path reads struct fields
// instead of doing map lookups; profiling showed map hashing accounting for
// 39% of CPU.
type Redis struct {
	Object string
	Raw    string
	ID     string
	// Stream is required for multi-stream ACK and XDEL operations.
	Stream string
}

// Batch is an envelope returned by Redis fetchers. When Items comes from a
// pooled slice, Release returns the backing array to the pool, avoiding a
// ~1.5 MB allocation per ReadBatch call.
type Batch struct {
	poolBuf *[]Redis
	pool    *sync.Pool
	Items   []Redis
}

// NewPooledBatch is the only way to associate a pool with a Batch since the
// pool fields are unexported.
func NewPooledBatch(items []Redis, poolBuf *[]Redis, pool *sync.Pool) Batch {
	return Batch{Items: items, poolBuf: poolBuf, pool: pool}
}

// Release is safe on zero-value or already-released batches.
func (b *Batch) Release() {
	if b.poolBuf != nil && b.pool != nil {
		clear(*b.poolBuf) // drop string references so the GC can collect them
		*b.poolBuf = (*b.poolBuf)[:0]
		b.pool.Put(b.poolBuf)
		b.poolBuf = nil
		b.pool = nil
	}
}

// AckMessage is the decoded MQTT acknowledgement payload.
type AckMessage struct {
	Stream string   `json:"stream"`
	IDs    []string `json:"ids"`
	Ack    bool     `json:"ack"`
}
