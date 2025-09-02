// Package processor provides streaming processing components.
// This file introduces MsgQueue, a lock-free message task queue that wraps the
// existing pkg/ringbuffer RingBuffer to avoid channel contention and closure
// allocations in the hot path. It preserves zero-copy semantics by passing
// *domain.Message pointers end-to-end.
package processor

import (
	"github.com/ibs-source/syslog/consumer/golang/internal/domain"
	"github.com/ibs-source/syslog/consumer/golang/pkg/ringbuffer"
)

// WorkerTaskHandler defines the function signature for processing a single message.
// Workers will invoke this handler with the message pointer, preserving zero-copy.
type WorkerTaskHandler func(*domain.Message)

// MsgQueue is a lock-free multi-producer multi-consumer queue for message tasks.
// It is a thin wrapper around pkg/ringbuffer.RingBuffer[*domain.Message] to provide
// a focused API for the worker pool fast-path.
type MsgQueue struct {
	rb *ringbuffer.RingBuffer[domain.Message]
}

// NewMsgQueue creates a new lock-free message queue with the given capacity.
// Capacity must be a power of two; the underlying ring buffer enforces this and will panic otherwise.
func NewMsgQueue(capacity uint32) *MsgQueue {
	return &MsgQueue{
		rb: ringbuffer.New[domain.Message](capacity),
	}
}

// Put enqueues a message pointer into the queue.
// Returns false if the queue is full.
func (q *MsgQueue) Put(msg *domain.Message) bool {
	return q.rb.Put(msg)
}

// TryGetBatch dequeues up to len(batch) messages into the provided slice.
// Returns the number of messages actually dequeued. The function does not allocate.
func (q *MsgQueue) TryGetBatch(batch []*domain.Message) int {
	return q.rb.TryGetBatch(batch)
}

// Size returns the current number of messages in the queue.
func (q *MsgQueue) Size() int {
	return q.rb.Size()
}

// Capacity returns the fixed capacity of the queue.
func (q *MsgQueue) Capacity() int {
	return q.rb.Capacity()
}
