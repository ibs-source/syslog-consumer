// Package ringbuffer implements a lock-free multi-producer multi-consumer ring buffer.
package ringbuffer

import (
	"math"
	"runtime"
	"sync/atomic"
	"unsafe"
)

const (
	// CacheLine size to prevent false sharing
	CacheLine = 64
)

// padding ensures cache line alignment
type padding [CacheLine]byte

// safeUint64ToInt converts u to int with an upper bound to avoid overflow.
// In this package, u is always derived from capacity (uint32), but we guard anyway.
func safeUint64ToInt(u uint64) int {
	maxU := uint64(math.MaxInt)
	if u > maxU {
		return math.MaxInt
	}
	return int(u)
}

// safeIntToUint64 converts n to uint64, clamping negatives to zero.
func safeIntToUint64(n int) uint64 {
	if n <= 0 {
		return 0
	}
	return uint64(n)
}

// RingBuffer is a lock-free multi-producer multi-consumer ring buffer
type RingBuffer[T any] struct {
	_              padding
	capacity       uint32
	mask           uint32
	_              padding
	writePos       atomic.Uint64
	_              padding
	readPos        atomic.Uint64
	_              padding
	buffer         []atomic.Pointer[T]
	_              padding
	cachedWritePos atomic.Uint64
	_              padding
	cachedReadPos  atomic.Uint64
}

// New creates a new ring buffer with the given capacity
// capacity must be a power of 2
func New[T any](capacity uint32) *RingBuffer[T] {
	if capacity == 0 || (capacity&(capacity-1)) != 0 {
		panic("capacity must be a power of 2")
	}

	rb := &RingBuffer[T]{
		capacity: capacity,
		mask:     capacity - 1,
		buffer:   make([]atomic.Pointer[T], capacity),
	}

	// Initialize buffer with nil pointers
	for i := range rb.buffer {
		rb.buffer[i].Store(nil)
	}

	return rb
}

// Internal availability helpers to reduce branching in batch ops (keeps lock-free semantics).
func (rb *RingBuffer[T]) availWrite(writePos, readPos uint64) int {
	capU := uint64(rb.capacity)
	used := writePos - readPos
	if used > capU {
		used = capU
	}
	return safeUint64ToInt(capU - used)
}

func (rb *RingBuffer[T]) availRead(writePos, readPos uint64) int {
	capU := uint64(rb.capacity)
	u := writePos - readPos
	if u > capU {
		u = capU
	}
	return safeUint64ToInt(u)
}

func (rb *RingBuffer[T]) ensureWriteAvailability(writePos *uint64, readPos *uint64) int {
	available := rb.availWrite(*writePos, *readPos)
	if available > 0 {
		return available
	}
	// Update cached read position and recompute
	rb.cachedReadPos.Store(rb.readPos.Load())
	*readPos = rb.cachedReadPos.Load()
	return rb.availWrite(*writePos, *readPos)
}

func (rb *RingBuffer[T]) ensureReadAvailability(readPos *uint64, writePos *uint64) int {
	available := rb.availRead(*writePos, *readPos)
	if available > 0 {
		return available
	}
	// Update cached write position and recompute
	rb.cachedWritePos.Store(rb.writePos.Load())
	*writePos = rb.cachedWritePos.Load()
	return rb.availRead(*writePos, *readPos)
}

// Put attempts to put an item into the ring buffer
// Returns false if the buffer is full
func (rb *RingBuffer[T]) Put(item *T) bool {
	var writePos, readPos uint64

	for {
		writePos = rb.writePos.Load()
		readPos = rb.cachedReadPos.Load()

		// Check if buffer is full
		if writePos-readPos >= uint64(rb.capacity) {
			// Update cached read position
			rb.cachedReadPos.Store(rb.readPos.Load())
			readPos = rb.cachedReadPos.Load()

			if writePos-readPos >= uint64(rb.capacity) {
				return false // Buffer is full
			}
		}

		// Try to claim the write position
		if rb.writePos.CompareAndSwap(writePos, writePos+1) {
			break
		}

		// Backoff on contention
		runtime.Gosched()
	}

	// Write the item (exclusive ownership of idx by claimed writePos)
	idx := writePos & uint64(rb.mask)
	rb.buffer[idx].Store(item)

	return true
}

// Get attempts to get an item from the ring buffer
// Returns nil if the buffer is empty
func (rb *RingBuffer[T]) Get() *T {
	var readPos, writePos uint64

	for {
		readPos = rb.readPos.Load()
		writePos = rb.cachedWritePos.Load()

		// Check if buffer is empty
		if readPos >= writePos {
			// Update cached write position
			rb.cachedWritePos.Store(rb.writePos.Load())
			writePos = rb.cachedWritePos.Load()

			if readPos >= writePos {
				return nil // Buffer is empty
			}
		}

		// Try to claim the read position
		if rb.readPos.CompareAndSwap(readPos, readPos+1) {
			break
		}

		// Backoff on contention
		runtime.Gosched()
	}

	// Read the item (atomically swap to nil)
	idx := readPos & uint64(rb.mask)
	for {
		if it := rb.buffer[idx].Swap(nil); it != nil {
			return it
		}
		runtime.Gosched()
	}
}

// TryPutBatch attempts to put multiple items into the ring buffer
// Returns the number of items successfully put
func (rb *RingBuffer[T]) TryPutBatch(items []*T) int {
	if len(items) == 0 {
		return 0
	}

	var writePos, readPos uint64
	var count int

	for {
		writePos = rb.writePos.Load()
		readPos = rb.cachedReadPos.Load()

		available := rb.ensureWriteAvailability(&writePos, &readPos)
		if available <= 0 {
			return 0
		}

		count = len(items)
		if count > available {
			count = available
		}

		if rb.writePos.CompareAndSwap(writePos, writePos+safeIntToUint64(count)) {
			break
		}
		runtime.Gosched()
	}

	// Write the items (exclusive ownership per claimed slot)
	for i := 0; i < count; i++ {
		idx := (writePos + safeIntToUint64(i)) & uint64(rb.mask)
		rb.buffer[idx].Store(items[i])
	}

	return count
}

// TryGetBatch attempts to get multiple items from the ring buffer
// Returns the actual number of items retrieved
func (rb *RingBuffer[T]) TryGetBatch(items []*T) int {
	if len(items) == 0 {
		return 0
	}

	var readPos, writePos uint64

	for {
		readPos = rb.readPos.Load()
		writePos = rb.cachedWritePos.Load()

		available := rb.ensureReadAvailability(&readPos, &writePos)
		if available <= 0 {
			return 0
		}

		limit := len(items)
		if limit > available {
			limit = available
		}

		// Determine contiguous ready items (non-nil) starting at readPos.
		ready := rb.readyCount(readPos, limit)
		if ready == 0 {
			// Items announced but not yet visible; yield and retry.
			runtime.Gosched()
			continue
		}

		if rb.readPos.CompareAndSwap(readPos, readPos+safeIntToUint64(ready)) {
			// Drain the ready items
			rb.drainRead(items, readPos, ready)
			return ready
		}

		runtime.Gosched()
	}
}

// readyCount returns the number of contiguous non-nil items available from readPos up to limit.
func (rb *RingBuffer[T]) readyCount(readPos uint64, limit int) int {
	ready := 0
	for i := 0; i < limit; i++ {
		idx := (readPos + safeIntToUint64(i)) & uint64(rb.mask)
		if rb.buffer[idx].Load() == nil {
			break
		}
		ready++
	}
	return ready
}

// drainRead drains 'ready' items starting at readPos into items slice, swapping each slot to nil.
func (rb *RingBuffer[T]) drainRead(items []*T, readPos uint64, ready int) {
	for i := 0; i < ready; i++ {
		idx := (readPos + safeIntToUint64(i)) & uint64(rb.mask)
		if it := rb.buffer[idx].Swap(nil); it != nil {
			items[i] = it
		}
	}
}

// Size returns the current number of items in the buffer
func (rb *RingBuffer[T]) Size() int {
	writePos := rb.writePos.Load()
	readPos := rb.readPos.Load()
	u := writePos - readPos
	capU := uint64(rb.capacity)
	if u > capU {
		u = capU
	}
	return safeUint64ToInt(u)
}

// IsEmpty returns true if the buffer is empty
func (rb *RingBuffer[T]) IsEmpty() bool {
	return rb.Size() == 0
}

// IsFull returns true if the buffer is full
func (rb *RingBuffer[T]) IsFull() bool {
	return rb.Size() >= int(rb.capacity)
}

// Capacity returns the capacity of the ring buffer
func (rb *RingBuffer[T]) Capacity() int {
	return int(rb.capacity)
}

// AvailableForWrite returns the number of slots available for writing
func (rb *RingBuffer[T]) AvailableForWrite() int {
	return int(rb.capacity) - rb.Size()
}

// AvailableForRead returns the number of items available for reading
func (rb *RingBuffer[T]) AvailableForRead() int {
	return rb.Size()
}

// DropOldest drops up to n oldest items from the buffer, invoking onDrop for each if provided.
// Returns the number of items dropped.
func (rb *RingBuffer[T]) DropOldest(n int, onDrop func(*T)) int {
	if n <= 0 {
		return 0
	}
	tmp := make([]*T, n)
	got := rb.TryGetBatch(tmp)
	if onDrop != nil {
		for i := 0; i < got; i++ {
			onDrop(tmp[i])
		}
	}
	return got
}

// EnsureCapacityOrDropOldest ensures there is space for 'need' items by dropping oldest if required.
// Returns the number of items dropped.
func (rb *RingBuffer[T]) EnsureCapacityOrDropOldest(need int, onDrop func(*T)) int {
	deficit := need - rb.AvailableForWrite()
	if deficit <= 0 {
		return 0
	}
	return rb.DropOldest(deficit, onDrop)
}

// DrainTo drains all available items to the provided function
// Returns the number of items drained
func (rb *RingBuffer[T]) DrainTo(fn func(*T)) int {
	count := 0
	for {
		item := rb.Get()
		if item == nil {
			break
		}
		fn(item)
		count++
	}
	return count
}

// GetUnsafe gets an item without any synchronization
// Only use when you have external synchronization
func (rb *RingBuffer[T]) GetUnsafe() *T {
	readPos := rb.readPos.Load()
	writePos := rb.writePos.Load()

	if readPos >= writePos {
		return nil
	}

	rb.readPos.Store(readPos + 1)
	idx := readPos & uint64(rb.mask)
	item := rb.buffer[idx].Load()
	rb.buffer[idx].Store(nil)

	return item
}

// PutUnsafe puts an item without any synchronization
// Only use when you have external synchronization
func (rb *RingBuffer[T]) PutUnsafe(item *T) bool {
	writePos := rb.writePos.Load()
	readPos := rb.readPos.Load()

	if writePos-readPos >= uint64(rb.capacity) {
		return false
	}

	idx := writePos & uint64(rb.mask)
	rb.buffer[idx].Store(item)
	rb.writePos.Store(writePos + 1)

	return true
}

// compile-time check for cache line size
var _ = unsafe.Sizeof(padding{}) == CacheLine
