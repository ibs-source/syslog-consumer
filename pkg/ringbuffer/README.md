# pkg/ringbuffer

Lock-free multi-producer multi-consumer (MPMC) ring buffer used as the core in-memory queue for the processing pipeline. Designed for high throughput and low latency with minimal allocation and careful cache behavior.

Goals

- MPMC lock-free semantics suitable for concurrent producers/consumers.
- Batch get/put operations for throughput under load.
- Cache-friendly layout (padding to avoid false sharing).
- Safe integer conversions and bounds checks to avoid overflows.

Core Concepts

- Capacity is a power of two; indexing uses a mask for fast wrap-around.
- Producers/consumers coordinate via atomic positions and cached positions to reduce contention.
- Slots are atomic pointers; readers swap slot to nil when consuming.
- Batch operations amortize CAS overhead and reduce context switches.

API (selected)

- New[T any](capacity uint32) \*RingBuffer[T]
- Put(item \*T) bool
- Get() \*T
- TryPutBatch(items []\*T) int
- TryGetBatch(items []\*T) int
- Size() int
- IsEmpty() bool
- IsFull() bool
- Capacity() int
- AvailableForWrite() int
- AvailableForRead() int
- DropOldest(n int, onDrop func(\*T)) int
- EnsureCapacityOrDropOldest(need int, onDrop func(\*T)) int
- DrainTo(fn func(\*T)) int
- PutUnsafe(item \*T) bool
- GetUnsafe() \*T

Behavioral Notes

- Put returns false if the buffer is full at the moment of claim.
- Get returns nil if the buffer is empty.
- TryPutBatch/TryGetBatch will write/read up to len(items) or available capacity.
- DropOldest and EnsureCapacityOrDropOldest are helpers used by the processor’s “oldest” drop policy.
- Unsafe variants assume external synchronization (use only in single-threaded or externally synchronized contexts).

Performance Hints

- Choose capacity as a power of two (the constructor enforces this), sized for expected burst traffic.
- Prefer batch APIs in hot paths; they reduce CAS loops and scheduler interrupts.
- Avoid calling Size() in hot loops for flow control; use AvailableForWrite/Read or design based on batch outcomes.
- Combine with backpressure policies at higher layers rather than busy-waiting.

Example

```go
rb := ringbuffer.New[int](1024) // capacity must be power of two

// Producer
val := 42
_ = rb.Put(&val)

// Consumer
if p := rb.Get(); p != nil {
  fmt.Println(*p)
}

// Batch
in := []*int{&val, &val}
n := rb.TryPutBatch(in)
out := make([]*int, 2)
m := rb.TryGetBatch(out)
_ = n
_ = m
```

Safety

- Internals use atomic operations and padding to minimize false sharing.
- Integer conversions are clamped to avoid overflow on 32/64-bit platforms.
- The implementation avoids panics in normal usage; constructor panics if capacity is zero or not a power of two.

Configuration

- This package has no runtime configuration; sizing and policies are handled by higher-level components (see internal/processor and root README).
