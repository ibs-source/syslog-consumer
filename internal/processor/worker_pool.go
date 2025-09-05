package processor

import (
	"context"
	"log"
	"math"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/ibs-source/syslog/consumer/golang/internal/domain"
)

// WorkerPool manages a pool of worker goroutines
type WorkerPool struct {
	minWorkers     int
	maxWorkers     int
	currentWorkers atomic.Int32
	tasks          chan func()
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	stopped        atomic.Bool
	started        atomic.Bool

	// Fast-path message queue (lock-free) and handler
	msgQueue   *MsgQueue
	msgHandler WorkerTaskHandler
}

// NewWorkerPool creates a new worker pool
func NewWorkerPool(minWorkers, maxWorkers int) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())

	if minWorkers <= 0 {
		minWorkers = 1
	}
	if maxWorkers < minWorkers {
		maxWorkers = minWorkers
	}

	return &WorkerPool{
		minWorkers: minWorkers,
		maxWorkers: maxWorkers,
		tasks:      make(chan func(), maxWorkers*10),
		ctx:        ctx,
		cancel:     cancel,

		// Default lock-free message queue capacity; tuned later by processor/config if needed.
		msgQueue: NewMsgQueue(1024),
	}
}

// NewWorkerPoolWithParent creates a new worker pool bound to a parent context.
func NewWorkerPoolWithParent(parent context.Context, minWorkers, maxWorkers int) *WorkerPool {
	ctx, cancel := context.WithCancel(parent)

	if minWorkers <= 0 {
		minWorkers = 1
	}
	if maxWorkers < minWorkers {
		maxWorkers = minWorkers
	}

	return &WorkerPool{
		minWorkers: minWorkers,
		maxWorkers: maxWorkers,
		tasks:      make(chan func(), maxWorkers*10),
		ctx:        ctx,
		cancel:     cancel,

		// Default lock-free message queue capacity; tuned later by processor/config if needed.
		msgQueue: NewMsgQueue(1024),
	}
}

// Start starts the worker pool
func (p *WorkerPool) Start() {
	if p.stopped.Load() {
		return
	}

	// Check if already started
	if !p.started.CompareAndSwap(false, true) {
		return
	}

	// Start minimum number of workers
	for i := 0; i < p.minWorkers; i++ {
		p.currentWorkers.Add(1)
		p.wg.Add(1)
		go p.runWorker()
	}
}

// Stop stops the worker pool gracefully
func (p *WorkerPool) Stop() {
	if !p.stopped.CompareAndSwap(false, true) {
		return
	}

	// Cancel context to signal workers to stop
	p.cancel()

	// Wait for all workers to finish
	p.wg.Wait()

	// Close task channel
	close(p.tasks)
}

// StopWithTimeout attempts to stop the worker pool within the provided context deadline.
// Returns true if all workers stopped before the context deadline, false otherwise.
// On timeout, it still closes the tasks channel to unblock any pending receives.
func (p *WorkerPool) StopWithTimeout(ctx context.Context) bool {
	// If already stopped, nothing to do
	if p.stopped.Load() {
		return true
	}

	// Mark stopped (only once)
	if !p.stopped.CompareAndSwap(false, true) {
		return true
	}

	// Signal workers to stop
	p.cancel()

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All workers exited; close tasks channel
		close(p.tasks)
		return true
	case <-ctx.Done():
		// Timed out: force-close tasks to unblock any receivers
		close(p.tasks)
		return false
	}
}

// Submit submits a task to the worker pool
func (p *WorkerPool) Submit(task func()) error {
	if p.stopped.Load() {
		return ErrPoolStopped
	}

	select {
	case p.tasks <- task:
		// Check if we need more workers
		p.maybeSpawnWorker()
		return nil
	case <-p.ctx.Done():
		return ErrPoolStopped
	}
}

// SubmitWait submits a task and waits for it to complete
func (p *WorkerPool) SubmitWait(task func()) error {
	if p.stopped.Load() {
		return ErrPoolStopped
	}

	done := make(chan struct{})
	wrapped := func() {
		defer close(done)
		task()
	}

	if err := p.Submit(wrapped); err != nil {
		return err
	}

	select {
	case <-done:
		return nil
	case <-p.ctx.Done():
		return ErrPoolStopped
	}
}

// GetWorkerCount returns the current number of workers
func (p *WorkerPool) GetWorkerCount() int {
	return int(p.currentWorkers.Load())
}

// SetMsgHandler sets the worker message handler for fast-path processing.
func (p *WorkerPool) SetMsgHandler(h WorkerTaskHandler) {
	p.msgHandler = h
}

// SetMsgQueueCapacity sets the capacity of the lock-free fast-path message queue.
// Must be called before Start(); capacity must be a power of two.
func (p *WorkerPool) SetMsgQueueCapacity(capacity uint32) {
	if capacity == 0 {
		return
	}
	p.msgQueue = NewMsgQueue(capacity)
}

// SubmitMsg enqueues a message into the lock-free queue for processing.
// Returns ErrPoolStopped if the pool is stopped, or ErrQueueFull if the queue is full.
func (p *WorkerPool) SubmitMsg(msg *domain.Message) error {
	if p.stopped.Load() {
		return ErrPoolStopped
	}
	if p.msgQueue == nil {
		return ErrPoolStopped
	}
	if ok := p.msgQueue.Put(msg); !ok {
		return ErrQueueFull
	}
	// Heuristic: ensure enough workers when queue grows
	p.maybeSpawnWorker()
	return nil
}

// SubmitBatch enqueues as many messages as possible into the lock-free queue.
// Returns the number of messages actually enqueued.
func (p *WorkerPool) SubmitBatch(msgs []*domain.Message) int {
	if p.stopped.Load() || p.msgQueue == nil {
		return 0
	}
	inserted := 0
	for _, m := range msgs {
		if !p.msgQueue.Put(m) {
			break
		}
		inserted++
	}
	if inserted > 0 {
		p.maybeSpawnWorker()
	}
	return inserted
}

// GetQueueDepth returns the current number of pending messages in the fast-path queue.
func (p *WorkerPool) GetQueueDepth() int {
	if p.msgQueue == nil {
		return 0
	}
	return p.msgQueue.Size()
}

// Helpers to reduce cyclomatic complexity in SetWorkerCount by extracting branching logic.

// clampInt clamps v to the inclusive range [min, max].
func clampInt(v, low, high int) int {
	if v < low {
		return low
	}
	if v > high {
		return high
	}
	return v
}

// capToMaxInt32 caps v to [0, math.MaxInt32] to remain safe for atomic.Int32 comparisons.
func capToMaxInt32(v int) int {
	if v < 0 {
		return 0
	}
	if v > math.MaxInt32 {
		return math.MaxInt32
	}
	return v
}

// SetWorkerCount adjusts the number of workers
func (p *WorkerPool) SetWorkerCount(count int) {
	// Compute target within configured min/max and ensure we respect atomic.Int32 capacity.
	target := clampInt(count, p.minWorkers, p.maxWorkers)
	boundMax := capToMaxInt32(p.maxWorkers)
	if target > boundMax {
		target = boundMax
	}

	// Incrementally spawn until we reach the target (no downsizing here).
	for {
		current := p.currentWorkers.Load()
		if int(current) >= target {
			break
		}
		if p.currentWorkers.CompareAndSwap(current, current+1) {
			p.wg.Add(1)
			go p.runWorker()
			continue
		}
		runtime.Gosched()
	}
}

func (p *WorkerPool) drainMsgQueue(scratch []*domain.Message) bool {
	if p.msgHandler == nil || p.msgQueue == nil {
		return false
	}
	if n := p.msgQueue.TryGetBatch(scratch); n > 0 {
		for i := 0; i < n; i++ {
			// Re-check cancellation between items to avoid long drains on shutdown.
			if p.stopped.Load() || p.ctx.Err() != nil {
				return true
			}
			p.executeMsg(scratch[i])
			// Help GC and avoid retaining references
			scratch[i] = nil
		}
		return true
	}
	return false
}

// runWorker is the main worker loop
func (p *WorkerPool) runWorker() {
	defer p.workerCleanup()

	// Preallocate scratch buffer for batch draining of the message queue.
	scratch := make([]*domain.Message, 64)

	for {
		// Exit promptly on stop/cancel even if the queue still has items.
		if p.stopped.Load() || p.ctx.Err() != nil {
			return
		}
		// Drain fast-path queue if available
		if p.drainMsgQueue(scratch) {
			continue
		}

		// Non-blocking check of the legacy task channel with context awareness.
		select {
		case task, ok := <-p.tasks:
			if !ok {
				return
			}
			p.executeTask(task)
		case <-p.ctx.Done():
			return
		default:
			// Nothing available right now; yield and loop to re-check msgQueue promptly.
			runtime.Gosched()
		}
	}
}

// workerCleanup cleans up after a worker exits
func (p *WorkerPool) workerCleanup() {
	p.currentWorkers.Add(-1)
	p.wg.Done()
}

// executeTask executes a task with panic recovery
func (p *WorkerPool) executeTask(task func()) {
	defer func() {
		if r := recover(); r != nil {
			// log and keep worker alive
			log.Printf("worker pool recovered from panic: %v\n%s", r, debug.Stack())
		}
	}()
	task()
}

// executeMsg executes the message handler with panic recovery.
func (p *WorkerPool) executeMsg(msg *domain.Message) {
	if msg == nil || p.msgHandler == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			// log and keep worker alive
			log.Printf("worker pool recovered from panic (msg): %v\n%s", r, debug.Stack())
		}
	}()
	p.msgHandler(msg)
}

// maybeSpawnWorker checks if we need to spawn more workers
func (p *WorkerPool) maybeSpawnWorker() {
	// Simple heuristic: if tasks channel is more than 50% full
	// and we haven't reached max workers, spawn a new one.
	// This check is racy, which is acceptable for a heuristic.
	if len(p.tasks) <= cap(p.tasks)/2 {
		return
	}

	// Use a CAS loop to prevent a thundering herd of new workers.
	for {
		current := p.currentWorkers.Load()

		// Bound maxWorkers to int32 once per check
		boundMax := p.maxWorkers
		if boundMax < 0 {
			boundMax = 0
		}
		if boundMax > math.MaxInt32 {
			boundMax = math.MaxInt32
		}
		if int(current) >= boundMax {
			return // Max capacity reached
		}

		// Attempt to increment the worker count atomically
		if p.currentWorkers.CompareAndSwap(current, current+1) {
			// If successful, we have "permission" to spawn a worker
			p.wg.Add(1)
			go p.runWorker()
			return
		}
		// If CAS fails, another goroutine just spawned a worker.
		// The loop will re-evaluate the condition. For high contention,
		// we can add a small backoff here.
		runtime.Gosched()
	}
}

// Error definitions
var (
	ErrPoolStopped = &PoolError{Message: "worker pool is stopped"}
	ErrQueueFull   = &PoolError{Message: "message queue is full"}
)

// PoolError represents a worker pool error
type PoolError struct {
	Message string
}

func (e *PoolError) Error() string {
	return e.Message
}
