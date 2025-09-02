package processor

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ibs-source/syslog/consumer/golang/internal/domain"
)

func TestMsgQueueBasicFIFO(t *testing.T) {
	q := NewMsgQueue(8)

	msgs := []*domain.Message{
		{ID: "m1"}, {ID: "m2"}, {ID: "m3"},
		{ID: "m4"}, {ID: "m5"}, {ID: "m6"},
	}

	for _, m := range msgs {
		if ok := q.Put(m); !ok {
			t.Fatalf("Put failed unexpectedly for %v", m.ID)
		}
	}

	batch := make([]*domain.Message, len(msgs))
	got := q.TryGetBatch(batch)
	if got != len(msgs) {
		t.Fatalf("TryGetBatch got=%d want=%d", got, len(msgs))
	}

	for i := 0; i < got; i++ {
		if batch[i] == nil {
			t.Fatalf("nil message at index %d", i)
		}
		if batch[i].ID != msgs[i].ID {
			t.Fatalf("order mismatch at %d: got=%s want=%s", i, batch[i].ID, msgs[i].ID)
		}
	}

	if sz := q.Size(); sz != 0 {
		t.Fatalf("queue size after drain = %d, want 0", sz)
	}
}

func TestMsgQueueFillAndOverflow(t *testing.T) {
	q := NewMsgQueue(4)

	// Fill to capacity
	for i := 0; i < 4; i++ {
		if ok := q.Put(&domain.Message{ID: "x"}); !ok {
			t.Fatalf("unexpected Put failure at i=%d", i)
		}
	}
	// One more should fail
	if ok := q.Put(&domain.Message{ID: "overflow"}); ok {
		t.Fatalf("expected Put to fail when full")
	}
}

func TestMsgQueueDrainAndRefill(t *testing.T) {
	q := NewMsgQueue(4)

	// Fill to capacity
	fillQueueN(q, 4)

	// Drain two
	if got := drainCount(q, 2); got != 2 {
		t.Fatalf("got=%d want=2", got)
	}

	// Refill two
	fillQueueN(q, 2)

	if sz := q.Size(); sz != 4 {
		t.Fatalf("size=%d want=4", sz)
	}

	// Drain all remaining
	if total := drainAllCount(q); total != 4 {
		t.Fatalf("drained=%d want=4", total)
	}
	if sz := q.Size(); sz != 0 {
		t.Fatalf("final size=%d want=0", sz)
	}
}

func TestMsgQueueSPMC(t *testing.T) {
	runMsgQueueSPMC(t, 4, 2000)
}

// runMsgQueueSPMC executes a Single Producer Multiple Consumer scenario (realistic for the actual system).
func runMsgQueueSPMC(t *testing.T, consumers, totalMessages int) {
	t.Helper()
	q := NewMsgQueue(1024)

	var remaining atomic.Int64
	remaining.Store(int64(totalMessages))

	var wgProducer sync.WaitGroup
	var wgConsumers sync.WaitGroup
	var errFlag atomic.Int32

	startSPMCProducer(q, totalMessages, &wgProducer)
	startSPMCConsumers(q, consumers, &remaining, &errFlag, &wgConsumers)

	wgProducer.Wait()
	wgConsumers.Wait()

	validateSPMCResults(t, totalMessages, &remaining, &errFlag, q)
}

// startSPMCProducer starts a single producer goroutine.
func startSPMCProducer(q *MsgQueue, totalMessages int, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < totalMessages; i++ {
			msg := &domain.Message{ID: makeID(0, i)}
			for !q.Put(msg) {
				runtime.Gosched()
			}
		}
	}()
}

// startSPMCConsumers starts multiple consumer goroutines.
func startSPMCConsumers(
	q *MsgQueue, consumers int, remaining *atomic.Int64, errFlag *atomic.Int32, wg *sync.WaitGroup,
) {
	wg.Add(consumers)
	for c := 0; c < consumers; c++ {
		go func() {
			defer wg.Done()
			scratch := make([]*domain.Message, 32)
			for {
				n := q.TryGetBatch(scratch)
				if n == 0 {
					if remaining.Load() <= 0 {
						return
					}
					runtime.Gosched()
					continue
				}

				// Process messages
				for i := 0; i < n; i++ {
					if scratch[i] == nil {
						errFlag.Store(1)
						return
					}
				}

				// Decrement remaining count and check if we're done
				newRemaining := remaining.Add(-int64(n))
				if newRemaining <= 0 {
					return
				}
			}
		}()
	}
}

// validateSPMCResults validates the results of the SPMC test.
func validateSPMCResults(t *testing.T, totalMessages int, remaining *atomic.Int64, errFlag *atomic.Int32, q *MsgQueue) {
	t.Helper()
	if errFlag.Load() != 0 {
		t.Fatalf("nil message encountered in consumer")
	}

	consumed := int64(totalMessages) - remaining.Load()
	if consumed != int64(totalMessages) {
		t.Fatalf("consumed=%d want=%d (queue_size=%d)", consumed, totalMessages, q.Size())
	}
}

// makeID builds a small unique id; avoids fmt to reduce allocations in hot path of tests.
func makeID(p, i int) string {
	// simple, deterministic, low-alloc
	const letters = "0123456789abcdef"
	b := [9]byte{'p', 0, 'i', 0, 0, 0, 0, 0, 0}
	b[1] = letters[p%len(letters)]
	// encode i in hex (up to 6 nibbles)
	for k := 0; k < 6; k++ {
		b[8-k] = letters[(i>>(4*k))&0xF]
	}
	return string(b[:])
}

func fillQueueN(q *MsgQueue, n int) {
	for i := 0; i < n; i++ {
		for !q.Put(&domain.Message{ID: "x"}) {
			runtime.Gosched()
		}
	}
}

func drainCount(q *MsgQueue, n int) int {
	tmp := make([]*domain.Message, n)
	return q.TryGetBatch(tmp)
}

func drainAllCount(q *MsgQueue) int {
	total := 0
	scratch := make([]*domain.Message, 64)
	for {
		n := q.TryGetBatch(scratch)
		if n == 0 {
			break
		}
		total += n
	}
	return total
}

func BenchmarkMsgQueuePutGet(b *testing.B) {
	q := NewMsgQueue(1024)
	var msg domain.Message
	out := make([]*domain.Message, 1)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for !q.Put(&msg) {
			runtime.Gosched()
		}
		for q.TryGetBatch(out) == 0 {
			runtime.Gosched()
		}
	}
}

func BenchmarkMsgQueueTryGetBatch(b *testing.B) {
	q := NewMsgQueue(4096)
	scratch := make([]*domain.Message, 64)

	// Preload some messages
	pre := 1 << 16
	for i := 0; i < pre; i++ {
		_ = q.Put(&domain.Message{})
	}

	b.ReportAllocs()
	b.ResetTimer()

	empties := 0
	for i := 0; i < b.N; i++ {
		n := q.TryGetBatch(scratch)
		if n == 0 {
			empties++
			// Refill a bit to keep draining cost measurable
			for j := 0; j < 1024; j++ {
				for !q.Put(&domain.Message{}) {
					runtime.Gosched()
				}
			}
		}
	}
	_ = empties // avoid compiler optimizing away
}
