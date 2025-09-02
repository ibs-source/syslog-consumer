package ringbuffer

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRingBuffer(t *testing.T) {
	t.Run("create with valid size", func(t *testing.T) {
		rb := New[interface{}](1024)
		assert.NotNil(t, rb)
		assert.Equal(t, 1024, rb.Capacity())
		assert.True(t, rb.IsEmpty())
		assert.False(t, rb.IsFull())
	})

	t.Run("create with minimum size", func(t *testing.T) {
		rb := New[interface{}](1)
		assert.NotNil(t, rb)
		assert.Equal(t, 1, rb.Capacity())
	})

	t.Run("create with non-power-of-2 size panics", func(t *testing.T) {
		assert.Panics(t, func() {
			New[interface{}](3)
		})
	})

	t.Run("create with zero size panics", func(t *testing.T) {
		assert.Panics(t, func() {
			New[interface{}](0)
		})
	})
}

func TestRingBufferPutGet_Basic(t *testing.T) {
	rb := New[string](16)

	testData := "test data"

	ok := rb.Put(&testData)
	assert.True(t, ok)

	retrieved := rb.Get()
	require.NotNil(t, retrieved)
	assert.Equal(t, testData, *retrieved)
}

func TestRingBufferPutGet_Multiple(t *testing.T) {
	rb := New[int](16)

	// Put multiple items
	for i := 0; i < 5; i++ {
		val := i
		ok := rb.Put(&val)
		assert.True(t, ok)
	}

	// Get them back in order
	for i := 0; i < 5; i++ {
		retrieved := rb.Get()
		require.NotNil(t, retrieved)
		assert.Equal(t, i, *retrieved)
	}
}

func TestRingBufferPutGet_PutToFull(t *testing.T) {
	rb := New[int](4)

	// Fill the buffer
	for i := 0; i < 4; i++ {
		val := i
		ok := rb.Put(&val)
		assert.True(t, ok)
	}

	assert.True(t, rb.IsFull())

	// Try to put when full - should fail
	val := 99
	ok := rb.Put(&val)
	assert.False(t, ok)

	// Get one item to make space
	retrieved := rb.Get()
	require.NotNil(t, retrieved)

	// Now put should succeed
	ok = rb.Put(&val)
	assert.True(t, ok)
}

func TestRingBufferPutGet_GetFromEmpty(t *testing.T) {
	rb := New[string](16)

	// Try to get from empty buffer - should return nil
	retrieved := rb.Get()
	assert.Nil(t, retrieved)

	// Put an item
	data := "test"
	rb.Put(&data)

	// Now get should succeed
	retrieved = rb.Get()
	require.NotNil(t, retrieved)
	assert.Equal(t, "test", *retrieved)
}

func TestRingBufferBatchOperations(t *testing.T) {
	t.Run("try put batch", func(t *testing.T) {
		rb := New[int](8)

		// Create items
		items := make([]*int, 5)
		for i := 0; i < 5; i++ {
			val := i
			items[i] = &val
		}

		// Put batch
		count := rb.TryPutBatch(items)
		assert.Equal(t, 5, count)
		assert.Equal(t, 5, rb.Size())

		// Try to put more than available
		moreItems := make([]*int, 5)
		for i := 0; i < 5; i++ {
			val := i + 10
			moreItems[i] = &val
		}

		count = rb.TryPutBatch(moreItems)
		assert.Equal(t, 3, count) // Only 3 slots available
		assert.True(t, rb.IsFull())
	})

	t.Run("try get batch", func(t *testing.T) {
		rb := New[int](8)

		// Fill with data
		for i := 0; i < 6; i++ {
			val := i
			rb.Put(&val)
		}

		// Get batch
		results := make([]*int, 4)
		count := rb.TryGetBatch(results)
		assert.Equal(t, 4, count)

		// Verify values
		for i := 0; i < 4; i++ {
			assert.NotNil(t, results[i])
			assert.Equal(t, i, *results[i])
		}

		// Try to get more than available
		moreResults := make([]*int, 4)
		count = rb.TryGetBatch(moreResults)
		assert.Equal(t, 2, count) // Only 2 items left
		assert.True(t, rb.IsEmpty())
	})
}

func TestRingBufferStatistics_Size(t *testing.T) {
	rb := New[int](16)
	assert.Equal(t, 0, rb.Size())

	// Add items
	for i := 0; i < 5; i++ {
		val := i
		rb.Put(&val)
	}
	assert.Equal(t, 5, rb.Size())

	// Remove items
	for i := 0; i < 2; i++ {
		rb.Get()
	}
	assert.Equal(t, 3, rb.Size())
}

func TestRingBufferStatistics_Capacity(t *testing.T) {
	rb := New[int](128)
	assert.Equal(t, 128, rb.Capacity())
}

func TestRingBufferStatistics_IsEmpty(t *testing.T) {
	rb := New[string](16)
	assert.True(t, rb.IsEmpty())

	data := "test"
	rb.Put(&data)
	assert.False(t, rb.IsEmpty())

	rb.Get()
	assert.True(t, rb.IsEmpty())
}

func TestRingBufferStatistics_IsFull(t *testing.T) {
	rb := New[int](4)
	assert.False(t, rb.IsFull())

	// Fill buffer
	for i := 0; i < 4; i++ {
		val := i
		rb.Put(&val)
	}
	assert.True(t, rb.IsFull())

	rb.Get()
	assert.False(t, rb.IsFull())
}

func TestRingBufferStatistics_AvailableForWriteRead(t *testing.T) {
	rb := New[int](8)

	assert.Equal(t, 8, rb.AvailableForWrite())
	assert.Equal(t, 0, rb.AvailableForRead())

	// Add some items
	for i := 0; i < 3; i++ {
		val := i
		rb.Put(&val)
	}

	assert.Equal(t, 5, rb.AvailableForWrite())
	assert.Equal(t, 3, rb.AvailableForRead())
}

func TestRingBufferDrainTo(t *testing.T) {
	rb := New[int](16)

	// Fill with data
	for i := 0; i < 10; i++ {
		val := i
		rb.Put(&val)
	}

	// Drain all items
	var collected []int
	count := rb.DrainTo(func(item *int) {
		collected = append(collected, *item)
	})

	assert.Equal(t, 10, count)
	assert.Equal(t, 10, len(collected))
	assert.True(t, rb.IsEmpty())

	// Verify order
	for i := 0; i < 10; i++ {
		assert.Equal(t, i, collected[i])
	}
}

func TestRingBufferConcurrency_PutGet(t *testing.T) {
	rb := New[int](1024)
	numProducers := 10
	numConsumers := 10
	itemsPerProducer := 100

	var wg sync.WaitGroup
	var producedCount, consumedCount atomic.Int64
	consumed := make(map[int]bool)
	var mu sync.Mutex

	// Producers
	for i := 0; i < numProducers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < itemsPerProducer; j++ {
				val := id*1000 + j
				for !rb.Put(&val) {
					runtime.Gosched()
				}
				producedCount.Add(1)
			}
		}(i)
	}

	// Consumers
	for i := 0; i < numConsumers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for consumedCount.Load() < int64(numProducers*itemsPerProducer) {
				item := rb.Get()
				if item != nil {
					mu.Lock()
					consumed[*item] = true
					mu.Unlock()
					consumedCount.Add(1)
				} else {
					runtime.Gosched()
				}
			}
		}()
	}

	wg.Wait()

	assert.Equal(t, producedCount.Load(), consumedCount.Load())
	assert.Equal(t, int64(numProducers*itemsPerProducer), producedCount.Load())
	assert.Equal(t, numProducers*itemsPerProducer, len(consumed))
}

func TestRingBufferConcurrency_BatchOperations(t *testing.T) {
	rb := New[int](256)

	var wg sync.WaitGroup
	var putCount, getCount atomic.Int64

	done := make(chan struct{})

	startBatchProducers(rb, 5, 20, 10, &putCount, &wg)
	startBatchConsumers(rb, 5, 10, &getCount, done, &wg)

	time.Sleep(100 * time.Millisecond)
	close(done)

	wg.Wait()

	// Final drain
	finalCount := rb.DrainTo(func(_ *int) {
		getCount.Add(1)
	})

	t.Logf("Put: %d, Get: %d, Final drain: %d", putCount.Load(), getCount.Load(), finalCount)
	assert.Equal(t, putCount.Load(), getCount.Load())
	assert.True(t, rb.IsEmpty())
}

func TestRingBufferUnsafeOperations(t *testing.T) {
	t.Run("unsafe put and get", func(t *testing.T) {
		rb := New[string](16)

		// Use unsafe operations (single-threaded context)
		data1 := "first"
		ok := rb.PutUnsafe(&data1)
		assert.True(t, ok)

		data2 := "second"
		ok = rb.PutUnsafe(&data2)
		assert.True(t, ok)

		// Get unsafe
		item1 := rb.GetUnsafe()
		assert.NotNil(t, item1)
		assert.Equal(t, "first", *item1)

		item2 := rb.GetUnsafe()
		assert.NotNil(t, item2)
		assert.Equal(t, "second", *item2)

		// Empty buffer
		item3 := rb.GetUnsafe()
		assert.Nil(t, item3)
	})
}

// Benchmark tests
func BenchmarkRingBufferPut(b *testing.B) {
	rb := New[int](1024)
	val := 42

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.Put(&val)
		rb.Get() // Keep buffer from filling
	}
}

func BenchmarkRingBufferGet(b *testing.B) {
	rb := New[int](1024)

	// Pre-fill buffer
	for i := 0; i < 512; i++ {
		val := i
		rb.Put(&val)
	}

	val := 42
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.Get()
		rb.Put(&val) // Keep buffer from emptying
	}
}

func BenchmarkRingBufferConcurrent(b *testing.B) {
	rb := New[int](1024)
	val := 42

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		localVal := val
		for pb.Next() {
			if rb.Put(&localVal) {
				rb.Get()
			}
		}
	})
}

func BenchmarkRingBufferBatchPut(b *testing.B) {
	rb := New[int](1024)

	// Prepare batch
	items := make([]*int, 10)
	for i := range items {
		val := i
		items[i] = &val
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.TryPutBatch(items)
		// Drain to keep space
		rb.DrainTo(func(*int) {})
	}
}

func BenchmarkRingBufferBatchGet(b *testing.B) {
	rb := New[int](1024)

	// Pre-fill
	for i := 0; i < 1024; i++ {
		val := i
		rb.Put(&val)
	}

	results := make([]*int, 10)
	refillItems := make([]*int, 10)
	for i := range refillItems {
		val := i
		refillItems[i] = &val
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.TryGetBatch(results)
		rb.TryPutBatch(refillItems) // Keep buffer from emptying
	}
}

func startBatchProducers(
	rb *RingBuffer[int],
	producers, rounds, batchSize int,
	putCount *atomic.Int64,
	wg *sync.WaitGroup,
) {
	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for round := 0; round < rounds; round++ {
				items := make([]*int, batchSize)
				for j := 0; j < batchSize; j++ {
					val := id*10000 + round*batchSize + j
					items[j] = &val
				}
				remaining := items
				for len(remaining) > 0 {
					count := rb.TryPutBatch(remaining)
					putCount.Add(int64(count))
					if count == len(remaining) {
						break
					}
					remaining = remaining[count:]
					runtime.Gosched()
				}
			}
		}(i)
	}
}

func startBatchConsumers(
	rb *RingBuffer[int],
	consumers, batchSize int,
	getCount *atomic.Int64,
	done <-chan struct{},
	wg *sync.WaitGroup,
) {
	for i := 0; i < consumers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results := make([]*int, batchSize)
			for {
				select {
				case <-done:
					return
				default:
					count := rb.TryGetBatch(results)
					if count > 0 {
						getCount.Add(int64(count))
					} else {
						runtime.Gosched()
					}
				}
			}
		}()
	}
}

func startStressWorkers(rb *RingBuffer[int], done <-chan struct{}, opsCount *atomic.Int64) {
	for i := 0; i < runtime.NumCPU(); i++ {
		// Producer
		go func(id int) {
			val := id
			for {
				select {
				case <-done:
					return
				default:
					if rb.Put(&val) {
						opsCount.Add(1)
					}
				}
			}
		}(i)

		// Consumer
		go func() {
			for {
				select {
				case <-done:
					return
				default:
					if rb.Get() != nil {
						opsCount.Add(1)
					}
				}
			}
		}()
	}
}

// Stress tests
func TestRingBufferStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	rb := New[int](256)
	duration := 2 * time.Second
	done := make(chan struct{})

	var opsCount atomic.Int64

	startStressWorkers(rb, done, &opsCount)

	time.Sleep(duration)
	close(done)

	// Give goroutines time to finish
	time.Sleep(100 * time.Millisecond)

	ops := opsCount.Load()
	opsPerSec := float64(ops) / duration.Seconds()
	t.Logf("Operations: %d, Ops/sec: %.2f", ops, opsPerSec)

	// Should achieve millions of ops/sec
	assert.Greater(t, ops, int64(1000000))
}

// Test edge cases
func TestRingBufferEdgeCases(t *testing.T) {
	t.Run("single element buffer", func(t *testing.T) {
		rb := New[string](1)

		data1 := "first"
		data2 := "second"

		// Put first
		ok := rb.Put(&data1)
		assert.True(t, ok)
		assert.True(t, rb.IsFull())

		// Try to put second (should fail)
		ok = rb.Put(&data2)
		assert.False(t, ok)

		// Get first
		retrieved := rb.Get()
		assert.NotNil(t, retrieved)
		assert.Equal(t, "first", *retrieved)
		assert.True(t, rb.IsEmpty())

		// Now can put second
		ok = rb.Put(&data2)
		assert.True(t, ok)
	})

	t.Run("power of 2 sizes", func(t *testing.T) {
		sizes := []uint32{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024}

		for _, size := range sizes {
			rb := New[int](size)
			assert.Equal(t, int(size), rb.Capacity())

			// Fill and empty
			for i := 0; i < int(size); i++ {
				val := i
				ok := rb.Put(&val)
				assert.True(t, ok)
			}
			assert.True(t, rb.IsFull())

			for i := 0; i < int(size); i++ {
				item := rb.Get()
				assert.NotNil(t, item)
				assert.Equal(t, i, *item)
			}
			assert.True(t, rb.IsEmpty())
		}
	})

	t.Run("batch with empty slices", func(t *testing.T) {
		rb := New[int](16)

		// Put empty batch
		count := rb.TryPutBatch([]*int{})
		assert.Equal(t, 0, count)

		// Get empty batch
		count = rb.TryGetBatch([]*int{})
		assert.Equal(t, 0, count)
	})
}
