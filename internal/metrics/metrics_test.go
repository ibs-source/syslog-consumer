package metrics

import (
	"expvar"
	"testing"
)

// TestExpvarRegistration verifies all expected expvar counters are registered
// and prevents accidental name collisions or typos.
func TestExpvarRegistration(t *testing.T) {
	expected := []string{
		"consumer.messages_fetched",
		"consumer.messages_published",
		"consumer.messages_acked",
		"consumer.messages_nacked",
		"consumer.messages_claimed",
		"consumer.errors_fetch",
		"consumer.errors_publish",
		"consumer.errors_ack",
		"consumer.ack_queue_depth",
		"consumer.streams_active",
		"consumer.streams_discovered",
		"consumer.dead_consumers_removed",
	}

	for _, name := range expected {
		v := expvar.Get(name)
		if v == nil {
			t.Errorf("expvar %q not registered", name)
			continue
		}
		if v.String() != "0" {
			t.Errorf("expvar %q initial value = %s; want 0", name, v.String())
		}
	}
}

// TestExpvarPointers verifies the package-level vars point to the registered expvars.
func TestExpvarPointers(t *testing.T) {
	vars := map[string]*expvar.Int{
		"consumer.messages_fetched":       MessagesFetched,
		"consumer.messages_published":     MessagesPublished,
		"consumer.messages_acked":         MessagesAcked,
		"consumer.messages_nacked":        MessagesNacked,
		"consumer.messages_claimed":       MessagesClaimed,
		"consumer.errors_fetch":           FetchErrors,
		"consumer.errors_publish":         PublishErrors,
		"consumer.errors_ack":             AckErrors,
		"consumer.ack_queue_depth":        AckQueueDepth,
		"consumer.streams_active":         StreamsActive,
		"consumer.streams_discovered":     StreamsDiscovered,
		"consumer.dead_consumers_removed": DeadConsumersRemoved,
	}

	for name, ptr := range vars {
		if ptr == nil {
			t.Errorf("var for %q is nil", name)
			continue
		}
		registered := expvar.Get(name)
		if registered == nil {
			t.Errorf("expvar %q not found in registry", name)
			continue
		}
		// Verify the pointer is the same as what's registered
		ptr.Add(1)
		if registered.String() != ptr.String() {
			t.Errorf("expvar %q: registered value = %s, pointer value = %s", name, registered.String(), ptr.String())
		}
		ptr.Add(-1) // reset
	}
}

// TestExpvarCount verifies we have exactly 13 counters (catches accidental additions/removals).
func TestExpvarCount(t *testing.T) {
	const wantCount = 13
	count := 0
	expvar.Do(func(kv expvar.KeyValue) {
		// Filter to our namespace; expvar.Do iterates all registered vars
		if len(kv.Key) > 9 && kv.Key[:9] == "consumer." {
			count++
		}
	})
	if count != wantCount {
		t.Errorf("expected %d consumer.* expvars, got %d", wantCount, count)
	}
}
