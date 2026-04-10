package message

import (
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"unsafe"
)

func TestAckMessage_JSONRoundTrip(t *testing.T) {
	original := AckMessage{IDs: []string{"123-0"}, Stream: "syslog-stream", Ack: true}
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var decoded AckMessage
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if !reflect.DeepEqual(decoded, original) {
		t.Errorf("got %+v, want %+v", decoded, original)
	}
}

func TestAckMessage_JSONFields(t *testing.T) {
	ack := AckMessage{IDs: []string{"x"}, Stream: "y", Ack: true}
	data, err := json.Marshal(ack)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	expected := `{"stream":"y","ids":["x"],"ack":true}`
	if string(data) != expected {
		t.Errorf("got %s, want %s", data, expected)
	}
}

func TestAckMessage_ZeroValue(t *testing.T) {
	var ack AckMessage
	if ack.IDs != nil || ack.Stream != "" || ack.Ack {
		t.Error("zero value not clean")
	}
}

func TestNewPooledBatch_Release(t *testing.T) {
	pool := &sync.Pool{
		New: func() any {
			s := make([]Redis, 0, 4)
			return &s
		},
	}

	pv := pool.Get()
	buf, ok := pv.(*[]Redis)
	if !ok {
		t.Fatal("pool.Get() returned unexpected type")
	}
	items := append(*buf, Redis{ID: "1", Stream: "s", Object: "{}", Raw: "raw"})

	batch := NewPooledBatch(items, buf, pool)

	if len(batch.Items) != 1 {
		t.Fatalf("Items len = %d; want 1", len(batch.Items))
	}
	if batch.Items[0].ID != "1" {
		t.Errorf("Items[0].ID = %q; want %q", batch.Items[0].ID, "1")
	}

	batch.Release()

	if batch.poolBuf != nil {
		t.Error("poolBuf should be nil after Release")
	}
	if batch.pool != nil {
		t.Error("pool should be nil after Release")
	}

	// Double release must be safe
	batch.Release()

	// Pool should return the recycled buffer
	pv2 := pool.Get()
	recycled, ok2 := pv2.(*[]Redis)
	if !ok2 {
		t.Fatal("pool.Get() returned unexpected type after release")
	}
	if cap(*recycled) < 4 {
		t.Errorf("recycled cap = %d; want >= 4", cap(*recycled))
	}
}

func TestBatch_Release_ZeroValue(_ *testing.T) {
	var b Batch
	b.Release() // must not panic
}

func TestRedis_CacheLine(t *testing.T) {
	var r Redis
	if sz := unsafe.Sizeof(r); sz > 64 {
		t.Errorf("Redis struct size = %d; want <= 64 (one cache line)", sz)
	}
}
