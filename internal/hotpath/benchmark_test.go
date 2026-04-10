package hotpath

import (
	"testing"

	"github.com/ibs-source/syslog-consumer/internal/message"
	"github.com/ubyte-source/go-jsonfast"
)

// BenchmarkBuildPayload measures the hot-path JSON envelope construction
// with a local builder (zero pool overhead).
func BenchmarkBuildPayload(b *testing.B) {
	hp := &HotPath{}
	builder := jsonfast.New(512)
	msg := message.Redis{
		ID:     "1234567890-0",
		Stream: "syslog:router1",
		Object: `{"facility":1,"severity":6,"message":"test syslog message","hostname":"router1"}`,
	}

	b.ResetTimer()
	b.ReportAllocs()
	var sink []byte
	for b.Loop() {
		sink = hp.buildPayload(builder, &msg)
	}
	_ = sink
}

// BenchmarkBuildPayload_LargePayload simulates a large syslog message with many fields.
func BenchmarkBuildPayload_LargePayload(b *testing.B) {
	hp := &HotPath{}
	builder := jsonfast.New(8192)
	msg := message.Redis{
		ID:     "1234567890-0",
		Stream: "syslog:router1",
		Object: `{"facility":1,"severity":6,"message":"` +
			string(make([]byte, 4000)) +
			`","hostname":"router1","source":"10.0.0.1"}`,
		Raw: string(make([]byte, 500)),
	}

	b.ResetTimer()
	b.ReportAllocs()
	var sink []byte
	for b.Loop() {
		sink = hp.buildPayload(builder, &msg)
	}
	_ = sink
}

// BenchmarkBuildPayload_EmptyFields measures edge case with empty fields (null payload).
func BenchmarkBuildPayload_EmptyFields(b *testing.B) {
	hp := &HotPath{}
	builder := jsonfast.New(256)
	msg := message.Redis{
		ID:     "1234567890-0",
		Stream: "syslog:router1",
	}

	b.ResetTimer()
	b.ReportAllocs()
	var sink []byte
	for b.Loop() {
		sink = hp.buildPayload(builder, &msg)
	}
	_ = sink
}

// BenchmarkBuildPayload_Parallel measures throughput under parallel workload.
// Each goroutine owns its builder (same as production: one builder per publish worker).
func BenchmarkBuildPayload_Parallel(b *testing.B) {
	hp := &HotPath{}
	msg := message.Redis{
		ID:     "1234567890-0",
		Stream: "syslog:router1",
		Object: `{"facility":1,"severity":6,"message":"test syslog message"}`,
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		builder := jsonfast.New(512)
		for pb.Next() {
			_ = hp.buildPayload(builder, &msg)
		}
	})
}

// BenchmarkBuildPayload_ObjectField measures the raw JSON embedding path.
func BenchmarkBuildPayload_ObjectField(b *testing.B) {
	hp := &HotPath{}
	builder := jsonfast.New(512)
	msg := message.Redis{
		ID:     "1234567890-0",
		Stream: "syslog:router1",
		Object: `{"facility":1,"severity":6,"message":"test syslog message","hostname":"router1"}`,
		Raw:    "test data",
	}

	b.ResetTimer()
	b.ReportAllocs()
	var sink []byte
	for b.Loop() {
		sink = hp.buildPayload(builder, &msg)
	}
	_ = sink
}
