package mqtt

import (
	"reflect"
	"testing"

	"github.com/ibs-source/syslog-consumer/internal/message"
)

func TestParseAck_Valid(t *testing.T) {
	tests := []struct {
		name     string
		payload  []byte
		expected message.AckMessage
	}{
		{
			name:    "single id ack true",
			payload: []byte(`{"ids":["msg-123"],"stream":"test-stream","ack":true}`),
			expected: message.AckMessage{
				IDs:    []string{"msg-123"},
				Stream: "test-stream",
				Ack:    true,
			},
		},
		{
			name:    "batch ids ack true",
			payload: []byte(`{"ids":["a","b","c"],"stream":"s","ack":true}`),
			expected: message.AckMessage{
				IDs:    []string{"a", "b", "c"},
				Stream: "s",
				Ack:    true,
			},
		},
		{
			name:    "ack false",
			payload: []byte(`{"ids":["msg-456"],"stream":"other-stream","ack":false}`),
			expected: message.AckMessage{
				IDs:    []string{"msg-456"},
				Stream: "other-stream",
				Ack:    false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ack, err := parseAck(tt.payload)
			if err != nil {
				t.Fatalf("parseAck() failed: %v", err)
			}

			if !reflect.DeepEqual(ack.IDs, tt.expected.IDs) {
				t.Errorf("expected IDs %v, got %v", tt.expected.IDs, ack.IDs)
			}
			if ack.Stream != tt.expected.Stream {
				t.Errorf("expected Stream %s, got %s", tt.expected.Stream, ack.Stream)
			}
			if ack.Ack != tt.expected.Ack {
				t.Errorf("expected Ack %v, got %v", tt.expected.Ack, ack.Ack)
			}
		})
	}
}

func TestParseAck_InvalidJSON(t *testing.T) {
	payload := []byte(`invalid json`)
	_, err := parseAck(payload)
	if err == nil {
		t.Error("expected error for invalid JSON, got nil")
	}
}

func TestParseAck_MissingIDs(t *testing.T) {
	payload := []byte(`{"ack":true}`)
	_, err := parseAck(payload)
	if err == nil {
		t.Error("expected error for missing ids, got nil")
	}
	if err != nil && err.Error() != "ack missing required field: ids" {
		t.Errorf("expected 'ack missing required field: ids', got '%s'", err.Error())
	}
}

func TestParseAck_EmptyIDs(t *testing.T) {
	payload := []byte(`{"ids":[],"stream":"s","ack":true}`)
	_, err := parseAck(payload)
	if err == nil {
		t.Error("expected error for empty ids array, got nil")
	}
}

func TestParseAck_MissingStream(t *testing.T) {
	payload := []byte(`{"ids":["msg-123"],"ack":true}`)
	_, err := parseAck(payload)
	if err == nil {
		t.Error("expected error for missing stream, got nil")
	}
	if err != nil && err.Error() != "ack missing required field: stream" {
		t.Errorf("expected 'ack missing required field: stream', got '%s'", err.Error())
	}
}

func TestParseAck_EmptyStream(t *testing.T) {
	payload := []byte(`{"ids":["msg-123"],"stream":"","ack":true}`)
	_, err := parseAck(payload)
	if err == nil {
		t.Error("expected error for empty stream, got nil")
	}
}

// --- benchmarks ---

var ackSink message.AckMessage

func BenchmarkParseAck(b *testing.B) {
	payload := []byte(`{"ids":["1771419690573-2"],"stream":"syslog-stream","ack":true}`)
	b.ReportAllocs()
	for range b.N {
		ack, err := parseAck(payload)
		if err != nil {
			b.Fatalf("parseAck(): %v", err)
		}
		ackSink = ack
	}
}

func BenchmarkParseAck_Parallel(b *testing.B) {
	payload := []byte(`{"ids":["1771419690573-2"],"stream":"syslog-stream","ack":true}`)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		var sink message.AckMessage
		for pb.Next() {
			ack, err := parseAck(payload)
			if err != nil {
				b.Fatalf("parseAck(): %v", err)
			}
			sink = ack
		}
		_ = sink
	})
}
