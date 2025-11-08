package message

import (
	"testing"
)

func TestRedisMessage(t *testing.T) {
	msg := Redis[Payload]{
		ID:     "1234567890-0",
		Stream: "test-stream",
		Body:   []byte("test payload"),
	}

	if msg.ID != "1234567890-0" {
		t.Errorf("expected ID 1234567890-0, got %s", msg.ID)
	}
	if msg.Stream != "test-stream" {
		t.Errorf("expected stream test-stream, got %s", msg.Stream)
	}
	if string(msg.Body) != "test payload" {
		t.Errorf("expected body 'test payload', got %s", string(msg.Body))
	}
}

func TestBatch(t *testing.T) {
	batch := Batch[Payload]{
		Items: []Redis[Payload]{
			{ID: "msg1", Stream: "stream1", Body: []byte("data1")},
			{ID: "msg2", Stream: "stream2", Body: []byte("data2")},
		},
	}

	if len(batch.Items) != 2 {
		t.Errorf("expected 2 items, got %d", len(batch.Items))
	}
}

func TestAckMessage(t *testing.T) {
	ack := AckMessage{
		ID:     "1234567890-0",
		Stream: "test-stream",
		Ack:    true,
	}

	if ack.ID != "1234567890-0" {
		t.Errorf("expected ID 1234567890-0, got %s", ack.ID)
	}
	if ack.Stream != "test-stream" {
		t.Errorf("expected stream test-stream, got %s", ack.Stream)
	}
	if !ack.Ack {
		t.Error("expected ack to be true")
	}
}
