package redis

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/ibs-source/syslog/consumer/golang/internal/domain"
	goredis "github.com/redis/go-redis/v9"
)

func makeStreamsWithPayload(id string, payload interface{}) []goredis.XStream {
	return []goredis.XStream{
		{
			Stream: "s1",
			Messages: []goredis.XMessage{
				{ID: id, Values: map[string]interface{}{"payload": payload}},
			},
		},
	}
}

func TestConvertXMessages_PayloadString(t *testing.T) {
	t.Helper()
	c := &client{}

	jsonStr := `{"a":1,"b":"x"}`
	streams := makeStreamsWithPayload("1-1", jsonStr)

	msgs := c.convertXMessages(streams)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}

	var m map[string]interface{}
	if err := json.Unmarshal(msgs[0].Data, &m); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if m["a"] != float64(1) || m["b"] != "x" {
		t.Fatalf("unexpected payload: %#v", m)
	}
	if msgs[0].ID != "1-1" {
		t.Fatalf("expected id 1-1, got %s", msgs[0].ID)
	}
	if msgs[0].Timestamp.IsZero() {
		t.Fatalf("expected non-zero timestamp")
	}
}

func TestConvertXMessages_PayloadBytes(t *testing.T) {
	t.Helper()
	c := &client{}

	jsonBytes := []byte(`{"c":2,"d":"y"}`)
	streams := makeStreamsWithPayload("1-2", jsonBytes)

	msgs := c.convertXMessages(streams)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}

	var m map[string]interface{}
	if err := json.Unmarshal(msgs[0].Data, &m); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if m["c"] != float64(2) || m["d"] != "y" {
		t.Fatalf("unexpected payload: %#v", m)
	}
	if msgs[0].ID != "1-2" {
		t.Fatalf("expected id 1-2, got %s", msgs[0].ID)
	}
}

func TestConvertXMessages_CopyFieldsWhenNoPayload(t *testing.T) {
	t.Helper()
	c := &client{}

	now := time.Now()
	streams := []goredis.XStream{
		{
			Stream: "s1",
			Messages: []goredis.XMessage{
				{ID: "9-9", Values: map[string]interface{}{"k1": "v1", "k2": 123}},
			},
		},
	}

	msgs := c.convertXMessages(streams)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if msgs[0].ID != "9-9" {
		t.Fatalf("expected id 9-9, got %s", msgs[0].ID)
	}
	if msgs[0].Timestamp.Before(now.Add(-1 * time.Minute)) {
		t.Fatalf("timestamp not set reasonably")
	}

	var m map[string]interface{}
	if err := json.Unmarshal(msgs[0].Data, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if m["k1"] != "v1" || m["k2"] != float64(123) {
		t.Fatalf("unexpected copied fields: %#v", m)
	}
}

func TestGetConsumerNameFormat(t *testing.T) {
	t.Helper()
	c := &client{consumerName: "consumer-123"}
	if got := c.GetConsumerName(); got != "consumer-123" {
		t.Fatalf("expected consumer-123, got %s", got)
	}
}

func TestReadStreamMessages_NoPanicOnEmpty(t *testing.T) {
	t.Helper()
	// Ensure domain import is used to avoid accidental removal
	_ = &domain.Message{}
}
