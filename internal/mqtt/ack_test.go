package mqtt

import (
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
			name:    "valid ack true",
			payload: []byte(`{"id":"msg-123","ack":true}`),
			expected: message.AckMessage{
				ID:  "msg-123",
				Ack: true,
			},
		},
		{
			name:    "valid ack false",
			payload: []byte(`{"id":"msg-456","ack":false}`),
			expected: message.AckMessage{
				ID:  "msg-456",
				Ack: false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ack, err := parseAck(tt.payload)
			if err != nil {
				t.Fatalf("parseAck() failed: %v", err)
			}

			if ack.ID != tt.expected.ID {
				t.Errorf("expected ID %s, got %s", tt.expected.ID, ack.ID)
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

func TestParseAck_MissingID(t *testing.T) {
	payload := []byte(`{"ack":true}`)
	_, err := parseAck(payload)
	if err == nil {
		t.Error("expected error for missing ID, got nil")
	}
	if err != nil && err.Error() != "ack missing required field: id" {
		t.Errorf("expected 'ack missing required field: id', got '%s'", err.Error())
	}
}

func TestParseAck_EmptyID(t *testing.T) {
	payload := []byte(`{"id":"","ack":true}`)
	_, err := parseAck(payload)
	if err == nil {
		t.Error("expected error for empty ID, got nil")
	}
}
