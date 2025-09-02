// Package domain contains core message types and shared metrics for the processing pipeline.
package domain

import (
	"bytes"
	"sync"
	"time"
)

// Message represents a syslog message in the system
type Message struct {
	ID        string
	Timestamp time.Time
	Data      []byte // Changed from map[string]interface{} to []byte
	Attempts  int32  // Added for retry logic
}

// Reset clears the message for reuse
func (m *Message) Reset() {
	m.ID = ""
	m.Timestamp = time.Time{}
	m.Data = m.Data[:0] // Reset slice length, keep capacity
	m.Attempts = 0
}

// AckMessage represents an acknowledgment from MQTT
// Format from JavaScript server: { id: string, ack: boolean }
type AckMessage struct {
	ID  string `json:"id"`
	Ack bool   `json:"ack"`
}

// BufferPool is a pool for byte buffers to reduce allocations
var BufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}
