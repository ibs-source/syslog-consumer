package mqtt

import (
	"encoding/json"
	"fmt"

	"github.com/ibs-source/syslog-consumer/internal/message"
)

// parseAck parses an ACK message from JSON payload
func parseAck(payload []byte) (message.AckMessage, error) {
	var ack message.AckMessage

	if err := json.Unmarshal(payload, &ack); err != nil {
		return message.AckMessage{}, fmt.Errorf("failed to parse ack: %w", err)
	}

	// Validate required fields
	if ack.ID == "" {
		return message.AckMessage{}, fmt.Errorf("ack missing required field: id")
	}

	return ack, nil
}
