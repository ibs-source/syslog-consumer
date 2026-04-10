package mqtt

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/ibs-source/syslog-consumer/internal/message"
	"github.com/ubyte-source/go-jsonfast"
)

// ackParser holds intermediate state while parsing an ACK JSON payload.
type ackParser struct {
	ack   message.AckMessage
	found int // bitmask: 1=ids, 2=stream, 4=ack
}

// handleField processes a single key/value pair from the JSON object.
func (p *ackParser) handleField(key, value []byte) bool {
	switch string(key) {
	case `"ids"`:
		jsonfast.IterateStringArrayUnsafe(value, func(id string) bool {
			p.ack.IDs = append(p.ack.IDs, strings.Clone(id))
			return true
		})
		p.found |= 1
	case `"stream"`:
		if len(value) >= 2 && value[0] == '"' {
			p.ack.Stream = string(value[1 : len(value)-1])
		}
		p.found |= 2
	case `"ack"`:
		p.ack.Ack = bytes.Equal(value, []byte("true"))
		p.found |= 4
	}
	return true
}

// parseAck parses an ACK message from JSON payload using go-jsonfast's
// single-pass scanner. The ACK schema is:
//
//	{"ids":["id1","id2",...],"stream":"…","ack":true/false}
//
// This avoids encoding/json.Unmarshal overhead (~10x faster) and delegates
// all JSON scanning to go-jsonfast's SWAR-accelerated primitives.
func parseAck(payload []byte) (message.AckMessage, error) {
	var p ackParser
	if !jsonfast.IterateFields(payload, p.handleField) {
		return message.AckMessage{}, fmt.Errorf("ack: malformed JSON")
	}
	return validateAck(p.ack, p.found)
}

// validateAck checks that all required fields were found.
func validateAck(ack message.AckMessage, found int) (message.AckMessage, error) {
	if found&1 == 0 || len(ack.IDs) == 0 {
		return message.AckMessage{}, fmt.Errorf("ack missing required field: ids")
	}
	if found&2 == 0 || ack.Stream == "" {
		return message.AckMessage{}, fmt.Errorf("ack missing required field: stream")
	}
	return ack, nil
}
