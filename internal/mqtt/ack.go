package mqtt

import (
	"errors"
	"strings"

	"github.com/ibs-source/syslog-consumer/internal/message"
	"github.com/ubyte-source/go-jsonfast"
)

type ackParser struct {
	ack   message.AckMessage
	found int // bitmask: 1=ids, 2=stream, 4=ack
}

func (p *ackParser) handleField(key, value []byte) bool {
	switch string(key) {
	case `"ids"`:
		jsonfast.IterateStringArray(value, func(id string) bool {
			p.ack.IDs = append(p.ack.IDs, strings.Clone(id))
			return true
		})
		p.found |= 1
	case `"stream"`:
		if s, ok := jsonfast.DecodeString(value); ok {
			p.ack.Stream = s
		}
		p.found |= 2
	case `"ack"`:
		if v, ok := jsonfast.DecodeBool(value); ok {
			p.ack.Ack = v
		}
		p.found |= 4
	}
	return true
}

// parseAck expects the payload {"ids":[...],"stream":"…","ack":bool}.
func parseAck(payload []byte) (message.AckMessage, error) {
	var p ackParser
	if !jsonfast.IterateFields(payload, p.handleField) {
		return message.AckMessage{}, errors.New("ack: malformed JSON")
	}
	return validateAck(p.ack, p.found)
}

func validateAck(ack message.AckMessage, found int) (message.AckMessage, error) {
	if found&1 == 0 || len(ack.IDs) == 0 {
		return message.AckMessage{}, errors.New("ack missing required field: ids")
	}
	if found&2 == 0 || ack.Stream == "" {
		return message.AckMessage{}, errors.New("ack missing required field: stream")
	}
	return ack, nil
}
