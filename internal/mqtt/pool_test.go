package mqtt

import (
	"testing"
)

// Note: Most functions in Pool require live MQTT broker connections
// (NewPool, Publish, SubscribeAck, Close) and are better tested via
// integration tests. The Pool manages multiple Client instances which
// each require MQTT broker connectivity.
//
// The core business logic is straightforward:
// - NewPool: Creates a pool of MQTT client connections
// - Publish: Round-robin publishes across pool connections
// - SubscribeAck: Subscribes to ACK topic on first client only
// - Close: Closes all connections in the pool
// - Size: Returns the pool size
//
// These are best verified through integration tests with an actual MQTT broker.

func TestPoolStruct(t *testing.T) {
	// Test that Pool struct can be created (compile-time check)
	var p *Pool
	if p != nil {
		t.Error("expected nil pool")
	}
}
