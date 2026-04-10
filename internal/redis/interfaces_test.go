package redis

import "testing"

// TestStreamClient_InterfaceCompliance verifies the compile-time
// interface satisfaction assertion declared in interfaces.go.
func TestStreamClient_InterfaceCompliance(t *testing.T) {
	// This is a compile-time check already in interfaces.go via:
	//   var _ StreamClient = (*Client)(nil)
	// This test exists to provide a _test.go counterpart for interfaces.go.

	var _ StreamClient = (*Client)(nil)
	t.Log("Client implements StreamClient")
}
