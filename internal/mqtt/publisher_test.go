package mqtt

import "testing"

// TestPublisher_InterfaceCompliance verifies compile-time interface
// satisfaction assertions declared in publisher.go.
func TestPublisher_InterfaceCompliance(t *testing.T) {
	// These are compile-time checks already in publisher.go via:
	//   var _ Publisher = (*Client)(nil)
	//   var _ Publisher = (*Pool)(nil)
	// This test exists to provide a _test.go counterpart for publisher.go
	// and to document that both Client and Pool satisfy Publisher.

	var _ Publisher = (*Client)(nil)
	var _ Publisher = (*Pool)(nil)
	t.Log("Client and Pool both implement Publisher")
}
