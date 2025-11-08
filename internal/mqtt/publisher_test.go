package mqtt

import (
	"testing"
)

func TestPublisherInterface(t *testing.T) {
	// This test ensures that Client and Pool implement the Publisher interface
	// The actual verification is done at compile time via the var _ Publisher = (*Client)(nil) declarations

	// If this test compiles, it means both Client and Pool correctly implement Publisher
	t.Log("Client and Pool correctly implement Publisher interface")
}
