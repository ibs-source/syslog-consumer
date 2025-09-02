package mqtt

import (
	"context"
	"testing"
	"time"

	"github.com/ibs-source/syslog/consumer/golang/internal/config"
)

func TestPublishSubscribeUnsubscribe_ErrWhenNotConnected(t *testing.T) {
	c := &client{
		cfg: &config.MQTTConfig{
			WriteTimeout: 50 * time.Millisecond,
			Topics:       config.TopicConfig{UseUserPrefix: false},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	if err := c.Publish(ctx, "t", 0, false, []byte("x")); err == nil {
		t.Fatalf("expected error when publishing while not connected")
	}
	if err := c.Subscribe(ctx, "t", 0, func(string, []byte) {}); err == nil {
		t.Fatalf("expected error when subscribing while not connected")
	}
	if err := c.Unsubscribe(ctx, "t"); err == nil {
		t.Fatalf("expected error when unsubscribing while not connected")
	}
}
