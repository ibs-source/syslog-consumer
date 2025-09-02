package mqtt

import (
	"testing"

	mqttlib "github.com/eclipse/paho.mqtt.golang"
	"github.com/ibs-source/syslog/consumer/golang/internal/config"
	"github.com/ibs-source/syslog/consumer/golang/internal/logger"
	"github.com/ibs-source/syslog/consumer/golang/internal/ports"
)

// stubMessage implements mqttlib.Message for testing onMessage routing.
type stubMessage struct {
	topic   string
	payload []byte
}

func (m *stubMessage) Duplicate() bool              { return false }
func (m *stubMessage) Qos() byte                    { return 1 }
func (m *stubMessage) Retained() bool               { return false }
func (m *stubMessage) Topic() string                { return m.topic }
func (m *stubMessage) MessageID() uint16            { return 1 }
func (m *stubMessage) Payload() []byte              { return m.payload }
func (m *stubMessage) Ack()                         {}
func (m *stubMessage) ReadPayload() ([]byte, error) { return m.payload, nil }

// Test buildTopic behavior with/without user prefix.
func TestBuildTopic(t *testing.T) {
	log, _ := logger.NewLogrusLogger("error", "json")
	c := &client{
		cfg: &config.MQTTConfig{
			Topics: config.TopicConfig{
				UseUserPrefix: true,
			},
		},
		logger:     log,
		userPrefix: "alice",
	}

	if got := c.buildTopic("foo/bar"); got != "alice/foo/bar" {
		t.Fatalf("expected alice/foo/bar, got %s", got)
	}

	// No double slashes
	if got := c.buildTopic("/foo/bar"); got != "alice/foo/bar" {
		t.Fatalf("expected alice/foo/bar, got %s", got)
	}

	// Without prefix
	c.userPrefix = ""
	if got := c.buildTopic("foo/bar"); got != "foo/bar" {
		t.Fatalf("expected foo/bar, got %s", got)
	}
}

// Test handler add/remove and onMessage routing logic using lock-free maps.
func TestHandlersAddRemoveAndOnMessage(t *testing.T) {
	log, _ := logger.NewLogrusLogger("error", "json")
	c := &client{
		cfg:    &config.MQTTConfig{Topics: config.TopicConfig{UseUserPrefix: false}},
		logger: log,
	}
	initial := make(map[string]ports.MessageHandler)
	c.handlers.Store(&initial)

	var handled int
	topic := "some/topic"
	full := c.buildTopic(topic)

	c.addHandler(full, func(_ string, _ []byte) {
		handled++
	})

	// Route a message to the registered handler
	msg := &stubMessage{topic: full, payload: []byte("x")}
	c.onMessage(mqttlib.Client(nil), msg)
	if handled != 1 {
		t.Fatalf("expected handler called once, got %d", handled)
	}

	// Remove and verify not handled again
	c.removeHandlers([]string{topic})
	c.onMessage(mqttlib.Client(nil), msg)
	if handled != 1 {
		t.Fatalf("expected handler count to remain 1 after removal, got %d", handled)
	}
}
