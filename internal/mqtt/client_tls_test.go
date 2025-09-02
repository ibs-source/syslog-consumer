package mqtt

import (
	"testing"

	"github.com/ibs-source/syslog/consumer/golang/internal/config"
	"github.com/ibs-source/syslog/consumer/golang/internal/logger"
)

func TestExtractUserPrefix_ValidCert(t *testing.T) {
	log, _ := logger.NewLogrusLogger("error", "json")
	c := &client{logger: log}

	tlsCfg := config.TLSConfig{
		Enabled:        true,
		CACertFile:     "../../testdata/ca.crt",
		ClientCertFile: "../../testdata/client.crt",
		ClientKeyFile:  "../../testdata/client.key",
	}

	prefix, err := c.extractUserPrefix(&tlsCfg)
	if err != nil {
		t.Fatalf("extractUserPrefix error: %v", err)
	}
	if prefix == "" {
		t.Fatalf("expected non-empty user prefix from certificate CN")
	}
}

func TestCreateTLSConfig_DeriveServerName(t *testing.T) {
	log, _ := logger.NewLogrusLogger("error", "json")
	c := &client{logger: log}

	tlsCfg := config.TLSConfig{
		Enabled:        true,
		CACertFile:     "../../testdata/ca.crt",
		ClientCertFile: "../../testdata/client.crt",
		ClientKeyFile:  "../../testdata/client.key",
		// ServerName intentionally empty to test derivation from broker URL
	}

	// Should derive "example.com" from the URL host when ServerName is empty
	brokers := []string{"ssl://example.com:8883"}
	conf, err := c.createTLSConfig(&tlsCfg, brokers)
	if err != nil {
		t.Fatalf("createTLSConfig error: %v", err)
	}
	if conf.ServerName == "" {
		t.Fatalf("expected derived ServerName, got empty")
	}
}

func TestIsConnectedAndDisconnectNilClient(t *testing.T) {
	// Ensure IsConnected handles nil client gracefully
	c := &client{}
	if c.IsConnected() {
		t.Fatalf("expected not connected when underlying client is nil")
	}
	// Disconnect should not panic when client is nil
	c.Disconnect(0)
}
