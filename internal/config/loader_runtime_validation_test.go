package config

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// generateTestCert creates a self-signed PEM certificate with the given CN
// and writes it to a temporary file. Returns the file path.
func generateTestCert(t *testing.T, cn string) string {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}

	certPath := filepath.Join(t.TempDir(), "cert.pem")
	f, err := os.Create(filepath.Clean(certPath))
	if err != nil {
		t.Fatalf("create cert file: %v", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			t.Errorf("close cert file: %v", err)
		}
	}()

	if err := pem.Encode(f, &pem.Block{Type: "CERTIFICATE", Bytes: der}); err != nil {
		t.Fatalf("encode PEM: %v", err)
	}
	return certPath
}

func TestApplyRuntimeValidation_WithoutCertCN(t *testing.T) {
	cfg := &Config{
		MQTT: MQTTConfig{
			PublishTopic:    "original/publish",
			AckTopic:        "original/ack",
			UseCertCNPrefix: false,
		},
	}

	if err := applyRuntimeValidation(cfg); err != nil {
		t.Fatalf("applyRuntimeValidation() error = %v; want nil", err)
	}

	// Topics should remain unchanged
	if cfg.MQTT.PublishTopic != "original/publish" {
		t.Errorf("PublishTopic = %s; want original/publish", cfg.MQTT.PublishTopic)
	}
	if cfg.MQTT.AckTopic != "original/ack" {
		t.Errorf("AckTopic = %s; want original/ack", cfg.MQTT.AckTopic)
	}
}

func TestApplyRuntimeValidation_WithCertCN(t *testing.T) {
	certPath := generateTestCert(t, "device-42")

	cfg := &Config{
		MQTT: MQTTConfig{
			PublishTopic:    "syslog/remote",
			AckTopic:        "syslog/remote/ack",
			UseCertCNPrefix: true,
			ClientCert:      certPath,
		},
	}

	if err := applyRuntimeValidation(cfg); err != nil {
		t.Fatalf("applyRuntimeValidation() error = %v; want nil", err)
	}

	if cfg.MQTT.PublishTopic != "device-42/syslog/remote" {
		t.Errorf("PublishTopic = %s; want device-42/syslog/remote", cfg.MQTT.PublishTopic)
	}
	if cfg.MQTT.AckTopic != "device-42/syslog/remote/ack" {
		t.Errorf("AckTopic = %s; want device-42/syslog/remote/ack", cfg.MQTT.AckTopic)
	}
}

func TestApplyRuntimeValidation_MissingCert(t *testing.T) {
	cfg := &Config{
		MQTT: MQTTConfig{
			PublishTopic:    "original/publish",
			AckTopic:        "original/ack",
			UseCertCNPrefix: true,
			ClientCert:      "/nonexistent/cert.pem",
		},
	}

	err := applyRuntimeValidation(cfg)
	if err == nil {
		t.Error("applyRuntimeValidation() error = nil; want error for missing cert")
	}
}

func TestExtractCNFromCertFile_InvalidCert(t *testing.T) {
	tmpDir := t.TempDir()
	certPath := filepath.Join(tmpDir, "invalid-cert.pem")
	if err := os.WriteFile(certPath, []byte("invalid cert content"), 0o600); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	_, err := extractCNFromCertFile(certPath)
	if err == nil {
		t.Error("extractCNFromCertFile() error = nil; want error for invalid cert")
	}
}

func TestExtractCNFromCertFile_ValidCert(t *testing.T) {
	certPath := generateTestCert(t, "my-device")

	cn, err := extractCNFromCertFile(certPath)
	if err != nil {
		t.Fatalf("extractCNFromCertFile() error = %v; want nil", err)
	}
	if cn != "my-device" {
		t.Errorf("CN = %s; want my-device", cn)
	}
}

func TestExtractCNFromCertFile_EmptyCN(t *testing.T) {
	certPath := generateTestCert(t, "") // empty CommonName

	_, err := extractCNFromCertFile(certPath)
	if err == nil {
		t.Error("extractCNFromCertFile() error = nil; want 'certificate has no CN'")
	}
}

func TestExtractCNFromCertFile_InvalidDER(t *testing.T) {
	// Valid PEM but invalid DER content → x509.ParseCertificate error
	tmpDir := t.TempDir()
	certPath := filepath.Join(tmpDir, "bad.pem")
	pemBlock := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: []byte("not valid DER"),
	})
	if err := os.WriteFile(certPath, pemBlock, 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}

	_, err := extractCNFromCertFile(certPath)
	if err == nil {
		t.Error("extractCNFromCertFile() error = nil; want parse error")
	}
}

func TestApplyTopicPrefix_NoCert(t *testing.T) {
	cfg := &Config{
		MQTT: MQTTConfig{
			PublishTopic:    "original/publish",
			AckTopic:        "original/ack",
			UseCertCNPrefix: true,
			ClientCert:      "", // Empty cert path
		},
	}

	if err := applyTopicPrefix(cfg); err != nil {
		t.Fatalf("applyTopicPrefix() error = %v; want nil", err)
	}

	// Topics should remain unchanged when cert path is empty
	if cfg.MQTT.PublishTopic != "original/publish" {
		t.Errorf("PublishTopic = %s; want original/publish", cfg.MQTT.PublishTopic)
	}
	if cfg.MQTT.AckTopic != "original/ack" {
		t.Errorf("AckTopic = %s; want original/ack", cfg.MQTT.AckTopic)
	}
}
