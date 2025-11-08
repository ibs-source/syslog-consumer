package config

import (
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
)

// applyRuntimeValidation applies runtime validations and transformations
func applyRuntimeValidation(cfg *Config) error {
	return applyTopicPrefix(cfg)
}

// applyTopicPrefix prefixes MQTT topics with certificate CN if configured
func applyTopicPrefix(cfg *Config) error {
	if cfg.MQTT.UseCertCNPrefix && cfg.MQTT.ClientCert != "" {
		cn, err := extractCNFromCertFile(cfg.MQTT.ClientCert)
		if err != nil {
			return fmt.Errorf("failed to extract CN from certificate: %w", err)
		}
		cfg.MQTT.PublishTopic = cn + "/" + cfg.MQTT.PublishTopic
		cfg.MQTT.AckTopic = cn + "/" + cfg.MQTT.AckTopic
	}
	return nil
}

// extractCNFromCertFile extracts the CN from a PEM certificate file
func extractCNFromCertFile(certPath string) (string, error) {
	certPEM, err := os.ReadFile(certPath) // #nosec G304 - certPath is from config, not user input
	if err != nil {
		return "", fmt.Errorf("failed to read certificate: %w", err)
	}

	block, _ := pem.Decode(certPEM)
	if block == nil {
		return "", fmt.Errorf("failed to decode PEM certificate")
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return "", fmt.Errorf("failed to parse certificate: %w", err)
	}

	if cert.Subject.CommonName == "" {
		return "", fmt.Errorf("certificate has no CN")
	}

	return cert.Subject.CommonName, nil
}
