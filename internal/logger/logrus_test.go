package logger

import (
	"testing"

	"github.com/ibs-source/syslog/consumer/golang/internal/ports"
)

func TestNewLogrusLoggerAndMethods(t *testing.T) {
	l, err := NewLogrusLogger("debug", "json")
	if err != nil {
		t.Fatalf("NewLogrusLogger error: %v", err)
	}

	// Ensure no panics on all levels and WithFields
	l.Trace("trace", ports.Field{Key: "k", Value: 1})
	l.Debug("debug")
	l.Info("info")
	l.Warn("warn")
	l.Error("error")

	l2 := l.WithFields(ports.Field{Key: "component", Value: "unit-test"})
	if l2 == nil {
		t.Fatalf("WithFields returned nil")
	}
	l2.Info("with-fields")

	// Fatal should not be called in tests, but ensure method exists
	_ = l2
}
