package log

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
)

func TestNew(t *testing.T) {
	logger := New()
	if logger == nil {
		t.Fatal("New() returned nil")
	}
	if logger.log == nil {
		t.Fatal("logger.log is nil")
	}
}

func TestNew_DefaultLevel(t *testing.T) {
	_ = os.Unsetenv("LOG_LEVEL")
	logger := New()
	if logger.log.GetLevel() != logrus.InfoLevel {
		t.Errorf("expected default level Info, got %v", logger.log.GetLevel())
	}
}

func TestNew_CustomLevels(t *testing.T) {
	tests := []struct {
		envValue string
		expected logrus.Level
	}{
		{"trace", logrus.TraceLevel},
		{"debug", logrus.DebugLevel},
		{"info", logrus.InfoLevel},
		{"warn", logrus.WarnLevel},
		{"warning", logrus.WarnLevel},
		{"error", logrus.ErrorLevel},
		{"invalid", logrus.InfoLevel}, // Should default to Info
	}

	for _, tt := range tests {
		t.Run(tt.envValue, func(t *testing.T) {
			t.Setenv("LOG_LEVEL", tt.envValue)

			logger := New()
			if logger.log.GetLevel() != tt.expected {
				t.Errorf("for LOG_LEVEL=%s, expected level %v, got %v", tt.envValue, tt.expected, logger.log.GetLevel())
			}
		})
	}
}

func TestSetLevel(t *testing.T) {
	logger := New()

	tests := []struct {
		level    string
		expected logrus.Level
	}{
		{"trace", logrus.TraceLevel},
		{"debug", logrus.DebugLevel},
		{"info", logrus.InfoLevel},
		{"warn", logrus.WarnLevel},
		{"warning", logrus.WarnLevel},
		{"error", logrus.ErrorLevel},
		{"fatal", logrus.FatalLevel},
		{"panic", logrus.PanicLevel},
	}

	for _, tt := range tests {
		t.Run(tt.level, func(t *testing.T) {
			logger.SetLevel(tt.level)
			if logger.log.GetLevel() != tt.expected {
				t.Errorf("for SetLevel(%s), expected level %v, got %v", tt.level, tt.expected, logger.log.GetLevel())
			}
		})
	}
}

func TestGetLogrus(t *testing.T) {
	logger := New()
	logrusLogger := logger.GetLogrus()

	if logrusLogger == nil {
		t.Fatal("GetLogrus() returned nil")
	}
	if logrusLogger != logger.log {
		t.Error("GetLogrus() did not return the underlying logrus instance")
	}
}

func TestTrace(t *testing.T) {
	var buf bytes.Buffer
	logger := New()
	logger.log.SetOutput(&buf)
	logger.log.SetLevel(logrus.TraceLevel)
	logger.log.SetFormatter(&logrus.TextFormatter{DisableTimestamp: true})

	logger.Trace("test trace message")

	output := buf.String()
	if !strings.Contains(output, "test trace message") {
		t.Errorf("expected trace message in output, got: %s", output)
	}
}

func TestTraceWithFields(t *testing.T) {
	var buf bytes.Buffer
	logger := New()
	logger.log.SetOutput(&buf)
	logger.log.SetLevel(logrus.TraceLevel)
	logger.log.SetFormatter(&logrus.TextFormatter{DisableTimestamp: true})

	logger.TraceWithFields(logrus.Fields{"key": "value"}, "test trace")

	output := buf.String()
	if !strings.Contains(output, "test trace") || !strings.Contains(output, "key=value") {
		t.Errorf("expected trace message with fields in output, got: %s", output)
	}
}

func TestDebug(t *testing.T) {
	var buf bytes.Buffer
	logger := New()
	logger.log.SetOutput(&buf)
	logger.log.SetLevel(logrus.DebugLevel)
	logger.log.SetFormatter(&logrus.TextFormatter{DisableTimestamp: true})

	logger.Debug("test debug message")

	output := buf.String()
	if !strings.Contains(output, "test debug message") {
		t.Errorf("expected debug message in output, got: %s", output)
	}
}

func TestDebugWithFields(t *testing.T) {
	var buf bytes.Buffer
	logger := New()
	logger.log.SetOutput(&buf)
	logger.log.SetLevel(logrus.DebugLevel)
	logger.log.SetFormatter(&logrus.TextFormatter{DisableTimestamp: true})

	logger.DebugWithFields(logrus.Fields{"id": "123"}, "test debug")

	output := buf.String()
	if !strings.Contains(output, "test debug") || !strings.Contains(output, "id=123") {
		t.Errorf("expected debug message with fields in output, got: %s", output)
	}
}

func TestInfo(t *testing.T) {
	var buf bytes.Buffer
	logger := New()
	logger.log.SetOutput(&buf)
	logger.log.SetFormatter(&logrus.TextFormatter{DisableTimestamp: true})

	logger.Info("test info message")

	output := buf.String()
	if !strings.Contains(output, "test info message") {
		t.Errorf("expected info message in output, got: %s", output)
	}
}

func TestInfoWithFields(t *testing.T) {
	var buf bytes.Buffer
	logger := New()
	logger.log.SetOutput(&buf)
	logger.log.SetFormatter(&logrus.TextFormatter{DisableTimestamp: true})

	logger.InfoWithFields(logrus.Fields{"status": "ok"}, "test info")

	output := buf.String()
	if !strings.Contains(output, "test info") || !strings.Contains(output, "status=ok") {
		t.Errorf("expected info message with fields in output, got: %s", output)
	}
}

func TestWarn(t *testing.T) {
	var buf bytes.Buffer
	logger := New()
	logger.log.SetOutput(&buf)
	logger.log.SetFormatter(&logrus.TextFormatter{DisableTimestamp: true})

	logger.Warn("test warn message")

	output := buf.String()
	if !strings.Contains(output, "test warn message") {
		t.Errorf("expected warn message in output, got: %s", output)
	}
}

func TestWarnWithFields(t *testing.T) {
	var buf bytes.Buffer
	logger := New()
	logger.log.SetOutput(&buf)
	logger.log.SetFormatter(&logrus.TextFormatter{DisableTimestamp: true})

	logger.WarnWithFields(logrus.Fields{"reason": "timeout"}, "test warn")

	output := buf.String()
	if !strings.Contains(output, "test warn") || !strings.Contains(output, "reason=timeout") {
		t.Errorf("expected warn message with fields in output, got: %s", output)
	}
}

func TestError(t *testing.T) {
	var buf bytes.Buffer
	logger := New()
	logger.log.SetOutput(&buf)
	logger.log.SetFormatter(&logrus.TextFormatter{DisableTimestamp: true})

	logger.Error("test error message")

	output := buf.String()
	if !strings.Contains(output, "test error message") {
		t.Errorf("expected error message in output, got: %s", output)
	}
}

func TestErrorWithFields(t *testing.T) {
	var buf bytes.Buffer
	logger := New()
	logger.log.SetOutput(&buf)
	logger.log.SetFormatter(&logrus.TextFormatter{DisableTimestamp: true})

	logger.ErrorWithFields(logrus.Fields{"code": "500"}, "test error")

	output := buf.String()
	if !strings.Contains(output, "test error") || !strings.Contains(output, "code=500") {
		t.Errorf("expected error message with fields in output, got: %s", output)
	}
}

func TestWithField(t *testing.T) {
	var buf bytes.Buffer
	logger := New()
	logger.log.SetOutput(&buf)
	logger.log.SetFormatter(&logrus.TextFormatter{DisableTimestamp: true})

	entry := logger.WithField("user", "john")
	entry.Info("test message")

	output := buf.String()
	if !strings.Contains(output, "test message") || !strings.Contains(output, "user=john") {
		t.Errorf("expected message with field in output, got: %s", output)
	}
}

func TestWithFields(t *testing.T) {
	var buf bytes.Buffer
	logger := New()
	logger.log.SetOutput(&buf)
	logger.log.SetFormatter(&logrus.TextFormatter{DisableTimestamp: true})

	entry := logger.WithFields(logrus.Fields{
		"user":   "john",
		"action": "login",
	})
	entry.Info("test message")

	output := buf.String()
	if !strings.Contains(output, "test message") {
		t.Errorf("expected 'test message' in output, got: %s", output)
	}
	if !strings.Contains(output, "user=john") || !strings.Contains(output, "action=login") {
		t.Errorf("expected fields 'user=john' and 'action=login' in output, got: %s", output)
	}
}
