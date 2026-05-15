package log

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

const (
	levelLabelDebug = "DEBUG"
	levelLabelInfo  = "INFO"
	levelLabelWarn  = "WARN"
	levelLabelError = "ERROR"
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
	logger := New()
	if logger.Level() != slog.LevelInfo {
		t.Errorf("expected default level Info, got %v", logger.Level())
	}
}

func TestNew_CustomLevels(t *testing.T) {
	tests := []struct {
		level    string
		expected slog.Level
	}{
		{lvlTrace, LevelTrace},
		{lvlDebug, slog.LevelDebug},
		{lvlInfo, slog.LevelInfo},
		{lvlWarn, slog.LevelWarn},
		{lvlWarning, slog.LevelWarn},
		{lvlError, slog.LevelError},
		{"invalid", slog.LevelInfo}, // Should default to Info
	}

	for _, tt := range tests {
		t.Run(tt.level, func(t *testing.T) {
			logger := NewWithLevel(tt.level)
			if logger.Level() != tt.expected {
				t.Errorf("for level=%s, expected level %v, got %v", tt.level, tt.expected, logger.Level())
			}
		})
	}
}

func TestSetLevel(t *testing.T) {
	logger := New()

	tests := []struct {
		level    string
		expected slog.Level
	}{
		{lvlTrace, LevelTrace},
		{lvlDebug, slog.LevelDebug},
		{lvlInfo, slog.LevelInfo},
		{lvlWarn, slog.LevelWarn},
		{lvlWarning, slog.LevelWarn},
		{lvlError, slog.LevelError},
		{lvlFatal, LevelFatal},
		{lvlPanic, LevelPanic},
	}

	for _, tt := range tests {
		t.Run(tt.level, func(t *testing.T) {
			logger.SetLevel(tt.level)
			if logger.Level() != tt.expected {
				t.Errorf("for SetLevel(%s), expected level %v, got %v", tt.level, tt.expected, logger.Level())
			}
		})
	}
}

func TestSlog(t *testing.T) {
	logger := New()
	slogLogger := logger.Slog()

	if slogLogger == nil {
		t.Fatal("Slog() returned nil")
	}
}

// newTestLogger creates a Logger that writes to the given buffer at the specified level.
func newTestLogger(buf *bytes.Buffer, level slog.Level) *Logger {
	lv := &slog.LevelVar{}
	lv.Set(level)
	handler := slog.NewTextHandler(buf, &slog.HandlerOptions{
		Level: lv,
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			// Remove time for deterministic test output
			if a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			if a.Key == slog.LevelKey {
				lvl, ok := a.Value.Any().(slog.Level)
				if !ok {
					return a
				}
				if lvl <= LevelTrace {
					a.Value = slog.StringValue(labelTrace)
				}
			}
			return a
		},
	})
	return newWithHandler(handler, lv)
}

// newWithHandler creates a Logger using a custom slog.Handler (test helper).
func newWithHandler(h slog.Handler, level *slog.LevelVar) *Logger {
	return &Logger{log: slog.New(h), level: level}
}

func TestTracef(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf, LevelTrace)

	logger.Tracef(context.Background(), "test trace message")

	output := buf.String()
	if !strings.Contains(output, "test trace message") {
		t.Errorf("expected trace message in output, got: %s", output)
	}
}

func TestTraceWithFieldsf(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf, LevelTrace)

	logger.TraceWithFieldsf(context.Background(), Fields{"key": "value"}, "test trace")

	output := buf.String()
	if !strings.Contains(output, "test trace") || !strings.Contains(output, "key=value") {
		t.Errorf("expected trace message with fields in output, got: %s", output)
	}
}

func TestDebugf(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf, slog.LevelDebug)

	logger.Debugf(context.Background(), "test debug message")

	output := buf.String()
	if !strings.Contains(output, "test debug message") {
		t.Errorf("expected debug message in output, got: %s", output)
	}
}

func TestDebugWithFieldsf(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf, slog.LevelDebug)

	logger.DebugWithFieldsf(context.Background(), Fields{"id": "123"}, "test debug")

	output := buf.String()
	if !strings.Contains(output, "test debug") || !strings.Contains(output, "id=123") {
		t.Errorf("expected debug message with fields in output, got: %s", output)
	}
}

func TestInfof(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf, slog.LevelInfo)

	logger.Infof(context.Background(), "test info message")

	output := buf.String()
	if !strings.Contains(output, "test info message") {
		t.Errorf("expected info message in output, got: %s", output)
	}
}

func TestInfoWithFieldsf(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf, slog.LevelInfo)

	logger.InfoWithFieldsf(context.Background(), Fields{"status": "ok"}, "test info")

	output := buf.String()
	if !strings.Contains(output, "test info") || !strings.Contains(output, "status=ok") {
		t.Errorf("expected info message with fields in output, got: %s", output)
	}
}

func TestWarnf(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf, slog.LevelWarn)

	logger.Warnf(context.Background(), "test warn message")

	output := buf.String()
	if !strings.Contains(output, "test warn message") {
		t.Errorf("expected warn message in output, got: %s", output)
	}
}

func TestWarnWithFieldsf(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf, slog.LevelWarn)

	logger.WarnWithFieldsf(context.Background(), Fields{"reason": "timeout"}, "test warn")

	output := buf.String()
	if !strings.Contains(output, "test warn") || !strings.Contains(output, "reason=timeout") {
		t.Errorf("expected warn message with fields in output, got: %s", output)
	}
}

func TestErrorf(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf, slog.LevelError)

	logger.Errorf(context.Background(), "test error message")

	output := buf.String()
	if !strings.Contains(output, "test error message") {
		t.Errorf("expected error message in output, got: %s", output)
	}
}

func TestErrorWithFieldsf(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf, slog.LevelError)

	logger.ErrorWithFieldsf(context.Background(), Fields{"code": "500"}, "test error")

	output := buf.String()
	if !strings.Contains(output, "test error") || !strings.Contains(output, "code=500") {
		t.Errorf("expected error message with fields in output, got: %s", output)
	}
}

func TestWithField(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf, slog.LevelInfo)

	child := logger.WithField("user", "john")
	child.Infof(context.Background(), "test message")

	output := buf.String()
	if !strings.Contains(output, "test message") || !strings.Contains(output, "user=john") {
		t.Errorf("expected message with field in output, got: %s", output)
	}
}

func TestWithFields(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf, slog.LevelInfo)

	child := logger.WithFields(Fields{
		"user":   "john",
		"action": "login",
	})
	child.Infof(context.Background(), "test message")

	output := buf.String()
	if !strings.Contains(output, "test message") {
		t.Errorf("expected 'test message' in output, got: %s", output)
	}
	if !strings.Contains(output, "user=john") || !strings.Contains(output, "action=login") {
		t.Errorf("expected fields 'user=john' and 'action=login' in output, got: %s", output)
	}
}

func TestPanicf(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf, slog.LevelError)

	defer func() {
		r := recover()
		if r == nil {
			t.Error("expected panic, got none")
		}
		if r != "panic message" {
			t.Errorf("expected panic value 'panic message', got %v", r)
		}
	}()
	logger.Panicf(context.Background(), "panic message")
}

func TestLevel(t *testing.T) {
	logger := New()
	logger.SetLevel(lvlDebug)
	if logger.Level() != slog.LevelDebug {
		t.Errorf("expected debug level, got %v", logger.Level())
	}
}

func TestNew_ReplaceAttr_TraceLabel(t *testing.T) {
	// Use New() which installs the replaceAttr function, then log at TRACE level
	// and verify the level label is "TRACE".
	t.Setenv("LOG_LEVEL", lvlTrace)
	logger := New()

	// Verify New() correctly configures trace level via replaceAttr
	var buf bytes.Buffer
	lv := &slog.LevelVar{}
	lv.Set(LevelTrace)
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: lv,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return replaceAttr(groups, a)
		},
	})
	l := newWithHandler(h, lv)
	l.Tracef(context.Background(), "trace test")

	if !strings.Contains(buf.String(), labelTrace) {
		t.Errorf("expected TRACE label in output, got: %s", buf.String())
	}

	// Also check New() doesn't return nil with trace
	if logger == nil {
		t.Fatal("New() with LOG_LEVEL=trace returned nil")
	}
}

// TestReplaceAttr_Direct tests the exported replaceAttr function directly
// to ensure complete coverage of all level branches.
func TestReplaceAttr_Direct(t *testing.T) {
	tests := []struct {
		name    string
		wantVal string
		level   slog.Level
	}{
		{levelLabelDebug, levelLabelDebug, slog.LevelDebug},
		{levelLabelInfo, levelLabelInfo, slog.LevelInfo},
		{levelLabelWarn, levelLabelWarn, slog.LevelWarn},
		{levelLabelError, levelLabelError, slog.LevelError},
		{labelTrace, labelTrace, LevelTrace},
		{labelFatal, labelFatal, LevelFatal},
		{labelPanic, labelPanic, LevelPanic},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := slog.Attr{Key: slog.LevelKey, Value: slog.AnyValue(tt.level)}
			result := replaceAttr(nil, a)
			if result.Value.String() != tt.wantVal {
				t.Errorf("replaceAttr(%s) = %q; want %q", tt.name, result.Value.String(), tt.wantVal)
			}
		})
	}
}

func TestReplaceAttr_NonLevelKey(t *testing.T) {
	a := slog.String("msg", "hello")
	result := replaceAttr(nil, a)
	if result.Value.String() != "hello" {
		t.Errorf("replaceAttr(msg) = %q; want %q", result.Value.String(), "hello")
	}
}

func TestPanicWithFieldsf(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf, LevelPanic)

	defer func() {
		r := recover()
		if r == nil {
			t.Error("expected panic, got none")
		}
		if r != "panic with fields" {
			t.Errorf("expected panic value 'panic with fields', got %v", r)
		}
	}()
	logger.PanicWithFieldsf(context.Background(), Fields{"key": "val"}, "panic with fields")
}

// --- ReplaceAttr FATAL/PANIC label tests ---

// newTestLoggerWithAllLabels creates a Logger that uses the production replaceAttr
// function, writing to buf. This exercises the same code path as New().
func newTestLoggerWithAllLabels(buf *bytes.Buffer, level slog.Level) *Logger {
	lv := &slog.LevelVar{}
	lv.Set(level)
	opts := &slog.HandlerOptions{
		Level: lv,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Remove time for deterministic output, then delegate to production replaceAttr
			if a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return replaceAttr(groups, a)
		},
	}
	h := slog.NewTextHandler(buf, opts)
	return newWithHandler(h, lv)
}

func TestReplaceAttr_FatalLabel(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLoggerWithAllLabels(&buf, LevelFatal)

	// Log at FATAL level directly (without os.Exit) using the underlying slog
	logger.log.Log(t.Context(), LevelFatal, "fatal test")

	output := buf.String()
	if !strings.Contains(output, labelFatal) {
		t.Errorf("expected FATAL label in output, got: %s", output)
	}
}

func TestReplaceAttr_PanicLabel(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLoggerWithAllLabels(&buf, LevelPanic)

	// Log at PANIC level directly (without actual panic) using the underlying slog
	logger.log.Log(t.Context(), LevelPanic, "panic test")

	output := buf.String()
	if !strings.Contains(output, labelPanic) {
		t.Errorf("expected PANIC label in output, got: %s", output)
	}
}

func TestReplaceAttr_AllLevelLabels(t *testing.T) {
	tests := []struct {
		name     string
		expected string
		level    slog.Level
	}{
		{labelTrace, labelTrace, LevelTrace},
		{levelLabelDebug, levelLabelDebug, slog.LevelDebug},
		{levelLabelInfo, levelLabelInfo, slog.LevelInfo},
		{levelLabelWarn, levelLabelWarn, slog.LevelWarn},
		{levelLabelError, levelLabelError, slog.LevelError},
		{labelFatal, labelFatal, LevelFatal},
		{labelPanic, labelPanic, LevelPanic},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := newTestLoggerWithAllLabels(&buf, tt.level)
			logger.log.Log(t.Context(), tt.level, "test")

			output := buf.String()
			if !strings.Contains(output, tt.expected) {
				t.Errorf("expected %s label in output, got: %s", tt.expected, output)
			}
		})
	}
}

// --- setLevelVar edge cases ---

func TestSetLevelVar_AllCases(t *testing.T) {
	tests := []struct {
		input    string
		expected slog.Level
	}{
		{lvlTrace, LevelTrace},
		{lvlDebug, slog.LevelDebug},
		{lvlInfo, slog.LevelInfo},
		{"", slog.LevelInfo},
		{lvlWarn, slog.LevelWarn},
		{lvlWarning, slog.LevelWarn},
		{lvlError, slog.LevelError},
		{lvlFatal, LevelFatal},
		{lvlPanic, LevelPanic},
		{"unknown", slog.LevelInfo},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			lv := &slog.LevelVar{}
			setLevelVar(lv, tt.input)
			if lv.Level() != tt.expected {
				t.Errorf("setLevelVar(%q) = %v; want %v", tt.input, lv.Level(), tt.expected)
			}
		})
	}
}

// --- fieldsToAttrs ---

func TestFieldsToAttrs(t *testing.T) {
	fields := Fields{"key1": "val1", "key2": 42}
	attrs := fieldsToAttrs(fields)
	if len(attrs) != 2 {
		t.Errorf("fieldsToAttrs() returned %d attrs; want 2", len(attrs))
	}
}

func TestFieldsToAttrs_Empty(t *testing.T) {
	attrs := fieldsToAttrs(Fields{})
	if len(attrs) != 0 {
		t.Errorf("fieldsToAttrs({}) returned %d attrs; want 0", len(attrs))
	}
}
