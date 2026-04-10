// Package log provides a structured logging wrapper around log/slog.
package log

import (
	"context"
	"fmt"
	"log/slog"
	"os"
)

// Fields is a map of structured key-value pairs for log messages.
// Replaces the former logrus.Fields dependency.
type Fields = map[string]any

// bgCtx caches context.Background() to avoid repeated calls on the hot path.
// context.Background() itself is cheap (returns a singleton), but caching
// avoids the function call overhead entirely.
var bgCtx = context.Background()

// Custom slog levels for Trace, Fatal, and Panic (not provided by slog).
const (
	LevelTrace slog.Level = slog.LevelDebug - 4
	LevelFatal slog.Level = slog.LevelError + 4
	LevelPanic slog.Level = slog.LevelError + 8
)

// Logger wraps slog.Logger for dependency injection.
type Logger struct {
	log   *slog.Logger
	level *slog.LevelVar
}

// New creates a new Logger instance with the default Info level.
func New() *Logger {
	return NewWithLevel("info")
}

// NewWithLevel creates a new Logger instance with the provided level.
func NewWithLevel(levelName string) *Logger {
	level := &slog.LevelVar{}
	setLevelVar(level, levelName)

	opts := &slog.HandlerOptions{
		Level:       level,
		ReplaceAttr: replaceAttr,
	}

	handler := slog.NewTextHandler(os.Stdout, opts)
	return &Logger{log: slog.New(handler), level: level}
}

// replaceAttr maps custom slog levels (TRACE, FATAL, PANIC) to readable labels.
func replaceAttr(_ []string, a slog.Attr) slog.Attr {
	if a.Key == slog.LevelKey {
		lvl, ok := a.Value.Any().(slog.Level)
		if !ok {
			return a
		}
		switch {
		case lvl <= LevelTrace:
			a.Value = slog.StringValue("TRACE")
		case lvl >= LevelPanic:
			a.Value = slog.StringValue("PANIC")
		case lvl >= LevelFatal:
			a.Value = slog.StringValue("FATAL")
		}
	}
	return a
}

// setLevelVar parses a level string into the LevelVar.
func setLevelVar(lv *slog.LevelVar, level string) {
	switch level {
	case "trace":
		lv.Set(LevelTrace)
	case "debug":
		lv.Set(slog.LevelDebug)
	case "info", "":
		lv.Set(slog.LevelInfo)
	case "warn", "warning":
		lv.Set(slog.LevelWarn)
	case "error":
		lv.Set(slog.LevelError)
	case "fatal":
		lv.Set(LevelFatal)
	case "panic":
		lv.Set(LevelPanic)
	default:
		lv.Set(slog.LevelInfo)
	}
}

// SetLevel sets the log level dynamically.
func (l *Logger) SetLevel(level string) {
	setLevelVar(l.level, level)
}

// Level returns the current slog.Level.
func (l *Logger) Level() slog.Level {
	return l.level.Level()
}

// DebugEnabled reports whether Debug-level logging is active.
// Use to guard hot-path Debug calls and avoid variadic []any allocation
// at the call site.
func (l *Logger) DebugEnabled() bool {
	return l.log.Enabled(bgCtx, slog.LevelDebug)
}

// InfoEnabled reports whether Info-level logging is active.
// Use to guard hot-path Info calls and avoid argument boxing.
func (l *Logger) InfoEnabled() bool {
	return l.log.Enabled(bgCtx, slog.LevelInfo)
}

// Slog returns the underlying *slog.Logger for advanced use.
func (l *Logger) Slog() *slog.Logger {
	return l.log
}

// Trace logs trace-level messages (maximum detail for debugging).
// Level-gated: fmt.Sprintf is skipped entirely when Trace is disabled,
// eliminating heap allocations on the hot path.
func (l *Logger) Trace(format string, v ...any) {
	if !l.log.Enabled(bgCtx, LevelTrace) {
		return
	}
	if len(v) == 0 {
		l.log.Log(bgCtx, LevelTrace, format)
		return
	}
	l.log.Log(bgCtx, LevelTrace, fmt.Sprintf(format, v...))
}

// TraceWithFields logs a trace message with structured fields.
func (l *Logger) TraceWithFields(fields Fields, format string, v ...any) {
	if !l.log.Enabled(bgCtx, LevelTrace) {
		return
	}
	l.log.LogAttrs(bgCtx, LevelTrace, fmt.Sprintf(format, v...), fieldsToAttrs(fields)...)
}

// Debug logs debug-level messages.
// Level-gated: fmt.Sprintf is skipped entirely when Debug is disabled.
func (l *Logger) Debug(format string, v ...any) {
	if !l.log.Enabled(bgCtx, slog.LevelDebug) {
		return
	}
	if len(v) == 0 {
		l.log.Debug(format)
		return
	}
	l.log.Debug(fmt.Sprintf(format, v...))
}

// DebugWithFields logs a debug message with structured fields.
func (l *Logger) DebugWithFields(fields Fields, format string, v ...any) {
	if !l.log.Enabled(bgCtx, slog.LevelDebug) {
		return
	}
	l.log.LogAttrs(bgCtx, slog.LevelDebug, fmt.Sprintf(format, v...), fieldsToAttrs(fields)...)
}

// Info logs informational messages.
// Level-gated: fmt.Sprintf is skipped entirely when Info is disabled.
func (l *Logger) Info(format string, v ...any) {
	if !l.log.Enabled(bgCtx, slog.LevelInfo) {
		return
	}
	if len(v) == 0 {
		l.log.Info(format)
		return
	}
	l.log.Info(fmt.Sprintf(format, v...))
}

// InfoWithFields logs an info message with structured fields.
func (l *Logger) InfoWithFields(fields Fields, format string, v ...any) {
	if !l.log.Enabled(bgCtx, slog.LevelInfo) {
		return
	}
	l.log.LogAttrs(bgCtx, slog.LevelInfo, fmt.Sprintf(format, v...), fieldsToAttrs(fields)...)
}

// Warn logs warning messages.
// Level-gated: fmt.Sprintf is skipped entirely when Warn is disabled.
func (l *Logger) Warn(format string, v ...any) {
	if !l.log.Enabled(bgCtx, slog.LevelWarn) {
		return
	}
	if len(v) == 0 {
		l.log.Warn(format)
		return
	}
	l.log.Warn(fmt.Sprintf(format, v...))
}

// WarnWithFields logs a warning message with structured fields.
func (l *Logger) WarnWithFields(fields Fields, format string, v ...any) {
	if !l.log.Enabled(bgCtx, slog.LevelWarn) {
		return
	}
	l.log.LogAttrs(bgCtx, slog.LevelWarn, fmt.Sprintf(format, v...), fieldsToAttrs(fields)...)
}

// Error logs error-level messages.
// Level-gated: fmt.Sprintf is skipped entirely when Error is disabled.
func (l *Logger) Error(format string, v ...any) {
	if !l.log.Enabled(bgCtx, slog.LevelError) {
		return
	}
	if len(v) == 0 {
		l.log.Error(format)
		return
	}
	l.log.Error(fmt.Sprintf(format, v...))
}

// ErrorWithFields logs an error message with structured fields.
func (l *Logger) ErrorWithFields(fields Fields, format string, v ...any) {
	if !l.log.Enabled(bgCtx, slog.LevelError) {
		return
	}
	l.log.LogAttrs(bgCtx, slog.LevelError, fmt.Sprintf(format, v...), fieldsToAttrs(fields)...)
}

// Fatal logs a fatal message and exits.
// Not level-gated: Fatal always executes (process termination must not be skipped).
func (l *Logger) Fatal(format string, v ...any) {
	l.log.Log(bgCtx, LevelFatal, fmt.Sprintf(format, v...))
	os.Exit(1)
}

// FatalWithFields logs a fatal message with structured fields and exits.
func (l *Logger) FatalWithFields(fields Fields, format string, v ...any) {
	l.log.LogAttrs(bgCtx, LevelFatal, fmt.Sprintf(format, v...), fieldsToAttrs(fields)...)
	os.Exit(1)
}

// Panic logs a message and panics.
// Not level-gated: Panic always executes (panic must not be skipped).
func (l *Logger) Panic(format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	l.log.Log(bgCtx, LevelPanic, msg)
	panic(msg)
}

// PanicWithFields logs a panic message with structured fields and panics.
func (l *Logger) PanicWithFields(fields Fields, format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	l.log.LogAttrs(bgCtx, LevelPanic, msg, fieldsToAttrs(fields)...)
	panic(msg)
}

// WithField creates a child logger with a single structured field.
func (l *Logger) WithField(key string, value any) *Logger {
	return &Logger{log: l.log.With(key, value), level: l.level}
}

// WithFields creates a child logger with multiple structured fields.
func (l *Logger) WithFields(fields Fields) *Logger {
	attrs := make([]any, 0, len(fields)*2)
	for k, v := range fields {
		attrs = append(attrs, k, v)
	}
	return &Logger{log: l.log.With(attrs...), level: l.level}
}

// fieldsToAttrs converts a Fields map to slog.Attr slice.
func fieldsToAttrs(fields Fields) []slog.Attr {
	attrs := make([]slog.Attr, 0, len(fields))
	for k, v := range fields {
		attrs = append(attrs, slog.Any(k, v))
	}
	return attrs
}
