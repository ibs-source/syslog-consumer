// Package log provides a structured logging wrapper around log/slog.
package log

import (
	"context"
	"fmt"
	"log/slog"
	"os"
)

// Fields carries structured key/value pairs for the *WithFieldsf log methods.
type Fields = map[string]any

// Custom slog levels not provided by the standard library.
const (
	LevelTrace slog.Level = slog.LevelDebug - 4
	LevelFatal slog.Level = slog.LevelError + 4
	LevelPanic slog.Level = slog.LevelError + 8
)

const (
	lvlTrace   = "trace"
	lvlDebug   = "debug"
	lvlInfo    = "info"
	lvlWarn    = "warn"
	lvlWarning = "warning"
	lvlError   = "error"
	lvlFatal   = "fatal"
	lvlPanic   = "panic"
)

const (
	labelTrace = "TRACE"
	labelFatal = "FATAL"
	labelPanic = "PANIC"
)

// Logger wraps *slog.Logger and a dynamically updatable level.
type Logger struct {
	log   *slog.Logger
	level *slog.LevelVar
}

// New defaults to Info level; use NewWithLevel to override at construction.
func New() *Logger {
	return NewWithLevel(lvlInfo)
}

// NewWithLevel accepts the same level strings as SetLevel; unknown values
// fall back to Info.
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

// replaceAttr maps the custom TRACE/FATAL/PANIC levels to readable labels.
func replaceAttr(_ []string, a slog.Attr) slog.Attr {
	if a.Key == slog.LevelKey {
		lvl, ok := a.Value.Any().(slog.Level)
		if !ok {
			return a
		}
		switch {
		case lvl <= LevelTrace:
			a.Value = slog.StringValue(labelTrace)
		case lvl >= LevelPanic:
			a.Value = slog.StringValue(labelPanic)
		case lvl >= LevelFatal:
			a.Value = slog.StringValue(labelFatal)
		}
	}
	return a
}

func setLevelVar(lv *slog.LevelVar, level string) {
	switch level {
	case lvlTrace:
		lv.Set(LevelTrace)
	case lvlDebug:
		lv.Set(slog.LevelDebug)
	case lvlInfo, "":
		lv.Set(slog.LevelInfo)
	case lvlWarn, lvlWarning:
		lv.Set(slog.LevelWarn)
	case lvlError:
		lv.Set(slog.LevelError)
	case lvlFatal:
		lv.Set(LevelFatal)
	case lvlPanic:
		lv.Set(LevelPanic)
	default:
		lv.Set(slog.LevelInfo)
	}
}

// SetLevel updates the threshold at runtime; unknown values fall back to Info.
func (l *Logger) SetLevel(level string) {
	setLevelVar(l.level, level)
}

// Level reads the threshold atomically.
func (l *Logger) Level() slog.Level {
	return l.level.Level()
}

// DebugEnabled guards hot-path Debug calls to avoid variadic []any boxing
// at the call site.
func (l *Logger) DebugEnabled(ctx context.Context) bool {
	return l.log.Enabled(ctx, slog.LevelDebug)
}

// InfoEnabled guards hot-path Info calls to avoid variadic []any boxing
// at the call site.
func (l *Logger) InfoEnabled(ctx context.Context) bool {
	return l.log.Enabled(ctx, slog.LevelInfo)
}

// Slog exposes the underlying *slog.Logger for direct use.
func (l *Logger) Slog() *slog.Logger {
	return l.log
}

// Tracef and the other *f methods skip fmt.Sprintf when their level is
// disabled, avoiding heap allocations on the hot path.
func (l *Logger) Tracef(ctx context.Context, format string, v ...any) {
	if !l.log.Enabled(ctx, LevelTrace) {
		return
	}
	if len(v) == 0 {
		l.log.Log(ctx, LevelTrace, format)
		return
	}
	l.log.Log(ctx, LevelTrace, fmt.Sprintf(format, v...))
}

// TraceWithFieldsf is Tracef with structured fields appended as slog.Attr.
func (l *Logger) TraceWithFieldsf(ctx context.Context, fields Fields, format string, v ...any) {
	if !l.log.Enabled(ctx, LevelTrace) {
		return
	}
	l.log.LogAttrs(ctx, LevelTrace, fmt.Sprintf(format, v...), fieldsToAttrs(fields)...)
}

// Debugf is the debug-level *f method. See Tracef for level-gating behavior.
func (l *Logger) Debugf(ctx context.Context, format string, v ...any) {
	if !l.log.Enabled(ctx, slog.LevelDebug) {
		return
	}
	if len(v) == 0 {
		l.log.DebugContext(ctx, format)
		return
	}
	l.log.DebugContext(ctx, fmt.Sprintf(format, v...))
}

// DebugWithFieldsf is Debugf with structured fields appended as slog.Attr.
func (l *Logger) DebugWithFieldsf(ctx context.Context, fields Fields, format string, v ...any) {
	if !l.log.Enabled(ctx, slog.LevelDebug) {
		return
	}
	l.log.LogAttrs(ctx, slog.LevelDebug, fmt.Sprintf(format, v...), fieldsToAttrs(fields)...)
}

// Infof is the info-level *f method. See Tracef for level-gating behavior.
func (l *Logger) Infof(ctx context.Context, format string, v ...any) {
	if !l.log.Enabled(ctx, slog.LevelInfo) {
		return
	}
	if len(v) == 0 {
		l.log.InfoContext(ctx, format)
		return
	}
	l.log.InfoContext(ctx, fmt.Sprintf(format, v...))
}

// InfoWithFieldsf is Infof with structured fields appended as slog.Attr.
func (l *Logger) InfoWithFieldsf(ctx context.Context, fields Fields, format string, v ...any) {
	if !l.log.Enabled(ctx, slog.LevelInfo) {
		return
	}
	l.log.LogAttrs(ctx, slog.LevelInfo, fmt.Sprintf(format, v...), fieldsToAttrs(fields)...)
}

// Warnf is the warn-level *f method. See Tracef for level-gating behavior.
func (l *Logger) Warnf(ctx context.Context, format string, v ...any) {
	if !l.log.Enabled(ctx, slog.LevelWarn) {
		return
	}
	if len(v) == 0 {
		l.log.WarnContext(ctx, format)
		return
	}
	l.log.WarnContext(ctx, fmt.Sprintf(format, v...))
}

// WarnWithFieldsf is Warnf with structured fields appended as slog.Attr.
func (l *Logger) WarnWithFieldsf(ctx context.Context, fields Fields, format string, v ...any) {
	if !l.log.Enabled(ctx, slog.LevelWarn) {
		return
	}
	l.log.LogAttrs(ctx, slog.LevelWarn, fmt.Sprintf(format, v...), fieldsToAttrs(fields)...)
}

// Errorf is the error-level *f method. See Tracef for level-gating behavior.
func (l *Logger) Errorf(ctx context.Context, format string, v ...any) {
	if !l.log.Enabled(ctx, slog.LevelError) {
		return
	}
	if len(v) == 0 {
		l.log.ErrorContext(ctx, format)
		return
	}
	l.log.ErrorContext(ctx, fmt.Sprintf(format, v...))
}

// ErrorWithFieldsf is Errorf with structured fields appended as slog.Attr.
func (l *Logger) ErrorWithFieldsf(ctx context.Context, fields Fields, format string, v ...any) {
	if !l.log.Enabled(ctx, slog.LevelError) {
		return
	}
	l.log.LogAttrs(ctx, slog.LevelError, fmt.Sprintf(format, v...), fieldsToAttrs(fields)...)
}

// Fatalf is intentionally not level-gated: process termination must not be skipped.
func (l *Logger) Fatalf(ctx context.Context, format string, v ...any) {
	l.log.Log(ctx, LevelFatal, fmt.Sprintf(format, v...))
	os.Exit(1)
}

// FatalWithFieldsf is Fatalf with structured fields; also terminates the process.
func (l *Logger) FatalWithFieldsf(ctx context.Context, fields Fields, format string, v ...any) {
	l.log.LogAttrs(ctx, LevelFatal, fmt.Sprintf(format, v...), fieldsToAttrs(fields)...)
	os.Exit(1)
}

// Panicf is intentionally not level-gated: the panic must not be skipped.
func (l *Logger) Panicf(ctx context.Context, format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	l.log.Log(ctx, LevelPanic, msg)
	panic(msg)
}

// PanicWithFieldsf is Panicf with structured fields; also panics with the formatted message.
func (l *Logger) PanicWithFieldsf(ctx context.Context, fields Fields, format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	l.log.LogAttrs(ctx, LevelPanic, msg, fieldsToAttrs(fields)...)
	panic(msg)
}

// WithField returns a child logger; the child shares the level pointer so
// dynamic SetLevel propagates.
func (l *Logger) WithField(key string, value any) *Logger {
	return &Logger{log: l.log.With(key, value), level: l.level}
}

// WithFields is WithField for an entire Fields map. The child shares the
// level pointer with its parent.
func (l *Logger) WithFields(fields Fields) *Logger {
	attrs := make([]any, 0, len(fields)*2)
	for k, v := range fields {
		attrs = append(attrs, k, v)
	}
	return &Logger{log: l.log.With(attrs...), level: l.level}
}

func fieldsToAttrs(fields Fields) []slog.Attr {
	attrs := make([]slog.Attr, 0, len(fields))
	for k, v := range fields {
		attrs = append(attrs, slog.Any(k, v))
	}
	return attrs
}
