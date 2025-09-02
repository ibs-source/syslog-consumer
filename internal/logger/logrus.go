// Package logger provides a thin wrapper around logrus to satisfy the ports.Logger interface.
package logger

import (
	"os"

	"github.com/ibs-source/syslog/consumer/golang/internal/ports"
	"github.com/sirupsen/logrus"
)

// LogrusLogger implements ports.Logger using logrus.
type LogrusLogger struct {
	logger *logrus.Entry
}

// NewLogrusLogger creates a new Logrus logger instance.
func NewLogrusLogger(level, format string) (*LogrusLogger, error) {
	logger := logrus.New()

	// Set log level
	switch level {
	case "trace":
		logger.SetLevel(logrus.TraceLevel)
	case "debug":
		logger.SetLevel(logrus.DebugLevel)
	case "info":
		logger.SetLevel(logrus.InfoLevel)
	case "warn":
		logger.SetLevel(logrus.WarnLevel)
	case "error":
		logger.SetLevel(logrus.ErrorLevel)
	case "fatal":
		logger.SetLevel(logrus.FatalLevel)
	case "panic":
		logger.SetLevel(logrus.PanicLevel)
	default:
		logger.SetLevel(logrus.InfoLevel)
	}

	// Set formatter
	if format == "json" {
		logger.SetFormatter(&logrus.JSONFormatter{
			TimestampFormat: "2006-01-02T15:04:05.000Z07:00",
			FieldMap: logrus.FieldMap{
				logrus.FieldKeyTime:  "timestamp",
				logrus.FieldKeyLevel: "level",
				logrus.FieldKeyMsg:   "message",
			},
		})
	} else {
		logger.SetFormatter(&logrus.TextFormatter{
			FullTimestamp:   true,
			TimestampFormat: "2006-01-02T15:04:05.000Z07:00",
		})
	}

	// Set output
	logger.SetOutput(os.Stdout)

	// Disable caller reporting for cleaner logs
	logger.SetReportCaller(false)

	return &LogrusLogger{
		logger: logrus.NewEntry(logger),
	}, nil
}

// Trace logs a trace message.
func (l *LogrusLogger) Trace(msg string, fields ...ports.Field) {
	l.logger.WithFields(convertToLogrusFields(fields)).Trace(msg)
}

// Debug logs a debug message.
func (l *LogrusLogger) Debug(msg string, fields ...ports.Field) {
	l.logger.WithFields(convertToLogrusFields(fields)).Debug(msg)
}

// Info logs an info message.
func (l *LogrusLogger) Info(msg string, fields ...ports.Field) {
	l.logger.WithFields(convertToLogrusFields(fields)).Info(msg)
}

// Warn logs a warning message.
func (l *LogrusLogger) Warn(msg string, fields ...ports.Field) {
	l.logger.WithFields(convertToLogrusFields(fields)).Warn(msg)
}

// Error logs an error message.
func (l *LogrusLogger) Error(msg string, fields ...ports.Field) {
	l.logger.WithFields(convertToLogrusFields(fields)).Error(msg)
}

// Fatal logs a fatal message and exits.
func (l *LogrusLogger) Fatal(msg string, fields ...ports.Field) {
	l.logger.WithFields(convertToLogrusFields(fields)).Fatal(msg)
}

// WithFields returns a new logger with additional fields.
func (l *LogrusLogger) WithFields(fields ...ports.Field) ports.Logger {
	return &LogrusLogger{
		logger: l.logger.WithFields(convertToLogrusFields(fields)),
	}
}

// convertToLogrusFields converts ports.Field slice to logrus.Fields.
func convertToLogrusFields(fields []ports.Field) logrus.Fields {
	logrusFields := make(logrus.Fields)
	for _, f := range fields {
		logrusFields[f.Key] = f.Value
	}
	return logrusFields
}

// Global logger instance.
var globalLogrusLogger *LogrusLogger

// InitGlobalLogger initializes the global logger with logrus.
func InitGlobalLogger(level, format string) error {
	logger, err := NewLogrusLogger(level, format)
	if err != nil {
		return err
	}
	globalLogrusLogger = logger
	return nil
}

// GetGlobalLogger returns the global logger instance.
func GetGlobalLogger() ports.Logger {
	if globalLogrusLogger == nil {
		// Fallback to a default logger
		logger, _ := NewLogrusLogger("info", "json")
		globalLogrusLogger = logger
	}
	return globalLogrusLogger
}

// Field helper functions.

// String creates a string-valued logging field with the given key.
func String(key, value string) ports.Field {
	return ports.Field{Key: key, Value: value}
}

// Int creates an int-valued logging field with the given key.
func Int(key string, value int) ports.Field {
	return ports.Field{Key: key, Value: value}
}

// Int64 creates an int64-valued logging field with the given key.
func Int64(key string, value int64) ports.Field {
	return ports.Field{Key: key, Value: value}
}

// Float64 creates a float64-valued logging field with the given key.
func Float64(key string, value float64) ports.Field {
	return ports.Field{Key: key, Value: value}
}

// Bool creates a bool-valued logging field with the given key.
func Bool(key string, value bool) ports.Field {
	return ports.Field{Key: key, Value: value}
}

// Error creates a logging field for an error value using the key "error".
func Error(err error) ports.Field {
	return ports.Field{Key: "error", Value: err}
}

// Any creates a logging field with an arbitrary value under the given key.
func Any(key string, value interface{}) ports.Field {
	return ports.Field{Key: key, Value: value}
}
