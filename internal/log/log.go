// Package log provides a structured logging wrapper around logrus.
package log

import (
	"os"

	"github.com/sirupsen/logrus"
)

// Logger wraps logrus.Logger per dependency injection
type Logger struct {
	log *logrus.Logger
}

// New crea una nuova istanza del logger con configurazione
func New() *Logger {
	l := logrus.New()
	l.SetOutput(os.Stdout)
	l.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05",
		ForceColors:     true,
	})

	// Default level: Info
	// Per abilitare trace/debug, impostare LOG_LEVEL=trace o LOG_LEVEL=debug
	level := os.Getenv("LOG_LEVEL")
	switch level {
	case "trace":
		l.SetLevel(logrus.TraceLevel)
	case "debug":
		l.SetLevel(logrus.DebugLevel)
	case "info":
		l.SetLevel(logrus.InfoLevel)
	case "warn", "warning":
		l.SetLevel(logrus.WarnLevel)
	case "error":
		l.SetLevel(logrus.ErrorLevel)
	default:
		l.SetLevel(logrus.InfoLevel)
	}

	return &Logger{log: l}
}

// SetLevel imposta il livello di log dinamicamente
func (l *Logger) SetLevel(level string) {
	switch level {
	case "trace":
		l.log.SetLevel(logrus.TraceLevel)
	case "debug":
		l.log.SetLevel(logrus.DebugLevel)
	case "info":
		l.log.SetLevel(logrus.InfoLevel)
	case "warn", "warning":
		l.log.SetLevel(logrus.WarnLevel)
	case "error":
		l.log.SetLevel(logrus.ErrorLevel)
	case "fatal":
		l.log.SetLevel(logrus.FatalLevel)
	case "panic":
		l.log.SetLevel(logrus.PanicLevel)
	}
}

// GetLogrus ritorna l'istanza logrus sottostante per usi avanzati
func (l *Logger) GetLogrus() *logrus.Logger {
	return l.log
}

// Trace logs trace-level messages (massimo dettaglio per debugging)
func (l *Logger) Trace(format string, v ...interface{}) {
	l.log.Tracef(format, v...)
}

// TraceWithFields logs trace message con campi strutturati
func (l *Logger) TraceWithFields(fields logrus.Fields, format string, v ...interface{}) {
	l.log.WithFields(fields).Tracef(format, v...)
}

// Debug logs debug messages
func (l *Logger) Debug(format string, v ...interface{}) {
	l.log.Debugf(format, v...)
}

// DebugWithFields logs debug message con campi strutturati
func (l *Logger) DebugWithFields(fields logrus.Fields, format string, v ...interface{}) {
	l.log.WithFields(fields).Debugf(format, v...)
}

// Info logs informational messages
func (l *Logger) Info(format string, v ...interface{}) {
	l.log.Infof(format, v...)
}

// InfoWithFields logs info message con campi strutturati
func (l *Logger) InfoWithFields(fields logrus.Fields, format string, v ...interface{}) {
	l.log.WithFields(fields).Infof(format, v...)
}

// Warn logs warning messages
func (l *Logger) Warn(format string, v ...interface{}) {
	l.log.Warnf(format, v...)
}

// WarnWithFields logs warning message con campi strutturati
func (l *Logger) WarnWithFields(fields logrus.Fields, format string, v ...interface{}) {
	l.log.WithFields(fields).Warnf(format, v...)
}

// Error logs error messages
func (l *Logger) Error(format string, v ...interface{}) {
	l.log.Errorf(format, v...)
}

// ErrorWithFields logs error message con campi strutturati
func (l *Logger) ErrorWithFields(fields logrus.Fields, format string, v ...interface{}) {
	l.log.WithFields(fields).Errorf(format, v...)
}

// Fatal logs an error message and exits
func (l *Logger) Fatal(format string, v ...interface{}) {
	l.log.Fatalf(format, v...)
}

// FatalWithFields logs fatal message con campi strutturati ed esce
func (l *Logger) FatalWithFields(fields logrus.Fields, format string, v ...interface{}) {
	l.log.WithFields(fields).Fatalf(format, v...)
}

// Panic logs a message e causa panic
func (l *Logger) Panic(format string, v ...interface{}) {
	l.log.Panicf(format, v...)
}

// PanicWithFields logs panic message con campi strutturati e causa panic
func (l *Logger) PanicWithFields(fields logrus.Fields, format string, v ...interface{}) {
	l.log.WithFields(fields).Panicf(format, v...)
}

// WithField crea entry con un campo strutturato
func (l *Logger) WithField(key string, value interface{}) *logrus.Entry {
	return l.log.WithField(key, value)
}

// WithFields crea entry con campi strutturati
func (l *Logger) WithFields(fields logrus.Fields) *logrus.Entry {
	return l.log.WithFields(fields)
}
