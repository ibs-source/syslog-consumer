// Package ports defines the service interfaces (ports) used by the application to decouple implementations.
package ports

import (
	"context"
	"time"

	"github.com/ibs-source/syslog/consumer/golang/internal/domain"
)

// RedisClient defines the interface for Redis operations
type RedisClient interface {
	// Stream operations
	CreateConsumerGroup(ctx context.Context, stream, group, startID string) error
	ReadMessages(
		ctx context.Context,
		group, consumer, stream string,
		count int64,
		block time.Duration,
	) ([]*domain.Message, error)
	AckMessages(ctx context.Context, stream, group string, ids ...string) error
	DeleteMessages(ctx context.Context, stream string, ids ...string) error
	ClaimPendingMessages(
		ctx context.Context,
		stream, group, consumer string,
		minIdleTime time.Duration,
		count int64,
	) ([]*domain.Message, error)
	GetPendingMessages(ctx context.Context, stream, group string, start, end string, count int64) ([]PendingMessage, error)
	GetConsumers(ctx context.Context, stream, group string) ([]ConsumerInfo, error)
	RemoveConsumer(ctx context.Context, stream, group, consumer string) error

	// Stream operations for draining
	ReadStreamMessages(
		ctx context.Context,
		stream string,
		start string,
		count int64,
	) ([]*domain.Message, error)
	GetStreamInfo(ctx context.Context, stream string) (*StreamInfo, error)
	GetConsumerGroupInfo(
		ctx context.Context,
		stream string,
		group string,
	) (*ConsumerGroupInfo, error)

	// Consumer name
	GetConsumerName() string

	// Health check
	Ping(ctx context.Context) error
	Close() error
}

// MQTTClient defines the interface for MQTT operations
type MQTTClient interface {
	Connect(ctx context.Context) error
	Disconnect(timeout time.Duration)
	IsConnected() bool
	Publish(ctx context.Context, topic string, qos byte, retained bool, payload []byte) error
	Subscribe(ctx context.Context, topic string, qos byte, handler MessageHandler) error
	Unsubscribe(ctx context.Context, topics ...string) error

	// Certificate-based operations
	GetUserPrefix() string
}

// MessageHandler is the callback for MQTT messages
type MessageHandler func(topic string, payload []byte)

// Logger defines the interface for logging
type Logger interface {
	Trace(msg string, fields ...Field)
	Debug(msg string, fields ...Field)
	Info(msg string, fields ...Field)
	Warn(msg string, fields ...Field)
	Error(msg string, fields ...Field)
	Fatal(msg string, fields ...Field)
	WithFields(fields ...Field) Logger
}

// Field represents a logging field
type Field struct {
	Key   string
	Value interface{}
}

// PendingMessage represents a pending message in Redis
type PendingMessage struct {
	ID         string
	Consumer   string
	Idle       time.Duration
	RetryCount int64
}

// ConsumerInfo represents consumer information
type ConsumerInfo struct {
	Name    string
	Pending int64
	Idle    time.Duration
}

// StreamInfo represents Redis stream information
type StreamInfo struct {
	Length          int64
	RadixTreeKeys   int64
	RadixTreeNodes  int64
	Groups          int64
	LastGeneratedID string
	FirstEntry      *domain.Message
	LastEntry       *domain.Message
}

// ConsumerGroupInfo represents consumer group information
type ConsumerGroupInfo struct {
	Name            string
	Consumers       int64
	Pending         int64
	LastDeliveredID string
}

// HealthStatus represents the health status of a component
type HealthStatus struct {
	Healthy bool
	Message string
	Details map[string]interface{}
}

// GaugeMetric represents a gauge metric
type GaugeMetric interface {
	Set(value float64)
	Inc()
	Dec()
	Add(delta float64)
	Sub(delta float64)
}

// CounterMetric represents a counter metric
type CounterMetric interface {
	Inc()
	Add(delta float64)
}

// HistogramMetric represents a histogram metric
type HistogramMetric interface {
	Observe(value float64)
}

// CircuitBreaker defines the interface for circuit breaker pattern
type CircuitBreaker interface {
	Execute(fn func() error) error
	GetState() string
	GetStats() CircuitBreakerStats
}

// CircuitBreakerStats represents circuit breaker statistics
type CircuitBreakerStats struct {
	Requests            uint64
	TotalSuccess        uint64
	TotalFailure        uint64
	ConsecutiveFailures uint64
	State               string
}

// RetryPolicy defines retry behavior
type RetryPolicy struct {
	MaxAttempts     int
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Multiplier      float64
	MaxElapsedTime  time.Duration
}

// BackoffStrategy defines the backoff strategy for retries
type BackoffStrategy interface {
	NextInterval(attempt int) time.Duration
}
