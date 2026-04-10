// Package metrics provides process-level counters exposed via expvar.
// All counters are accessible at /debug/vars (standard expvar endpoint) and
// are grouped under the "consumer" namespace.
package metrics

import "expvar"

var (
	// MessagesFetched counts messages fetched from Redis.
	MessagesFetched = expvar.NewInt("consumer.messages_fetched")
	// MessagesPublished counts messages published to MQTT.
	MessagesPublished = expvar.NewInt("consumer.messages_published")
	// MessagesAcked counts messages acknowledged and deleted.
	MessagesAcked = expvar.NewInt("consumer.messages_acked")
	// MessagesNacked counts negatively acknowledged messages.
	MessagesNacked = expvar.NewInt("consumer.messages_nacked")
	// MessagesClaimed counts idle messages reclaimed.
	MessagesClaimed = expvar.NewInt("consumer.messages_claimed")

	// FetchErrors counts Redis fetch errors.
	FetchErrors = expvar.NewInt("consumer.errors_fetch")
	// PublishErrors counts MQTT publish errors.
	PublishErrors = expvar.NewInt("consumer.errors_publish")
	// AckErrors counts acknowledgement errors.
	AckErrors = expvar.NewInt("consumer.errors_ack")

	// AckQueueDepth tracks the current ACK worker queue depth.
	AckQueueDepth = expvar.NewInt("consumer.ack_queue_depth")

	// FetchBackpressure counts how often fetchLoop blocked waiting for publish workers.
	FetchBackpressure = expvar.NewInt("consumer.fetch_backpressure")

	// StreamsActive tracks the number of active streams.
	StreamsActive = expvar.NewInt("consumer.streams_active")
	// StreamsDiscovered counts newly discovered streams.
	StreamsDiscovered = expvar.NewInt("consumer.streams_discovered")

	// DeadConsumersRemoved counts dead consumers cleaned up.
	DeadConsumersRemoved = expvar.NewInt("consumer.dead_consumers_removed")
)
