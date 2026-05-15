// Package metrics provides process-level counters exposed via expvar at
// /debug/vars under the "consumer" namespace.
package metrics

import "expvar"

// Counters published by the consumer. The expvar key — not the Go identifier
// — is the public contract; renaming a variable is safe, renaming the string
// passed to expvar.NewInt is a breaking change for any dashboard or scraper.
var (
	MessagesFetched   = expvar.NewInt("consumer.messages_fetched")
	MessagesPublished = expvar.NewInt("consumer.messages_published")
	MessagesAcked     = expvar.NewInt("consumer.messages_acked")
	MessagesNacked    = expvar.NewInt("consumer.messages_nacked")
	MessagesClaimed   = expvar.NewInt("consumer.messages_claimed")

	FetchErrors   = expvar.NewInt("consumer.errors_fetch")
	PublishErrors = expvar.NewInt("consumer.errors_publish")
	AckErrors     = expvar.NewInt("consumer.errors_ack")

	AckQueueDepth = expvar.NewInt("consumer.ack_queue_depth")

	// FetchBackpressure is incremented every time fetchLoop's non-blocking
	// send fails and we have to wait for a publish worker to drain.
	FetchBackpressure = expvar.NewInt("consumer.fetch_backpressure")

	StreamsActive     = expvar.NewInt("consumer.streams_active")
	StreamsDiscovered = expvar.NewInt("consumer.streams_discovered")

	DeadConsumersRemoved = expvar.NewInt("consumer.dead_consumers_removed")
)
