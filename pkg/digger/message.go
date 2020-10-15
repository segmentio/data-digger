package digger

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type message struct {
	// For now, just wraps a kafka message. In the future, we might expand this and/or replace
	// the underlying Kafka message with something else.
	msg kafka.Message
}

// Consumer is an interface for types that consume messages from a source and feed them
// into a channel for downstream processing.
type Consumer interface {
	Run(ctx context.Context, messageChan chan message) error
}
