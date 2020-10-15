package digger

import (
	"context"

	log "github.com/sirupsen/logrus"
)

const (
	// Maximum message size to set for scanners
	maxMessageSize = 512 * 1024
)

// Digger is a struct that digs through JSON or proto formatted message streams.
type Digger struct {
	SourceConsumer Consumer
	Processors     []Processor
}

// Run runs the digger with the provided context. The function returns when all data has been
// consumed, a fatal error is encountered, or the context is cancelled.
func (d *Digger) Run(ctx context.Context) error {
	messageChan := make(chan message)
	errChan := make(chan error)

	go func() {
		errChan <- d.SourceConsumer.Run(ctx, messageChan)
	}()

	for {
		select {
		case err := <-errChan:
			return err
		case message := <-messageChan:
			select {
			// If the context is done, don't process subsequent messages
			case <-ctx.Done():
				continue
			default:
			}

			for _, p := range d.Processors {
				if err := p.Process(ctx, message); err != nil {
					log.Warnf("Failed to process message: %v", err)
				}
			}
		}
	}
}
