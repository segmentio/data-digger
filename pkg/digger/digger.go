package digger

import (
	"context"
	"runtime"

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
	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			select {
			case <-ctx.Done():
				return
			case message := <-messageChan:
				for _, p := range d.Processors {
					if err := p.Process(ctx, message); err != nil {
						log.Warnf("Failed to process message: %v", err)
					}
				}
			}
		}()
	}
	select {
	case err := <-errChan:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
