package digger

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

const (
	kafkaReadbackoffMin time.Duration = 200 * time.Millisecond
	kafkaReadBackoffMax time.Duration = 10 * time.Second
	kafkaMaxAttempts    int           = 5
)

// KafkaConsumer is a Consumer implementation that reads messages from a Kafka topic.
type KafkaConsumer struct {
	Address    string
	Topic      string
	Partitions []kafka.Partition
	Offset     int64
	Since      time.Duration
	Until      time.Duration

	MinBytes int
	MaxBytes int
}

var _ Consumer = (*KafkaConsumer)(nil)

// Run starts the kafka consumer. Messages are passed to the argument message channel.
func (k *KafkaConsumer) Run(
	ctx context.Context,
	messageChan chan message,
) error {
	errChan := make(chan error, len(k.Partitions))

	for _, partition := range k.Partitions {
		go func(partitionID int) {
			errChan <- k.consumePartition(ctx, messageChan, partitionID)
		}(partition.ID)
	}

	for i := 0; i < len(k.Partitions); i++ {
		if err := <-errChan; err != nil {
			return err
		}
	}

	return nil
}

func (k *KafkaConsumer) consumePartition(
	ctx context.Context,
	messageChan chan message,
	partition int,
) error {
	reader, err := k.newReader(ctx, partition)
	if err != nil {
		return err
	}
	defer reader.Close()

	var stopTime time.Time
	if k.Until != 0 {
		stopTime = time.Now().Add(k.Until)
	}

	for {
		messageObj := message{}

		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				log.Warnf("Failed to read message: %v", err)
			}

			continue
		}

		if !stopTime.IsZero() && msg.Time.After(stopTime) {
			log.Warnf("Partition %d has reached until duration, stopping", partition)
			return nil
		}

		messageObj.msg = msg
		messageChan <- messageObj
	}
}

func (k *KafkaConsumer) newReader(
	ctx context.Context,
	partition int,
) (*kafka.Reader, error) {
	reader := kafka.NewReader(
		kafka.ReaderConfig{
			Brokers:        []string{k.Address},
			Topic:          k.Topic,
			Partition:      partition,
			MinBytes:       k.MinBytes,
			MaxBytes:       k.MaxBytes,
			ReadBackoffMin: kafkaReadbackoffMin,
			ReadBackoffMax: kafkaReadBackoffMax,
			MaxAttempts:    kafkaMaxAttempts,
		},
	)

	if k.Since != 0 {
		err := reader.SetOffsetAt(ctx, time.Now().Add(k.Since))
		if err != nil {
			return nil, err
		}
	} else {
		var offset int64

		if k.Offset == 0 {
			offset = kafka.LastOffset
		} else {
			offset = k.Offset
		}
		reader.SetOffset(offset)
	}

	return reader, nil
}
