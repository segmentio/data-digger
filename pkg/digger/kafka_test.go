package digger

import (
	"context"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testKafkaAddr string

func init() {
	// In CI, need to use a non-localhost address; get this from environment.
	if _, ok := os.LookupEnv("DIGGER_TEST_KAFKA_ADDR"); ok {
		testKafkaAddr = os.Getenv("DIGGER_TEST_KAFKA_ADDR")
	} else {
		testKafkaAddr = "localhost:9092"
	}
}

func TestKafkaConsumer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	topicName := createTestTopic(ctx, t)

	consumer := KafkaConsumer{
		Address: testKafkaAddr,
		Topic:   topicName,
		Partitions: []kafka.Partition{
			{
				ID: 0,
			},
		},
		MinBytes: 0,
		MaxBytes: 100,
	}

	messageChan := make(chan message, 10)
	errChan := make(chan error, 1)

	go func() {
		errChan <- consumer.Run(ctx, messageChan)
	}()

	time.Sleep(100 * time.Millisecond)
	writer := kafka.NewWriter(
		kafka.WriterConfig{
			Brokers:   []string{testKafkaAddr},
			Topic:     topicName,
			BatchSize: 1,
		},
	)

	messages := []kafka.Message{}
	for i := 0; i < 10; i++ {
		messages = append(
			messages,
			kafka.Message{
				Key:   []byte(fmt.Sprintf("%02d", i)),
				Value: []byte(fmt.Sprintf("value-%d", i)),
			},
		)
	}

	err := writer.WriteMessages(ctx, messages...)
	require.NoError(t, err)

	received := []kafka.Message{}

outerLoop:
	for {
		select {
		case msg := <-messageChan:
			received = append(received, msg.msg)
			if len(received) == 10 {
				break outerLoop
			}
		case err = <-errChan:
			assert.Contains(t, err, "context")
			break outerLoop
		}
	}

	require.Equal(t, 10, len(received))
	sort.Slice(received, func(a, b int) bool {
		return string(received[a].Key) < string(received[b].Key)
	})

	assert.Equal(t, "00", string(received[0].Key))
	assert.Equal(t, "value-0", string(received[0].Value))
}

func createTestTopic(ctx context.Context, t *testing.T) string {
	topicName := fmt.Sprintf("test-topic-%d", time.Now().UnixNano())

	conn, err := kafka.DialLeader(
		ctx,
		"tcp",
		testKafkaAddr,
		topicName,
		0,
	)
	require.NoError(t, err)

	defer conn.Close()
	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topicName,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}

	err = conn.CreateTopics(topicConfigs...)
	require.NoError(t, err)

	return topicName
}
