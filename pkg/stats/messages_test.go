package stats

import (
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

func TestMessageCounter(t *testing.T) {
	counter := NewMessageCounter()

	counter.Update(
		kafka.Message{
			Partition: 2,
			Offset:    123,
			Time:      time.Unix(900, 0),
		},
		true,
	)
	counter.Update(
		kafka.Message{
			Partition: 3,
			Offset:    1234,
			Time:      time.Unix(1000, 0),
		},
		true,
	)
	counter.Update(
		kafka.Message{
			Partition: 3,
			Offset:    1235,
			Time:      time.Unix(1100, 0),
		},
		false,
	)

	summary := counter.Summary()
	assert.Equal(t, int64(3), summary.TotalMessages)
	assert.Equal(t, int64(2), summary.PostFilterMessages)
	assert.Equal(t, time.Unix(900, 0), summary.FirstTime)
	assert.Equal(t, time.Unix(1100, 0), summary.LastTime)

	part3Counter := summary.PartitionCounters[3]
	assert.Equal(
		t,
		PartitionCounter{
			PartitionID:        3,
			TotalMessages:      2,
			PostFilterMessages: 1,
			FirstOffset:        1234,
			LastOffset:         1235,
			FirstTime:          time.Unix(1000, 0),
			LastTime:           time.Unix(1100, 0),
		},
		part3Counter,
	)
}
