package stats

import (
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// MessageCounter is a type that stores counts by partition (or file or S3 key for non-Kafka
// sources). It's used by the digger stats progress view.
type MessageCounter struct {
	sync.Mutex

	totalMessages      int64
	postFilterMessages int64
	partitionCounters  map[int]*PartitionCounter
}

// MessageCounterSummary stores a summary of the message counts seen so far.
type MessageCounterSummary struct {
	TotalMessages      int64
	PostFilterMessages int64
	FirstTime          time.Time
	LastTime           time.Time
	PartitionCounters  map[int]PartitionCounter
}

// PartitionCounter stores detailed stats about the messages seen so far in a specific
// partition (or file or S3 key).
type PartitionCounter struct {
	PartitionID        int
	TotalMessages      int64
	PostFilterMessages int64
	FirstOffset        int64
	LastOffset         int64
	FirstTime          time.Time
	LastTime           time.Time
}

// NewMessageCounter returns a new MessageCounter instance.
func NewMessageCounter() *MessageCounter {
	return &MessageCounter{
		partitionCounters: map[int]*PartitionCounter{},
	}
}

// Update updates the counter for the provided message.
func (m *MessageCounter) Update(msg kafka.Message, postFilter bool) {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	m.totalMessages++

	if postFilter {
		m.postFilterMessages++
	}

	counter, ok := m.partitionCounters[msg.Partition]
	if !ok {
		counter = &PartitionCounter{
			PartitionID:   msg.Partition,
			TotalMessages: 1,
			FirstOffset:   msg.Offset,
			LastOffset:    msg.Offset,
			FirstTime:     msg.Time,
			LastTime:      msg.Time,
		}
		if postFilter {
			counter.PostFilterMessages = 1
		}
		m.partitionCounters[msg.Partition] = counter
		return
	}

	counter.TotalMessages++
	if postFilter {
		counter.PostFilterMessages++
	}

	if msg.Offset < counter.FirstOffset {
		counter.FirstOffset = msg.Offset
	}
	if msg.Offset > counter.LastOffset {
		counter.LastOffset = msg.Offset
	}
	if counter.FirstTime.IsZero() || msg.Time.Before(counter.FirstTime) {
		counter.FirstTime = msg.Time
	}
	if counter.LastTime.IsZero() || msg.Time.After(counter.LastTime) {
		counter.LastTime = msg.Time
	}
}

// Summary returns a MessageCounterSummary instance based on the stats recorded
// thus far for this counter.
func (m *MessageCounter) Summary() MessageCounterSummary {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	summary := MessageCounterSummary{
		TotalMessages:      m.totalMessages,
		PostFilterMessages: m.postFilterMessages,
		PartitionCounters:  map[int]PartitionCounter{},
	}

	for partition, partitionCounter := range m.partitionCounters {
		summary.PartitionCounters[partition] = *partitionCounter

		if summary.FirstTime.IsZero() || partitionCounter.FirstTime.Before(summary.FirstTime) {
			summary.FirstTime = partitionCounter.FirstTime
		}
		if summary.LastTime.IsZero() || partitionCounter.LastTime.After(summary.LastTime) {
			summary.LastTime = partitionCounter.LastTime
		}
	}

	return summary
}
