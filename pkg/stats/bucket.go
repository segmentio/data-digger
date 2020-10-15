package stats

import (
	"sync"
	"time"
)

// TimeBucketCounter is a counter that records the approximate number of events over a
// recent interval; the length of this interval and the resolution are configurable. Used
// to measure the approximate processing rate for the digger.
type TimeBucketCounter struct {
	sync.Mutex

	headIncrement int64
	resolution    time.Duration
	length        time.Duration
	maxSize       int
	buckets       []int64
	currTotal     int64
}

// NewTimeBucketCounter creates a new TimeBucketCounter instance for the given resolution and
// length.
func NewTimeBucketCounter(
	resolution time.Duration,
	length time.Duration,
) *TimeBucketCounter {
	return &TimeBucketCounter{
		resolution: resolution,
		length:     length,
		maxSize:    int(length / resolution),
		buckets:    []int64{},
	}
}

// Increment updates the counter for the argument count, assuming that the current time is now.
func (t *TimeBucketCounter) Increment(now time.Time, count int64) {
	t.Lock()
	defer t.Unlock()

	if t.headIncrement == 0 {
		t.headIncrement = now.UnixNano() / int64(t.resolution)
		t.buckets = []int64{count}
		t.currTotal = count
		return
	}

	t.advance(now)
	t.buckets[0] += count
	t.currTotal += count
}

// Total gets the total count for this counter.
func (t *TimeBucketCounter) Total() int64 {
	t.Lock()
	defer t.Unlock()

	return t.currTotal
}

// RatePerSec returns the average count per second for this counter.
func (t *TimeBucketCounter) RatePerSec() float64 {
	t.Lock()
	defer t.Unlock()

	return float64(t.currTotal) / t.length.Seconds()
}

func (t *TimeBucketCounter) advance(now time.Time) {
	newHead := now.UnixNano() / int64(t.resolution)
	if newHead == t.headIncrement {
		// Nothing to do
		return
	}

	t.buckets = append(
		make([]int64, newHead-t.headIncrement, newHead-t.headIncrement),
		t.buckets...,
	)

	if len(t.buckets) > t.maxSize {
		for i := t.maxSize; i < len(t.buckets); i++ {
			t.currTotal -= t.buckets[i]
		}

		t.buckets = t.buckets[0:t.maxSize]
	}

	t.headIncrement = newHead
}
