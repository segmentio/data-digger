package stats

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimeBucketCounter(t *testing.T) {
	counter := NewTimeBucketCounter(time.Second, time.Minute)
	now := time.Date(2020, 10, 5, 3, 11, 0, 0, time.UTC)

	counter.Increment(now, 10)
	now = now.Add(10 * time.Second)
	counter.Increment(now, 5)
	assert.Equal(t, int64(15), counter.Total())

	now = now.Add(50 * time.Second)
	counter.Increment(now, 20)
	assert.Equal(t, int64(25), counter.Total())

	now = now.Add(15 * time.Second)
	counter.Increment(now, 100)
	assert.Equal(t, int64(120), counter.Total())

	now = now.Add(59 * time.Second)
	counter.Increment(now, 0)
	assert.Equal(t, int64(100), counter.Total())

	now = now.Add(time.Second)
	counter.Increment(now, 0)
	assert.Equal(t, int64(0), counter.Total())
}
