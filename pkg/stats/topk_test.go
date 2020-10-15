package stats

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTopKCounter(t *testing.T) {
	counter := NewTopKCounter(4)

	for i := 0; i < 20; i++ {
		counter.Add("a", 1.0)
		counter.Add("b", 1.0)
		counter.Add("c", 1.0)
	}

	for i := 0; i < 400; i++ {
		counter.Add("d", 1.0)
		counter.Add("e", 1.0)
	}

	for i := 0; i < 100; i++ {
		counter.Add("d", 2.0)
		counter.Add("e", 2.0)
	}

	for i := 0; i < 1000; i++ {
		counter.Add("f", 2.0)
	}

	counter.Clean(4)

	buckets := counter.Buckets(4, false)
	for i := 0; i < len(buckets); i++ {
		// Index is an internal implementation detail, don't check it
		buckets[i].Index = 0
	}

	assert.Equal(
		t,
		[]Bucket{
			{
				Key:   "f",
				Count: 1000,
				Min:   2.0,
				Max:   2.0,
				Sum:   2000.0,
			},
			{
				Key:   "d",
				Count: 500,
				Min:   1.0,
				Max:   2.0,
				Sum:   600.0,
			},
			{
				Key:   "e",
				Count: 500,
				Min:   1.0,
				Max:   2.0,
				Sum:   600.0,
			},
			{
				Key:   "a",
				Count: 20,
				Min:   1.0,
				Max:   1.0,
				Sum:   20.0,
			},
		},
		buckets,
	)

	bucketsByName := counter.Buckets(4, true)
	for i := 0; i < len(bucketsByName); i++ {
		// Index is an internal implementation detail, don't check it
		bucketsByName[i].Index = 0
	}

	assert.Equal(
		t,
		[]Bucket{
			{
				Key:   "a",
				Count: 20,
				Min:   1.0,
				Max:   1.0,
				Sum:   20.0,
			},
			{
				Key:   "d",
				Count: 500,
				Min:   1.0,
				Max:   2.0,
				Sum:   600.0,
			},
			{
				Key:   "e",
				Count: 500,
				Min:   1.0,
				Max:   2.0,
				Sum:   600.0,
			},
			{
				Key:   "f",
				Count: 1000,
				Min:   2.0,
				Max:   2.0,
				Sum:   2000.0,
			},
		},
		bucketsByName,
	)
}
