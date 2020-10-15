package digger

import (
	"context"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLiveStatsSingleDimension(t *testing.T) {
	ctx := context.Background()

	liveStats, err := NewLiveStats(
		LiveStatsConfig{
			Filter:   "filter-value",
			K:        10,
			PathsStr: "body.id,body.name",
		},
	)
	require.Nil(t, err)

	kafkaMessages := []kafka.Message{
		{
			Value: []byte(`
				{
					"body": {
						"id": "id1"
					},
					"key": "filter-value"
				}
			`),
		},
		{
			Value: []byte(`
				{
					"body": {
						"id": "id1"
					},
					"key": "filter-value"
				}
			`),
		},
		{
			Value: []byte(`
				{
					"body": {
						"id": "id1"
					},
					"key": "non-matching-value"
				}
			`),
		},
		{
			Value: []byte(`
				{
					"body": {
						"name": "testname"
					},
					"key": "filter-value"
				}
			`),
		},
		{
			Value: []byte(`
				{
					"body": {
						"name": ["testname", "othername"]
					},
					"key": "filter-value"
				}
			`),
		},
		{
			Value: []byte(`
				{
					"body": {},
					"key": "filter-value"
				}
			`),
		},
	}

	for _, kafkaMessage := range kafkaMessages {
		err := liveStats.Process(ctx, message{kafkaMessage})
		require.NoError(t, err)
	}

	err = liveStats.Stop()
	require.NoError(t, err)

	buckets := liveStats.topKCounter.Buckets(4, false)
	require.Equal(t, 4, len(buckets))

	assert.Equal(t, "id1", buckets[0].Key)
	assert.Equal(t, 2, buckets[0].Count)
	assert.Equal(t, "testname", buckets[1].Key)
	assert.Equal(t, 2, buckets[1].Count)
	assert.Equal(t, "__missing__", buckets[2].Key)
	assert.Equal(t, 1, buckets[2].Count)
	assert.Equal(t, "othername", buckets[3].Key)
	assert.Equal(t, 1, buckets[3].Count)
}

func TestLiveStatsSingleDimensionNumeric(t *testing.T) {
	ctx := context.Background()

	liveStats, err := NewLiveStats(
		LiveStatsConfig{
			K:        10,
			Numeric:  true,
			PathsStr: "body.value",
		},
	)
	require.Nil(t, err)

	kafkaMessages := []kafka.Message{
		{
			Value: []byte(`
				{
					"body": {
						"value": 1
					}
				}
			`),
		},
		{
			Value: []byte(`
				{
					"body": {
						"value": 2
					}
				}
			`),
		},
		{
			Value: []byte(`
				{
					"body": {
						"value": 3
					}
				}
			`),
		},
		{
			Value: []byte(`
				{
					"body": {
						"value": 4
					}
				}
			`),
		},
		{
			Value: []byte(`
				{
					"body": {
						"value": "not a number"
					}
				}
			`),
		},
		{
			Value: []byte(`
				{
					"body": {}
				}
			`),
		},
	}

	for _, kafkaMessage := range kafkaMessages {
		err := liveStats.Process(ctx, message{kafkaMessage})
		require.NoError(t, err)
	}

	err = liveStats.Stop()
	require.NoError(t, err)

	buckets := liveStats.topKCounter.Buckets(4, false)
	require.Equal(t, 3, len(buckets))

	assert.Equal(t, "__all__", buckets[0].Key)
	assert.Equal(t, 4, buckets[0].Count)
	assert.Equal(t, 10.0, buckets[0].Sum)
	assert.Equal(t, 1.0, buckets[0].Min)
	assert.Equal(t, 4.0, buckets[0].Max)
	assert.Equal(t, "__invalid__", buckets[1].Key)
	assert.Equal(t, 1, buckets[1].Count)
	assert.Equal(t, "__missing__", buckets[2].Key)
	assert.Equal(t, 1, buckets[2].Count)
}

func TestLiveStatsMultiDimension(t *testing.T) {
	ctx := context.Background()

	liveStats, err := NewLiveStats(
		LiveStatsConfig{
			K:        10,
			PathsStr: "body.id,body.altId;body.name",
		},
	)
	require.Nil(t, err)

	kafkaMessages := []kafka.Message{
		{
			Value: []byte(`
				{
					"body": {
						"id": "id1",
						"name": "name1"
					},
					"key": "filter-value"
				}
			`),
		},
		{
			Value: []byte(`
				{
					"body": {
						"altId": "id1",
						"name": "name1"
					},
					"key": "filter-value"
				}
			`),
		},
		{
			Value: []byte(`
				{
					"body": {
						"id": "id1",
						"name": "name2"
					},
					"key": "filter-value"
				}
			`),
		},
		{
			Value: []byte(`
				{
					"body": {
						"id": "id2",
						"name": "name1"
					},
					"key": "non-matching-value"
				}
			`),
		},
		{
			Value: []byte(`
				{
					"body": {
						"name": "name3"
					},
					"key": "filter-value"
				}
			`),
		},
	}

	for _, kafkaMessage := range kafkaMessages {
		err := liveStats.Process(ctx, message{kafkaMessage})
		require.NoError(t, err)
	}

	err = liveStats.Stop()
	require.NoError(t, err)

	buckets := liveStats.topKCounter.Buckets(4, false)
	require.Equal(t, 4, len(buckets))

	assert.Equal(t, "id1∪∪name1", buckets[0].Key)
	assert.Equal(t, 2, buckets[0].Count)
	assert.Equal(t, "__missing__∪∪name3", buckets[1].Key)
	assert.Equal(t, 1, buckets[1].Count)
	assert.Equal(t, "id1∪∪name2", buckets[2].Key)
	assert.Equal(t, 1, buckets[2].Count)
	assert.Equal(t, "id2∪∪name1", buckets[3].Key)
	assert.Equal(t, 1, buckets[3].Count)
}

func TestLiveStatsMultiDimensionNumeric(t *testing.T) {
	ctx := context.Background()

	liveStats, err := NewLiveStats(
		LiveStatsConfig{
			K:        10,
			Numeric:  true,
			PathsStr: "body.id;body.value",
		},
	)
	require.Nil(t, err)

	kafkaMessages := []kafka.Message{
		{
			Value: []byte(`
				{
					"body": {
						"id": "id1",
						"value": 1
					}
				}
			`),
		},
		{
			Value: []byte(`
				{
					"body": {
						"id": "id1",
						"value": 2
					}
				}
			`),
		},
		{
			Value: []byte(`
				{
					"body": {
						"id": "id2",
						"value": 3
					}
				}
			`),
		},
		{
			Value: []byte(`
				{
					"body": {
						"id": "id2"
					}
				}
			`),
		},
		{
			Value: []byte(`
				{
					"body": {
						"id": "id3",
						"value": "not a number"
					}
				}
			`),
		},
	}

	for _, kafkaMessage := range kafkaMessages {
		err := liveStats.Process(ctx, message{kafkaMessage})
		require.NoError(t, err)
	}

	err = liveStats.Stop()
	require.NoError(t, err)

	buckets := liveStats.topKCounter.Buckets(4, false)
	require.Equal(t, 4, len(buckets))

	assert.Equal(t, "id1", buckets[0].Key)
	assert.Equal(t, 2, buckets[0].Count)
	assert.Equal(t, 1.0, buckets[0].Min)
	assert.Equal(t, 2.0, buckets[0].Max)
	assert.Equal(t, 3.0, buckets[0].Sum)
	assert.Equal(t, "__invalid__", buckets[1].Key)
	assert.Equal(t, 1, buckets[1].Count)
	assert.Equal(t, "__missing__", buckets[2].Key)
	assert.Equal(t, 1, buckets[2].Count)
	assert.Equal(t, "id2", buckets[3].Key)
	assert.Equal(t, 1, buckets[3].Count)
	assert.Equal(t, 3.0, buckets[3].Min)
	assert.Equal(t, 3.0, buckets[3].Max)
	assert.Equal(t, 3.0, buckets[3].Sum)
}
