package digger

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileConsumerRecursive(t *testing.T) {
	ctx := context.Background()

	consumer := FileConsumer{
		Paths: []string{
			"testdata/files",
		},
		Recursive: true,
	}
	messageChan := make(chan message, 50)
	err := consumer.Run(ctx, messageChan)
	require.NoError(t, err)

	require.Equal(t, len(messageChan), 7)
	message1 := <-messageChan
	message2 := <-messageChan

	assert.Equal(t, 0, message1.msg.Partition)
	assert.Equal(t, 0, message2.msg.Partition)
	assert.Equal(t, int64(0), message1.msg.Offset)
	assert.Equal(t, int64(1), message2.msg.Offset)
	assert.Equal(t, []byte("testdata/files/file1.txt"), message1.msg.Key)
	assert.Equal(t, []byte("testdata/files/file1.txt"), message2.msg.Key)
	assert.Equal(t, []byte(`{"key1":"value1"}`), message1.msg.Value)
	assert.Equal(t, []byte(`{"key1":"value2"}`), message2.msg.Value)
}

func TestFileConsumerNonRecursive(t *testing.T) {
	ctx := context.Background()

	consumer := FileConsumer{
		Paths: []string{
			"testdata/files/subdir/file3.txt",
			"testdata/files",
		},
		Recursive: false,
	}
	messageChan := make(chan message, 50)
	err := consumer.Run(ctx, messageChan)
	require.NoError(t, err)

	require.Equal(t, len(messageChan), 4)
	message1 := <-messageChan
	message2 := <-messageChan

	assert.Equal(t, 0, message1.msg.Partition)
	assert.Equal(t, 1, message2.msg.Partition)
	assert.Equal(t, int64(0), message1.msg.Offset)
	assert.Equal(t, int64(0), message2.msg.Offset)
	assert.Equal(t, []byte("testdata/files/subdir/file3.txt"), message1.msg.Key)
	assert.Equal(t, []byte("testdata/files/file1.txt"), message2.msg.Key)
	assert.Equal(t, []byte(`{"key3":"value3"}`), message1.msg.Value)
	assert.Equal(t, []byte(`{"key1":"value1"}`), message2.msg.Value)
}
