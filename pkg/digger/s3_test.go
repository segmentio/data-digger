package digger

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS3Consumer(t *testing.T) {
	ctx := context.Background()
	sess := session.Must(session.NewSession())

	var s3Endpoint string

	// In CI, need to use a non-localhost address; get this from environment.
	if _, ok := os.LookupEnv("DIGGER_TEST_S3_ADDR"); ok {
		s3Endpoint = os.Getenv("DIGGER_TEST_S3_ADDR")
	} else {
		s3Endpoint = "http://localhost:4572"
	}

	s3Client := s3.New(
		sess,
		&aws.Config{
			// These need to be set, but they can be anything since localstack
			// doesn't do any checking
			Credentials: credentials.NewStaticCredentials("test", "test", "test"),

			Endpoint:         aws.String(s3Endpoint),
			Region:           aws.String("us-west-2"),
			DisableSSL:       aws.Bool(true),
			S3ForcePathStyle: aws.Bool(true),
		},
	)

	testBucket := createBucket(ctx, t, s3Client)

	time.Sleep(100 * time.Millisecond)
	writeKey(ctx, t, s3Client, testBucket, "test-prefix1/key1", "value1\nvalue2")
	writeKey(ctx, t, s3Client, testBucket, "test-prefix1/key2", "value3\nvalue4")
	writeKey(ctx, t, s3Client, testBucket, "test-prefix2/key3", "value5")

	messageChan := make(chan message, 5)
	consumer := S3Consumer{
		S3Client:   s3Client,
		Bucket:     testBucket,
		Prefixes:   []string{"test-prefix1", "test-prefix2"},
		NumWorkers: 1,
	}
	err := consumer.Run(ctx, messageChan)
	require.NoError(t, err)

	require.Equal(t, 5, len(messageChan))
	message1 := <-messageChan
	message2 := <-messageChan

	assert.Equal(t, 0, message1.msg.Partition)
	assert.Equal(t, 0, message2.msg.Partition)
	assert.Equal(t, int64(0), message1.msg.Offset)
	assert.Equal(t, int64(1), message2.msg.Offset)
	assert.Equal(t, []byte("test-prefix1/key1"), message1.msg.Key)
	assert.Equal(t, []byte("test-prefix1/key1"), message2.msg.Key)
	assert.Equal(t, []byte("value1"), message1.msg.Value)
	assert.Equal(t, []byte("value2"), message2.msg.Value)
}

func createBucket(ctx context.Context, t *testing.T, s3Client *s3.S3) string {
	bucketName := fmt.Sprintf("test-bucket-%d", time.Now().UnixNano())

	_, err := s3Client.CreateBucketWithContext(
		ctx,
		&s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
		},
	)
	require.NoError(t, err)
	return bucketName
}

func writeKey(
	ctx context.Context,
	t *testing.T,
	s3Client *s3.S3,
	bucket string,
	key string,
	value string,
) {
	body := bytes.NewBufferString(value)

	_, err := s3Client.PutObjectWithContext(
		ctx,
		&s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   aws.ReadSeekCloser(body),
		},
	)
	require.NoError(t, err)
}
