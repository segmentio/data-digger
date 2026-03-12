package digger

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS3Consumer(t *testing.T) {
	ctx := context.Background()

	var s3Endpoint string

	// In CI, need to use a non-localhost address; get this from environment.
	if _, ok := os.LookupEnv("DIGGER_TEST_S3_ADDR"); ok {
		s3Endpoint = os.Getenv("DIGGER_TEST_S3_ADDR")
	} else {
		s3Endpoint = "http://localhost:4572"
	}

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(
			// These need to be set, but they can be anything since localstack
			// doesn't do any checking
			credentials.NewStaticCredentialsProvider("test", "test", "test"),
		),
		config.WithRegion("us-west-2"),
		config.WithEndpointResolverWithOptions(
			aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL: s3Endpoint,
					HostnameImmutable: true,
					SigningRegion: "us-west-2",
				}, nil
			}),
		),
	)

	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

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
	err = consumer.Run(ctx, messageChan)
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

func createBucket(ctx context.Context, t *testing.T, s3Client *s3.Client) string {
	bucketName := fmt.Sprintf("test-bucket-%d", time.Now().UnixNano())

	_, err := s3Client.CreateBucket(
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
	s3Client *s3.Client,
	bucket string,
	key string,
	value string,
) {
	body := bytes.NewBufferString(value)

	_, err := s3Client.PutObject(
		ctx,
		&s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   body,
		},
	)
	require.NoError(t, err)
}
