package digger

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

// S3Consumer is a Consumer implementation that reads from one or more prefixes in an S3
// bucket.
type S3Consumer struct {
	S3Client   *s3.S3
	Bucket     string
	Prefixes   []string
	NumWorkers int
}

var _ Consumer = (*S3Consumer)(nil)

type s3ObjTask struct {
	objInfo *s3.Object
	index   int
}

// Run starts the s3 consumer. Messages are passed to the argument message channel.
func (s *S3Consumer) Run(
	ctx context.Context,
	messageChan chan message,
) error {
	objectChan := make(chan s3ObjTask, s.NumWorkers)
	errChan := make(chan error, s.NumWorkers+1)

	for i := 0; i < s.NumWorkers; i++ {
		go func() {
			errChan <- s.runSubTasks(ctx, messageChan, objectChan)
		}()
	}

	go func() {
		errChan <- s.processPrefixes(ctx, objectChan)
		close(objectChan)
	}()

	for i := 0; i < s.NumWorkers+1; i++ {
		if err := <-errChan; err != nil {
			return err
		}
	}
	return nil
}

func (s *S3Consumer) processPrefixes(
	ctx context.Context,
	objectChan chan s3ObjTask,
) error {
	keysRead := 0
	prefixesRead := 0

	for _, prefix := range s.Prefixes {
		err := s.S3Client.ListObjectsPagesWithContext(
			ctx,
			&s3.ListObjectsInput{
				Bucket: aws.String(s.Bucket),
				Prefix: aws.String(prefix),
			},
			func(output *s3.ListObjectsOutput, hasMore bool) bool {
				for _, objInfo := range output.Contents {
					subTask := s3ObjTask{
						objInfo: objInfo,
						index:   keysRead,
					}
					select {
					case objectChan <- subTask:
					case <-ctx.Done():
						return false
					}
					keysRead++
				}

				return true
			},
		)

		prefixesRead++
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *S3Consumer) runSubTasks(
	ctx context.Context,
	messageChan chan message,
	objectChan chan s3ObjTask,
) error {
	for {
		select {
		case subTask, ok := <-objectChan:
			if !ok {
				return nil
			}

			err := s.processKey(ctx, messageChan, subTask.objInfo, subTask.index)
			if err != nil {
				return fmt.Errorf(
					"Error processing key %s: %+v",
					aws.StringValue(subTask.objInfo.Key),
					err,
				)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *S3Consumer) processKey(
	ctx context.Context,
	messageChan chan message,
	objInfo *s3.Object,
	index int,
) error {
	log.Debugf("Processing key %s", aws.StringValue(objInfo.Key))

	var contentEncoding *string
	if strings.HasSuffix(aws.StringValue(objInfo.Key), ".gz") {
		// Assume gzip encoding (which might not actually be set in the object in S3)
		contentEncoding = aws.String("gzip")
	}

	obj, err := s.S3Client.GetObjectWithContext(
		ctx,
		&s3.GetObjectInput{
			Bucket:                  aws.String(s.Bucket),
			Key:                     objInfo.Key,
			ResponseContentEncoding: contentEncoding,
		},
	)
	if err != nil {
		return err
	}
	if obj.Body == nil {
		err = errors.New("Unexpected nil buffer")
		return err
	}
	defer obj.Body.Close()

	// Wrap the body in a large buffer to improve performance
	buffer := bufio.NewReaderSize(obj.Body, 10e6)

	scanner := bufio.NewScanner(buffer)
	buf := make([]byte, 4096)
	scanner.Buffer(buf, maxMessageSize)

	var offset int64

	for {
		if scanner.Scan() {
			contents := scanner.Bytes()

			// Need to do a copy since scanner can change underlying bytes when
			// next scan call is made.
			copiedContents := make([]byte, len(contents))
			copy(copiedContents, contents)

			messageChan <- message{
				msg: kafka.Message{
					Partition: index,
					Time:      aws.TimeValue(objInfo.LastModified),
					Key:       []byte(aws.StringValue(objInfo.Key)),
					Offset:    offset,
					Value:     copiedContents,
				},
			}
			offset++
		} else {
			return scanner.Err()
		}
	}
}
