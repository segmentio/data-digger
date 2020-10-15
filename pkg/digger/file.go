package digger

import (
	"bufio"
	"compress/gzip"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

// FileConsumer is a Consumer implementation that reads from local files.
type FileConsumer struct {
	Paths     []string
	Recursive bool
}

var _ Consumer = (*FileConsumer)(nil)

type fileSubTask struct {
	fileInfo    os.FileInfo
	messageChan chan message
	index       int
}

// Run starts the file consumer. Messages are passed to the argument message channel.
func (f *FileConsumer) Run(
	ctx context.Context,
	messageChan chan message,
) error {
	numFiles := 0

	for _, path := range f.Paths {
		fileInfo, err := os.Stat(path)
		if err != nil {
			return err
		}
		if fileInfo.IsDir() {
			err = filepath.Walk(
				path,
				func(subPath string, subInfo os.FileInfo, err error) error {
					if err != nil {
						return err
					}

					if !subInfo.IsDir() {
						err := f.processFile(ctx, messageChan, subPath, fileInfo, numFiles)
						if err != nil {
							return err
						}

						numFiles++
					} else if !f.Recursive && subPath != path {
						return filepath.SkipDir
					}

					return nil
				},
			)
			if err != nil {
				return err
			}
		} else {
			err := f.processFile(ctx, messageChan, path, fileInfo, numFiles)
			if err != nil {
				return err
			}

			numFiles++
		}
	}

	return nil
}

func (f *FileConsumer) processFile(
	ctx context.Context,
	messageChan chan message,
	filePath string,
	fileInfo os.FileInfo,
	index int,
) error {
	log.Debugf("Processing file %s", filePath)

	inputFile, err := os.Open(filePath)
	if err != nil {
		log.Infof("Error opening file: %+v", err)
		return err
	}
	defer inputFile.Close()

	var scanReader io.Reader

	if strings.HasSuffix(filePath, ".gz") {
		gzipReader, err := gzip.NewReader(inputFile)
		if err != nil {
			return err
		}
		defer gzipReader.Close()
		scanReader = gzipReader
	} else {
		scanReader = inputFile
	}

	scanner := bufio.NewScanner(scanReader)
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
					Time:      fileInfo.ModTime(),
					Key:       []byte(filePath),
					Offset:    offset,
					Value:     copiedContents,
				},
			}
			offset++

			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		} else {
			return scanner.Err()
		}
	}
}
