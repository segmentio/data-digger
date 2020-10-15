package digger

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/briandowns/spinner"
	"github.com/gosuri/uilive"
	"github.com/segmentio/data-digger/pkg/json"
	"github.com/segmentio/data-digger/pkg/proto"
	"github.com/segmentio/data-digger/pkg/stats"
	sjson "github.com/segmentio/encoding/json"
	log "github.com/sirupsen/logrus"
)

var (
	spinnerStates = spinner.CharSets[21]
)

// Processor is an interface that can process and summarize messages.
type Processor interface {
	Process(context.Context, message) error
	Stop() error
	Summary() string
}

// LiveStatsConfig stores the inputs for a LiveStats processor.
type LiveStatsConfig struct {
	Filter       string
	K            int
	Numeric      bool
	PathsStr     string
	PrintMissing bool
	ProtoTypes   []string
	Raw          bool
	RawExtended  bool
	SortByName   bool
}

// LiveStats is a processor that calculates and displays stats based on a structured
// message stream.
type LiveStats struct {
	config       LiveStatsConfig
	decoder      *proto.Decoder
	pathGroups   [][]string
	filterRegexp *regexp.Regexp
	stopChan     chan struct{}
	wg           sync.WaitGroup

	topKCounter       *stats.TopKCounter
	messageCounter    *stats.MessageCounter
	timeBucketCounter *stats.TimeBucketCounter
}

var _ Processor = (*LiveStats)(nil)

// NewLiveStats creates a new LiveStats instance and starts the main progress printing loop.
func NewLiveStats(config LiveStatsConfig) (*LiveStats, error) {
	var filterRegexp *regexp.Regexp
	var err error

	if config.Filter != "" {
		filterRegexp, err = regexp.Compile(config.Filter)
		if err != nil {
			return nil, err
		}
	}

	decoder, err := proto.NewDecoder(config.ProtoTypes)
	if err != nil {
		return nil, err
	}

	pathGroups := [][]string{}

	if config.PathsStr != "" {
		for _, pathGroupStr := range strings.Split(config.PathsStr, ";") {
			pathGroups = append(pathGroups, strings.Split(pathGroupStr, ","))
		}
	} else {
		pathGroups = append(pathGroups, []string{""})
	}

	l := &LiveStats{
		config:       config,
		decoder:      decoder,
		filterRegexp: filterRegexp,
		pathGroups:   pathGroups,
		stopChan:     make(chan struct{}),
		wg:           sync.WaitGroup{},

		topKCounter:       stats.NewTopKCounter(config.K),
		messageCounter:    stats.NewMessageCounter(),
		timeBucketCounter: stats.NewTimeBucketCounter(250*time.Millisecond, 5*time.Second),
	}

	l.wg.Add(1)
	go l.progressLoop()

	return l, nil
}

// Process updates the stats in this LiveStats for a single message.
func (l *LiveStats) Process(ctx context.Context, messageObj message) error {
	decodedMsg, err := l.decoder.ToJSON(messageObj.msg.Value)

	if log.IsLevelEnabled(log.DebugLevel) {
		log.Debugf("Got message: ts=%s partition=%d offset=%d key=%s value=%s",
			messageObj.msg.Time.Format(time.RFC3339),
			messageObj.msg.Partition,
			messageObj.msg.Offset,
			string(messageObj.msg.Key),
			string(decodedMsg),
		)
	}

	if err != nil {
		log.Debugf("Error decoding message to JSON (%+v): %s", err, decodedMsg)
		l.topKCounter.Add(stats.InvalidValue, 1.0)
		return nil
	}

	l.timeBucketCounter.Increment(time.Now(), 1)

	if l.filterRegexp != nil && !l.filterRegexp.Match(decodedMsg) {
		l.messageCounter.Update(messageObj.msg, false)
		log.Debug("Dropping message due to filter")
		return nil
	}

	if l.config.Raw || l.config.RawExtended {
		fmt.Println(l.rawString(messageObj, decodedMsg))
	}

	l.messageCounter.Update(messageObj.msg, true)

	values := json.GJsonPathValues(decodedMsg, l.pathGroups)

	for _, value := range values {
		if l.config.Numeric {
			components := strings.Split(value, stats.DimSeparator)
			numericComponent := components[len(components)-1]

			// TODO: Create a new bucket in case that number is missing
			// but subdimensions are not?
			if numericComponent == stats.MissingValue {
				l.topKCounter.Add(stats.MissingValue, 1.0)
				continue
			}

			floatValue, err := strconv.ParseFloat(numericComponent, 64)
			if err != nil {
				log.Debugf("Invalid numeric value: %s", numericComponent)
				l.topKCounter.Add(stats.InvalidValue, 1.0)
				continue
			}

			var bucketValue string
			if len(components) == 1 {
				bucketValue = stats.AllValue
			} else {
				bucketValue = strings.Join(
					components[0:len(components)-1],
					stats.DimSeparator,
				)
			}

			l.topKCounter.Add(bucketValue, floatValue)
		} else {
			l.topKCounter.Add(value, 1.0)
		}
	}

	if len(values) == 0 {
		l.topKCounter.Add(stats.MissingValue, 1.0)
		if l.config.PrintMissing {
			log.Infof(
				"Message is missing all paths: %s",
				string(decodedMsg),
			)
		}
	}

	return nil
}

func (l *LiveStats) progressLoop() {
	var progressWriter *uilive.Writer
	var outputWriter io.Writer
	var ticker *time.Ticker

	if l.config.Raw || l.config.RawExtended {
		// In raw case, don't output anything that will mess with jq or other downstream
		// components
		outputWriter = ioutil.Discard
		ticker = time.NewTicker(100 * time.Hour)
	} else if log.IsLevelEnabled(log.DebugLevel) || l.config.PrintMissing {
		// The spinner really messes up frequent log output, so don't use it in debug mode
		ticker = time.NewTicker(2 * time.Second)
		outputWriter = os.Stderr
	} else {
		progressWriter = uilive.New()
		// A really big time so that we can control the flushing manually
		progressWriter.RefreshInterval = 5000 * time.Hour
		progressWriter.Start()

		ticker = time.NewTicker(50 * time.Millisecond)
		outputWriter = progressWriter
	}

	spinnerIndex := 0

outerLoop:
	for {
		select {
		case <-l.stopChan:
			// Print one last update
			l.printProgress(outputWriter, spinnerIndex)
			break outerLoop
		case <-ticker.C:
			l.printProgress(outputWriter, spinnerIndex)
			if progressWriter != nil {
				progressWriter.Flush()
			}

			spinnerIndex++
			spinnerIndex = spinnerIndex % len(spinnerStates)
		}
	}

	ticker.Stop()
	if progressWriter != nil {
		progressWriter.Stop()
	}
	l.wg.Done()
}

func (l *LiveStats) printProgress(outputWriter io.Writer, spinnerIndex int) {
	topKSummary := l.topKCounter.Summary()
	messageSummary := l.messageCounter.Summary()

	fmt.Fprintf(
		outputWriter,
		strings.Join(
			[]string{
				fmt.Sprintf(
					"%s Reading messages",
					spinnerStates[spinnerIndex],
				),
				fmt.Sprintf(
					"  %0.0f messages / sec",
					l.timeBucketCounter.RatePerSec(),
				),
				fmt.Sprintf(
					"  %d messages total (%d partitions/files, %s->%s)",
					messageSummary.TotalMessages,
					len(messageSummary.PartitionCounters),
					messageSummary.FirstTime.Format(time.RFC3339),
					messageSummary.LastTime.Format(time.RFC3339),
				),
				fmt.Sprintf(
					"  %d messages post-filters",
					messageSummary.PostFilterMessages,
				),
				fmt.Sprintf("  %d message values added", topKSummary.TotalAdded),
				fmt.Sprintf("  %d categories", topKSummary.NumCategories),
				fmt.Sprintf(
					"  %d messages with no value categories",
					topKSummary.TotalMissing,
				),
				fmt.Sprintf(
					"  %d messages with invalid structures",
					topKSummary.TotalInvalid,
				),
				fmt.Sprintf(
					"  %d message values dropped due to category overflow\n",
					topKSummary.TotalRemoved,
				),
			},
			"\n",
		),
	)
}

// Stop stops this LiveStats instance.
func (l *LiveStats) Stop() error {
	l.stopChan <- struct{}{}
	l.wg.Wait()
	return nil
}

// Summary returns a pretty table summary of the stats calculated by this LiveStats instance.
func (l *LiveStats) Summary() string {
	return fmt.Sprintf(
		"Top K values (approximate):\n%s",
		l.topKCounter.PrettyTable(
			len(l.pathGroups),
			l.config.Numeric,
			l.config.SortByName,
		),
	)
}

type extendedMessage struct {
	DecodedValue sjson.RawMessage `json:"decodedValue"`
	Key          string           `json:"key"`
	Offset       int64            `json:"offset"`
	Partition    int              `json:"partition"`
	Time         time.Time        `json:"time"`
}

func (l *LiveStats) rawString(messageObj message, decodedBytes []byte) string {
	if l.config.RawExtended {
		output, err := sjson.Marshal(
			extendedMessage{
				DecodedValue: sjson.RawMessage(decodedBytes),
				Key:          string(messageObj.msg.Key),
				Offset:       messageObj.msg.Offset,
				Partition:    messageObj.msg.Partition,
				Time:         messageObj.msg.Time,
			},
		)
		if err != nil {
			log.Warnf("Error marshalling JSON: %+v", err)
			return ""
		}

		return string(output)
	}

	return string(decodedBytes)
}
