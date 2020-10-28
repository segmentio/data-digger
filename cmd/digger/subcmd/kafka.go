package subcmd

import (
	"context"
	"strings"
	"time"

	"github.com/segmentio/cli"
	dig "github.com/segmentio/data-digger/pkg/digger"
	"github.com/segmentio/data-digger/pkg/util"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

type kafkaConfig struct {
	commonConfig

	Address    string `flag:"-a,--address"    help:"kafka address"`
	Offset     int64  `flag:"-o,--offset"     help:"kafka offset" default:"-1"`
	Partitions string `flag:"-p,--partitions" help:"comma-separated list of partitions" default:"-"`
	ProtoTypes string `flag:"--proto-types"   help:"comma-separated list of registered proto types" default:"-"`
	SinceStr   string `flag:"--since"         help:"time to start at; can be either RFC3339 timestamp or duration relative to now" default:"-"`
	Topic      string `flag:"-t,--topic"      help:"kafka topic"`
	UntilStr   string `flag:"--until"         help:"time to end at; can be either RFC3339 timestamp or duration relative to now" default:"-"`
}

func KafkaCmd(ctx context.Context) cli.Function {
	return cli.Command(
		func(config kafkaConfig) {
			if config.Debug {
				log.SetLevel(log.DebugLevel)
			} else {
				log.SetLevel(log.InfoLevel)
			}
			err := loadPlugins(config.Plugins)
			if err != nil {
				log.Fatalf("Could not load plugins: %+v", err)
			}

			now := time.Now().UTC()
			since, err := util.ParseTimeOrDuration(config.SinceStr, now)
			if err != nil {
				log.Fatalf("Error parsing since flag: %+v", err)
			}
			until, err := util.ParseTimeOrDuration(config.UntilStr, now)
			if err != nil {
				log.Fatalf("Error parsing until flag: %+v", err)
			}

			if !since.IsZero() && since.After(now) {
				log.Fatalf("Since must be in past")
			}
			if !since.IsZero() && !until.IsZero() && since.After(until) {
				log.Fatalf("Until must be after since")
			}

			partitions, total, err := readKafkaPartitions(
				config.Address,
				config.Topic,
				config.Partitions,
			)
			if err != nil {
				log.Fatalf(
					"Failed to read partitions for %s: %v",
					config.Address,
					err,
				)
			}

			if !config.Raw {
				log.Infof(
					"Reading from %d partitions (out of %d total)",
					len(partitions),
					total,
				)
			}

			processors, err := makeProcessors(
				config.commonConfig,
				strings.Split(config.ProtoTypes, ","),
			)
			if err != nil {
				log.Fatalf("Error creating processors: %+v", err)
			}

			digger := &dig.Digger{
				SourceConsumer: &dig.KafkaConsumer{
					Address:    config.Address,
					Topic:      config.Topic,
					Offset:     config.Offset,
					Since:      since,
					Until:      until,
					Partitions: partitions,
					MinBytes:   10e3,
					MaxBytes:   10e6,
				},
				Processors: processors,
			}

			if !config.Raw {
				log.Infof("Starting digger; press control-c to stop and print out summary")
			}

			if err := digger.Run(ctx); err != nil && ctx.Err() == nil {
				log.Fatalf("Error running digger: %v", err)
			}

			for _, processor := range processors {
				processor.Stop()
			}

			for _, processor := range processors {
				log.Infof("Processor summary:\n%s", processor.Summary())
			}
		},
	)
}

func readKafkaPartitions(
	address, topic, partitions string,
) ([]kafka.Partition, int, error) {
	conn, err := kafka.Dial("tcp", address)
	if err != nil {
		return nil, 0, err
	}

	log.Debugf("Fetching partitions for %s from %s", topic, address)

	availablePartitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return nil, 0, err
	}

	if partitions == "" {
		return availablePartitions, len(availablePartitions), nil
	}

	partitionIDs, err := util.ParseRangeStr(partitions)
	if err != nil {
		return nil, 0, err
	}

	requestedPartitions := []kafka.Partition{}

	for _, partition := range availablePartitions {
		if _, ok := partitionIDs[partition.ID]; ok {
			requestedPartitions = append(requestedPartitions, partition)
		}
	}

	return requestedPartitions, len(availablePartitions), nil
}
