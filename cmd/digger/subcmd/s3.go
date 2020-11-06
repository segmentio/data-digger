package subcmd

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/segmentio/cli"
	dig "github.com/segmentio/data-digger/pkg/digger"
	log "github.com/sirupsen/logrus"
)

type s3Config struct {
	commonConfig

	Bucket     string `flag:"-b,--bucket"   help:"s3 bucket"`
	NumWorkers int    `flag:"--num-workers" help:"number of objects to read in parallel" default:"4"`
	Prefixes   string `flag:"-p,--prefixes" help:"comma-separated list of prefixes"`
}

// S3Cmd defines a CLI function for digging through S3 objects.
func S3Cmd(ctx context.Context) cli.Function {
	return cli.Command(
		func(config s3Config) {
			if config.Debug {
				log.SetLevel(log.DebugLevel)
			} else {
				log.SetLevel(log.InfoLevel)
			}
			err := loadPlugins(config.Plugins)
			if err != nil {
				log.Fatalf("Could not load plugins: %+v", err)
			}

			processors, err := makeProcessors(config.commonConfig, []string{})
			if err != nil {
				log.Fatalf("Error creating processors: %+v", err)
			}

			sess := session.Must(session.NewSession())
			s3Client := s3.New(sess)

			digger := &dig.Digger{
				SourceConsumer: &dig.S3Consumer{
					S3Client:   s3Client,
					Bucket:     config.Bucket,
					NumWorkers: config.NumWorkers,
					Prefixes:   strings.Split(config.Prefixes, ","),
				},
				Processors: processors,
			}

			if !config.Raw {
				log.Infof(
					"Starting digger; press control-c to stop and print out summary",
				)
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
