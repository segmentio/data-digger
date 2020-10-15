package main

import (
	"context"
	"strings"

	"github.com/segmentio/cli"
	dig "github.com/segmentio/data-digger/pkg/digger"
	log "github.com/sirupsen/logrus"
)

type fileConfig struct {
	commonConfig

	FilePaths string `flag:"--file-paths" help:"comma-separated list of file paths"`
	Recursive bool   `flag:"--recursive"  help:"scan subdirectories recursively" default:"false"`
}

func fileCmd(ctx context.Context) cli.Function {
	return cli.Command(
		func(config fileConfig) {
			if config.Debug {
				log.SetLevel(log.DebugLevel)
			} else {
				log.SetLevel(log.InfoLevel)
			}
			loadPlugins(config.Plugins)

			processors, err := makeProcessors(config.commonConfig, []string{})
			if err != nil {
				log.Fatalf("Error creating processors: %+v", err)
			}

			digger := &dig.Digger{
				SourceConsumer: &dig.FileConsumer{
					Paths:     strings.Split(config.FilePaths, ","),
					Recursive: config.Recursive,
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
