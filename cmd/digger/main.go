package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/segmentio/cli"
	"github.com/segmentio/data-digger/cmd/digger/subcmd"
	log "github.com/sirupsen/logrus"
	prefixed "github.com/x-cray/logrus-prefixed-formatter"
)

func init() {
	log.SetFormatter(&prefixed.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
		FullTimestamp:   true,
	})
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Debugf("Caught interrupt, cancelling context")
		cancel()
	}()

	cli.Exec(
		cli.CommandSet{
			"file":    subcmd.FileCmd(ctx),
			"kafka":   subcmd.KafkaCmd(ctx),
			"s3":      subcmd.S3Cmd(ctx),
			"version": subcmd.VersionCmd(ctx),
		},
	)
}
