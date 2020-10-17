package subcmd

import (
	"context"
	"fmt"

	"github.com/segmentio/cli"
	"github.com/segmentio/data-digger/pkg/version"
)

type versionConfig struct{}

func VersionCmd(ctx context.Context) cli.Function {
	return cli.Command(
		func(config versionConfig) {
			fmt.Printf("digger version v%s\n", version.Version)
		},
	)
}
