package avro

import (
	"github.com/agnosticeng/icepq/cmd/avro/show"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name: "avro",
		Subcommands: []*cli.Command{
			show.Command(),
		},
	}
}
