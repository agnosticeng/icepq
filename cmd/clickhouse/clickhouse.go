package clickhouse

import (
	"github.com/agnosticeng/icepq/cmd/clickhouse/function"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name: "clickhouse",
		Subcommands: []*cli.Command{
			function.Command(),
		},
	}
}
