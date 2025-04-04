package function

import (
	"github.com/agnosticeng/icepq/cmd/clickhouse/function/add"
	"github.com/agnosticeng/icepq/cmd/clickhouse/function/replace"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name: "function",
		Subcommands: []*cli.Command{
			add.Command(),
			replace.Command(),
		},
	}
}
