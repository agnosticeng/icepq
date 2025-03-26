package function

import (
	"github.com/agnosticeng/icepq/cmd/clickhouse/function/append"
	"github.com/agnosticeng/icepq/cmd/clickhouse/function/replace"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name: "function",
		Subcommands: []*cli.Command{
			append.Command(),
			replace.Command(),
		},
	}
}
