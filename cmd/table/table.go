package table

import (
	"github.com/agnosticeng/icepq/cmd/table/append"
	"github.com/agnosticeng/icepq/cmd/table/files"
	"github.com/agnosticeng/icepq/cmd/table/replace"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name: "table",
		Subcommands: []*cli.Command{
			append.Command(),
			files.Command(),
			replace.Command(),
		},
	}
}
