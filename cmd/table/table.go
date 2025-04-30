package table

import (
	"github.com/agnosticeng/icepq/cmd/table/add"
	"github.com/agnosticeng/icepq/cmd/table/reachable_files"
	"github.com/agnosticeng/icepq/cmd/table/replace"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name: "table",
		Subcommands: []*cli.Command{
			add.Command(),
			replace.Command(),
			reachable_files.Command(),
		},
	}
}
