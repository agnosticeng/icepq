package table

import (
	"github.com/agnosticeng/icepq/cmd/table/create_or_add_files"
	"github.com/agnosticeng/icepq/cmd/table/expire_snapshots"
	"github.com/agnosticeng/icepq/cmd/table/field_bound_values"
	"github.com/agnosticeng/icepq/cmd/table/reachable_files"
	"github.com/agnosticeng/icepq/cmd/table/replace_files"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name: "table",
		Subcommands: []*cli.Command{
			create_or_add_files.Command(),
			replace_files.Command(),
			reachable_files.Command(),
			expire_snapshots.Command(),
			field_bound_values.Command(),
		},
	}
}
