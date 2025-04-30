package add

import (
	ice "github.com/agnosticeng/icepq/internal/iceberg"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:  "add",
		Usage: "add <location> <file1> [<file2> ...]",
		Flags: []cli.Flag{
			&cli.StringSliceFlag{Name: "prop"},
		},
		Action: func(ctx *cli.Context) error {
			var (
				location = ctx.Args().Get(0)
				files    = ctx.Args().Slice()[1:]
				props    = ice.ParseProperties(ctx.StringSlice("prop"))
			)

			if len(files) == 0 {
				return nil
			}

			return ice.DoCommit(func() error {
				return ice.CreateOrAddFiles(
					ctx.Context,
					location,
					files,
					props,
				)
			})
		},
	}
}
