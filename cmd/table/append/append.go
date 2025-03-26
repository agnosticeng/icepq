package append

import (
	"errors"
	"net/url"

	ice "github.com/agnosticeng/icepq/internal/iceberg"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/samber/lo"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:  "append",
		Usage: "append <location> <file1> [<file2> ...]",
		Action: func(ctx *cli.Context) error {
			location, err := url.Parse(ctx.Args().Get(0))

			if err != nil {
				return err
			}

			var paths = ctx.Args().Slice()[1:]

			if len(paths) == 0 {
				return nil
			}

			cat, err := ice.NewVersionHintCatalog(location.String())

			if err != nil {
				return err
			}

			t, err := cat.LoadTable(ctx.Context, nil, iceberg.Properties{})

			if errors.Is(err, catalog.ErrNoSuchTable) {
				sch, err := ice.SchemaFromParquetDataFiles(ctx.Context, location, paths)

				if err != nil {
					return err
				}

				t, err = cat.CreateTable(ctx.Context, nil, sch)

				if err != nil {
					return err
				}
			} else {
				if err != nil {
					return err
				}
			}

			var tx = t.NewTransaction()

			if err := tx.AddFiles(
				lo.Map(paths, func(path string, _ int) string { return location.JoinPath("data", path).String() }),
				nil,
				true,
			); err != nil {
				return err
			}

			_, err = tx.Commit(ctx.Context)
			return err
		},
	}
}
