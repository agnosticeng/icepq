package append

import (
	"fmt"
	"net/url"

	ice "github.com/agnosticeng/icepq/internal/iceberg"
	pq "github.com/agnosticeng/icepq/internal/parquet"
	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/sourcegraph/conc/iter"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:  "append",
		Usage: "append <location> <file1> [<file2> ...]",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "uuid", Value: uuid.Must(uuid.NewV7()).String()},
		},
		Action: func(ctx *cli.Context) error {
			location, err := url.Parse(ctx.Args().Get(0))

			if err != nil {
				return err
			}

			tableUUID, err := uuid.Parse(ctx.String("uuid"))

			if err != nil {
				return err
			}

			var paths = ctx.Args().Slice()[1:]

			if len(paths) == 0 {
				return nil
			}

			files, err := iter.MapErr(paths, func(path *string) (pq.File, error) {
				return pq.OpenFile(ctx.Context, location.JoinPath("data", *path))
			})

			if err != nil {
				return err
			}

			md, err := ice.FetchLatestMetadata(ctx.Context, location)

			if err != nil {
				return err
			}

			if md == nil {
				schemas, err := iter.MapErr(files, func(f *pq.File) (*iceberg.Schema, error) {
					return pq.ToIcebergSchema(f.Metadata().Schema)
				})

				if err != nil {
					return err
				}

				if !lo.EveryBy(schemas, func(sch *iceberg.Schema) bool {
					return schemas[0].Equals(sch)
				}) {
					return fmt.Errorf("not all provided Parquet files have the same schema")
				}

				if err := ice.CreateTable(ctx.Context, location, schemas[0], ice.CreateTableOptions{
					TableUUID: tableUUID,
				}); err != nil {
					return err
				}

				md, err = ice.FetchLatestMetadata(ctx.Context, location)

				if err != nil {
					return err
				}
			}

			return ice.AppendToTable(ctx.Context, md, files, ice.AppendToTableOptions{})
		},
	}
}
