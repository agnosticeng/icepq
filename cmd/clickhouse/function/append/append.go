package append

import (
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"

	"github.com/ClickHouse/ch-go/proto"
	ice "github.com/agnosticeng/icepq/internal/iceberg"
	pq "github.com/agnosticeng/icepq/internal/parquet"
	"github.com/apache/iceberg-go"
	"github.com/samber/lo"
	"github.com/sourcegraph/conc/iter"
	"github.com/urfave/cli/v2"
)

func Flags() []cli.Flag {
	return []cli.Flag{}
}

func Command() *cli.Command {
	return &cli.Command{
		Name:  "append",
		Flags: Flags(),
		Action: func(ctx *cli.Context) error {
			var (
				buf                   proto.Buffer
				inputTableLocationCol = new(proto.ColStr)
				inputFilesCol         = new(proto.ColStr).Array()
				outputErrorCol        = new(proto.ColStr)

				input = proto.Results{
					{Name: "table_location", Data: inputTableLocationCol},
					{Name: "files", Data: inputFilesCol},
				}

				output = proto.Input{
					{Name: "error", Data: outputErrorCol},
				}
			)

			for {
				var (
					inputBlock proto.Block
					err        = inputBlock.DecodeRawBlock(
						proto.NewReader(os.Stdin),
						54451,
						input,
					)
				)

				if errors.Is(err, io.EOF) {
					return nil
				}

				if err != nil {
					return err
				}

				for i := 0; i < input.Rows(); i++ {
					location, err := url.Parse(inputTableLocationCol.Row(i))

					if err != nil {
						return err
					}

					files, err := iter.MapErr(inputFilesCol.Row(i), func(path *string) (pq.File, error) {
						fmt.Fprint(os.Stderr, "location", location.String(), "path", location.JoinPath("data", *path))

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

						if err := ice.CreateTable(ctx.Context, location, schemas[0], ice.CreateTableOptions{}); err != nil {
							return err
						}

						md, err = ice.FetchLatestMetadata(ctx.Context, location)

						if err != nil {
							return err
						}
					}

					if err := ice.AppendToTable(ctx.Context, md, files, ice.AppendToTableOptions{}); err != nil {
						return err
					}

					outputErrorCol.Append("")
				}

				var outputblock = proto.Block{
					Columns: 1,
					Rows:    input.Rows(),
				}

				if err := outputblock.EncodeRawBlock(&buf, 54451, output); err != nil {
					return err
				}

				if _, err := os.Stdout.Write(buf.Buf); err != nil {
					return err
				}

				proto.Reset(
					&buf,
					inputTableLocationCol,
					inputFilesCol,
					outputErrorCol,
				)
			}
		},
	}
}
