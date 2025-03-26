package replace

import (
	"errors"
	"io"
	"net/url"
	"os"

	"github.com/ClickHouse/ch-go/proto"
	ice "github.com/agnosticeng/icepq/internal/iceberg"
	"github.com/apache/iceberg-go"
	"github.com/samber/lo"
	"github.com/urfave/cli/v2"
)

func Flags() []cli.Flag {
	return []cli.Flag{}
}

func Command() *cli.Command {
	return &cli.Command{
		Name:  "replace",
		Flags: Flags(),
		Action: func(ctx *cli.Context) error {
			var (
				buf                   proto.Buffer
				inputTableLocationCol = new(proto.ColStr)
				inputInputFilesCol    = proto.NewArray(new(proto.ColStr))
				inputOutputFilesCol   = proto.NewArray(new(proto.ColStr))
				outputErrorCol        = new(proto.ColStr)

				input = proto.Results{
					{Name: "table_location", Data: inputTableLocationCol},
					{Name: "input_files", Data: inputInputFilesCol},
					{Name: "output_files", Data: inputOutputFilesCol},
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

					cat, err := ice.NewVersionHintCatalog(location.String())

					if err != nil {
						return err
					}

					t, err := cat.LoadTable(ctx.Context, nil, iceberg.Properties{})

					if err != nil {
						return err
					}

					var (
						inputFiles = lo.Map(inputInputFilesCol.Row(i), func(path string, _ int) string {
							return location.JoinPath("data", path).String()
						})
						outputFiles = lo.Map(inputOutputFilesCol.Row(i), func(path string, _ int) string {
							return location.JoinPath("data", path).String()
						})
					)

					var tx = t.NewTransaction()

					if err := tx.ReplaceDataFiles(inputFiles, outputFiles, nil); err != nil {
						return err
					}

					if _, err := tx.Commit(ctx.Context); err != nil {
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
					inputInputFilesCol,
					inputOutputFilesCol,
					outputErrorCol,
				)
			}
		},
	}
}
