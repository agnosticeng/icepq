package replace

import (
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"

	"github.com/ClickHouse/ch-go/proto"
	ice "github.com/agnosticeng/icepq/internal/iceberg"
	"github.com/apache/iceberg-go"
	"github.com/samber/lo"
	"github.com/urfave/cli/v2"
	slogctx "github.com/veqryn/slog-context"
)

func Flags() []cli.Flag {
	return []cli.Flag{}
}

func Command() *cli.Command {
	return &cli.Command{
		Name:  "replace",
		Flags: Flags(),
		Action: func(ctx *cli.Context) error {
			slogctx.FromCtx(ctx.Context).Info("popopo")

			fmt.Fprintf(os.Stderr, "loooooooooooooooo\n")

			fmt.Fprintln(os.Stderr, "cocococ")

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

			fmt.Fprintln(os.Stderr, "begin")

			for {
				var (
					inputBlock proto.Block
					err        = inputBlock.DecodeRawBlock(
						proto.NewReader(os.Stdin),
						54451,
						input,
					)
				)

				fmt.Fprintln(os.Stderr, "read")

				if errors.Is(err, io.EOF) {
					return nil
				}

				if err != nil {
					return err
				}

				for i := 0; i < input.Rows(); i++ {
					fmt.Fprintln(os.Stderr, "before parse url")

					location, err := url.Parse(inputTableLocationCol.Row(i))

					if err != nil {
						return err
					}

					fmt.Fprintln(os.Stderr, "before new catalog")

					cat, err := ice.NewVersionHintCatalog(location.String())

					if err != nil {
						return err
					}

					fmt.Fprintln(os.Stderr, "before load table")

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

					fmt.Fprintln(os.Stderr, "before tx")

					var tx = t.NewTransaction()

					if err := tx.ReplaceDataFiles(ctx.Context, inputFiles, outputFiles, nil); err != nil {
						return err
					}

					fmt.Fprintln(os.Stderr, "after replace")

					if _, err := tx.Commit(ctx.Context); err != nil {
						return err
					}

					fmt.Fprintln(os.Stderr, "after commit")

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
