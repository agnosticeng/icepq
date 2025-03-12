package merge

import (
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"

	"github.com/ClickHouse/ch-go/proto"
	ice "github.com/agnosticeng/icepq/internal/iceberg"
	pq "github.com/agnosticeng/icepq/internal/parquet"
	"github.com/sourcegraph/conc/iter"
	"github.com/urfave/cli/v2"
)

func Flags() []cli.Flag {
	return []cli.Flag{}
}

func Command() *cli.Command {
	return &cli.Command{
		Name:  "merge",
		Flags: Flags(),
		Action: func(ctx *cli.Context) error {
			var (
				buf                   proto.Buffer
				inputTableLocationCol = new(proto.ColStr)
				inputMergesCol        = proto.NewArray(proto.NewArray(new(proto.ColStr)))
				outputErrorCol        = new(proto.ColStr)

				input = proto.Results{
					{Name: "table_location", Data: inputTableLocationCol},
					{Name: "merges", Data: inputMergesCol},
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

					var merges = inputMergesCol.Row(i)

					mergeOps, err := iter.MapErr(
						merges,
						func(ss *[]string) (ice.MergeOp, error) {
							if len(*ss) < 3 {
								return ice.MergeOp{}, fmt.Errorf("invalid merge: %s (must have 1 output and at least 2 inputs)", *ss)
							}

							output, err := pq.OpenFile(ctx.Context, location.JoinPath("data", (*ss)[0]))

							if err != nil {
								return ice.MergeOp{}, err
							}

							inputs, err := iter.MapErr((*ss)[1:], func(s *string) (pq.File, error) {
								return pq.OpenFile(ctx.Context, location.JoinPath("data", *s))
							})

							if err != nil {
								return ice.MergeOp{}, err
							}

							return ice.MergeOp{
								Output: output,
								Inputs: inputs,
							}, nil
						},
					)

					if err != nil {
						return err
					}

					md, err := ice.FetchLatestMetadata(ctx.Context, location)

					if err != nil {
						return err
					}

					if err := ice.MergeTable(ctx.Context, md, mergeOps, ice.MergeTableOptions{}); err != nil {
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
					inputMergesCol,
					outputErrorCol,
				)
			}
		},
	}
}
