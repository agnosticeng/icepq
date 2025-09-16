package field_bound_values

import (
	"encoding/json"
	"errors"
	"io"
	"os"

	"github.com/ClickHouse/ch-go/proto"
	ice "github.com/agnosticeng/icepq/internal/iceberg"
	"github.com/sourcegraph/conc/iter"
	"github.com/urfave/cli/v2"
)

func Flags() []cli.Flag {
	return []cli.Flag{}
}

func Command() *cli.Command {
	return &cli.Command{
		Name:  "field-bound-values",
		Flags: Flags(),
		Action: func(ctx *cli.Context) error {
			var (
				buf                   proto.Buffer
				inputTableLocationCol = new(proto.ColStr)
				inputFieldNameCol     = new(proto.ColStr)
				outputResultCol       = new(proto.ColStr).Array()

				input = proto.Results{
					{Name: "table_location", Data: inputTableLocationCol},
					{Name: "field_name", Data: inputFieldNameCol},
				}

				output = proto.Input{
					{Name: "result", Data: outputResultCol},
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
					values, err := ice.FieldBoundValues(
						ctx.Context,
						inputTableLocationCol.Row(i),
						inputFieldNameCol.Row(i),
						ice.FieldBoundValuesConfig{
							FailOnDeleteFiles:   true,
							FailOnMissingValues: true,
						},
					)

					if err != nil {
						return err
					}

					res, err := iter.MapErr(values, func(item *ice.FieldBoundValuesItem) (string, error) {
						js, err := json.Marshal(item)
						if err != nil {
							return "", err
						}
						return string(js), nil
					})

					if err != nil {
						return err
					}

					outputResultCol.Append(res)
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
					inputFieldNameCol,
					outputResultCol,
				)
			}
		},
	}
}
