package field_bound_values

import (
	"encoding/json"
	"fmt"

	ice "github.com/agnosticeng/icepq/internal/iceberg"
	"github.com/urfave/cli/v2"
	_ "gocloud.dev/blob/s3blob"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:  "field-bound-values",
		Usage: "<location> <field-name>",
		Flags: []cli.Flag{
			&cli.BoolFlag{Name: "fail-on-delete-files"},
			&cli.BoolFlag{Name: "fail-on-missing-values"},
		},
		Action: func(ctx *cli.Context) error {
			var (
				location  = ctx.Args().Get(0)
				fieldName = ctx.Args().Get(1)
				conf      = ice.FieldBoundValuesConfig{
					FailOnDeleteFiles:   ctx.Bool("fail-on-delete-files"),
					FailOnMissingValues: ctx.Bool("fail-on-missing-values"),
				}
			)

			items, err := ice.FieldBoundValues(ctx.Context, location, fieldName, conf)
			if err != nil {
				return err
			}

			for _, item := range items {
				js, err := json.Marshal(item)
				if err != nil {
					return err
				}

				fmt.Println(string(js))
			}

			return nil
		},
	}
}
