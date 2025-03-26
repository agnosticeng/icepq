package schema

import (
	"fmt"
	"net/url"

	ice "github.com/agnosticeng/icepq/internal/iceberg"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:  "schema",
		Usage: "schema <path>",
		Action: func(ctx *cli.Context) error {
			u, err := url.Parse(ctx.Args().Get(0))

			if err != nil {
				return err
			}

			sch, err := ice.SchemaFromParquetFile(ctx.Context, u)

			if err != nil {
				return err
			}

			fmt.Println()
			fmt.Println(sch)

			return nil
		},
	}
}
