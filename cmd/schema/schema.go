package schema

import (
	"encoding/json"
	"fmt"
	"net/url"

	pq "github.com/agnosticeng/icepq/internal/parquet"
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

			f, err := pq.OpenFile(ctx.Context, u)

			js, _ := json.MarshalIndent(f.Metadata().Schema, "", "    ")
			fmt.Println("PARQUET SCHEMA", string(js))

			sch, err := pq.ToIcebergSchema(f.Metadata().Schema)

			if err != nil {
				return err
			}

			fmt.Println("ICEBERG SCHEMA", sch.String())

			js, err = json.MarshalIndent(sch, "", "   ")

			if err != nil {
				return err
			}

			fmt.Println("ICEBERG SCHEMA (JSON)", string(js))

			return nil
		},
	}
}
