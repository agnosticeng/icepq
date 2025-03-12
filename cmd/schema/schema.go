package schema

import (
	"encoding/json"
	"fmt"
	"net/url"

	pq "github.com/agnosticeng/icepq/internal/parquet"
	"github.com/agnosticeng/objstr"
	"github.com/parquet-go/parquet-go"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:  "schema",
		Usage: "schema <path>",
		Action: func(ctx *cli.Context) error {
			var os = objstr.FromContext(ctx.Context)

			u, err := url.Parse(ctx.Args().Get(0))

			if err != nil {
				return err
			}

			md, err := os.ReadMetadata(ctx.Context, u)

			if err != nil {
				return err
			}

			r, err := os.ReaderAt(ctx.Context, u)

			if err != nil {
				return err
			}

			defer r.Close()

			f, err := parquet.OpenFile(r, int64(md.Size))

			if err != nil {
				return err
			}

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
