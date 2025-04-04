package replace

import (
	"fmt"
	"net/url"
	"strings"

	ice "github.com/agnosticeng/icepq/internal/iceberg"
	"github.com/samber/lo"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:  "replace",
		Usage: "replace <location>  <input_file_1,input_file_2,...>  <output_file_1,output_file_2>",
		Flags: []cli.Flag{
			&cli.StringSliceFlag{Name: "prop"},
		},
		Action: func(ctx *cli.Context) error {
			var (
				props         = ice.ParseProperties(ctx.StringSlice("prop"))
				location, err = url.Parse(ctx.Args().Get(0))
			)

			if err != nil {
				return err
			}

			cat, err := ice.NewVersionHintCatalog(location.String())

			if err != nil {
				return err
			}

			t, err := cat.LoadTable(ctx.Context, nil, props)

			if err != nil {
				return err
			}

			inputFiles, err := toDataFileURLs(location, ctx.Args().Get(1))

			if err != nil {
				return err
			}

			outputFiles, err := toDataFileURLs(location, ctx.Args().Get(2))

			if err != nil {
				return err
			}

			var tx = t.NewTransaction()

			if err := tx.ReplaceDataFiles(ctx.Context, inputFiles, outputFiles, props); err != nil {
				return err
			}

			_, err = tx.Commit(ctx.Context)
			return err
		},
	}
}

func toDataFileURLs(location *url.URL, s string) ([]string, error) {
	var files = strings.Split(s, ",")

	if len(files) < 1 {
		return nil, fmt.Errorf("files list must have at least 1 item")
	}

	return lo.Map(files, func(file string, _ int) string { return location.JoinPath("data", file).String() }), nil
}
