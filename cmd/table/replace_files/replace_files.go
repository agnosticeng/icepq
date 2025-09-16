package replace_files

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
		Name:  "replace-files",
		Usage: "<location>  <input_file_1,input_file_2,...>  <output_file_1,output_file_2>",
		Flags: []cli.Flag{
			&cli.StringSliceFlag{Name: "prop"},
		},
		Action: func(ctx *cli.Context) error {
			var (
				location, err = url.Parse(ctx.Args().Get(0))
				props         = ice.ParseProperties(ctx.StringSlice("prop"))
			)

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

			return ice.DoCommit(func() error {
				return ice.ReplaceFiles(
					ctx.Context,
					location.String(),
					inputFiles,
					outputFiles,
					props,
				)
			})
		},
	}
}

func toDataFileURLs(location *url.URL, s string) ([]string, error) {
	var files = lo.Compact(strings.Split(s, ","))

	if len(files) < 1 {
		return nil, fmt.Errorf("files list must have at least 1 item")
	}

	return files, nil
}
