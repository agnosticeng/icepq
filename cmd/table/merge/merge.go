package merge

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	ice "github.com/agnosticeng/icepq/internal/iceberg"
	pq "github.com/agnosticeng/icepq/internal/parquet"
	"github.com/sourcegraph/conc/iter"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:  "merge",
		Usage: "merge <location> <output_file_1=input_file_1,input_file_2> [<output_file_2=input_file_3,input_file_4> ...]",
		Action: func(ctx *cli.Context) error {
			location, err := url.Parse(ctx.Args().Get(0))

			if err != nil {
				return err
			}

			mergeOps, err := iter.MapErr(
				ctx.Args().Slice()[1:],
				func(s *string) (ice.MergeOp, error) { return toMergeOp(ctx.Context, location, *s) },
			)

			if err != nil {
				return err
			}

			md, err := ice.FetchLatestMetadata(ctx.Context, location)

			if err != nil {
				return err
			}

			return ice.MergeTable(ctx.Context, md, mergeOps, ice.MergeTableOptions{})
		},
	}
}

func toMergeOp(ctx context.Context, location *url.URL, s string) (ice.MergeOp, error) {
	outputFile, s, found := strings.Cut(s, "=")

	if !found {
		return ice.MergeOp{}, fmt.Errorf("invalid merge op: %s", s)
	}

	var inputFiles = strings.Split(s, ",")

	if len(inputFiles) <= 1 {
		return ice.MergeOp{}, fmt.Errorf("merge op must have at least 2 input files: %s", s)
	}

	output, err := pq.OpenFile(ctx, location.JoinPath("data", outputFile))

	if err != nil {
		return ice.MergeOp{}, err
	}

	inputs, err := iter.MapErr(inputFiles, func(s *string) (pq.File, error) {
		return pq.OpenFile(ctx, location.JoinPath("data", *s))
	})

	if err != nil {
		return ice.MergeOp{}, err
	}

	return ice.MergeOp{
		Output: output,
		Inputs: inputs,
	}, nil
}
