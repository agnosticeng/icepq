package iceberg

import (
	"context"
	"net/url"

	"github.com/apache/iceberg-go"
	"github.com/samber/lo"
)

func ReplaceFiles(
	ctx context.Context,
	tableLocation string,
	inputFiles []string,
	outputFiles []string,
	props iceberg.Properties,
) error {
	location, err := url.Parse(tableLocation)

	if err != nil {
		return err
	}

	cat, err := NewVersionHintCatalog(location.String())

	if err != nil {
		return err
	}

	t, err := cat.LoadTable(ctx, nil, iceberg.Properties{})

	if err != nil {
		return err
	}

	var (
		inputLocations = lo.Map(inputFiles, func(path string, _ int) string {
			return location.JoinPath("data", path).String()
		})
		outputLocations = lo.Map(outputFiles, func(path string, _ int) string {
			return location.JoinPath("data", path).String()
		})
	)

	var tx = t.NewTransaction()

	if err := tx.ReplaceDataFiles(ctx, inputLocations, outputLocations, props); err != nil {
		return err
	}

	if _, err := tx.Commit(ctx); err != nil {
		return err
	}

	return nil
}
