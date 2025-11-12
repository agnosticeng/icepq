package iceberg

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/samber/lo"
)

func CreateOrAddFiles(
	ctx context.Context,
	tableLocation string,
	inputFiles []string,
	props iceberg.Properties,
) error {
	var location, err = url.Parse(tableLocation)
	if err != nil {
		return err
	}

	cleanupURL(location)

	cat, err := NewVersionHintCatalog(location.String())
	if err != nil {
		return err
	}

	t, err := cat.LoadTable(ctx, nil)

	if errors.Is(err, catalog.ErrNoSuchTable) {
		sch, err := SchemaFromParquetDataFiles(ctx, location, inputFiles)
		if err != nil {
			return err
		}

		t, err = cat.CreateTable(ctx, nil, sch, catalog.WithProperties(props))
		if err != nil {
			return err
		}
	} else {
		if err != nil {
			return err
		}
	}

	var tx = t.NewTransaction()

	if err := tx.AddFiles(
		ctx,
		lo.Map(inputFiles, func(path string, _ int) string { return location.JoinPath("data", path).String() }),
		props,
		true,
	); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return err
	}

	if _, err := tx.Commit(ctx); err != nil {
		return err
	}

	return nil
}
