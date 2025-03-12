package iceberg

import (
	"bytes"
	"context"
	"net/url"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
)

type CreateTableOptions struct {
	TableUUID uuid.UUID
}

func CreateTable(
	ctx context.Context,
	location *url.URL,
	schema *iceberg.Schema,
	opts CreateTableOptions,
) error {
	var metadataLocation = location.JoinPath("metadata", metadataFileName(0))

	if bytes.Equal(opts.TableUUID[:], uuid.Nil[:]) {
		opts.TableUUID = uuid.Must(uuid.NewV7())
	}

	tableMdBuilder, err := table.NewMetadataBuilder()

	if err != nil {
		return err
	}

	tableMdBuilder, err = tableMdBuilder.SetUUID(opts.TableUUID)

	if err != nil {
		return err
	}

	tableMdBuilder, err = tableMdBuilder.SetLoc(location.String())

	if err != nil {
		return err
	}

	tableMdBuilder, err = tableMdBuilder.AddSchema(schema, schema.HighestFieldID(), true)

	if err != nil {
		return err
	}

	tableMdBuilder, err = tableMdBuilder.AddPartitionSpec(iceberg.UnpartitionedSpec, true)

	if err != nil {
		return err
	}

	tableMdBuilder, err = tableMdBuilder.AddSortOrder(&table.UnsortedSortOrder, true)

	if err != nil {
		return err
	}

	tableMdBuilder, err = tableMdBuilder.SetFormatVersion(2)

	if err != nil {
		return err
	}

	tableMd, err := tableMdBuilder.Build()

	if err != nil {
		return err
	}

	if err := WriteMetadata(ctx, metadataLocation, tableMd); err != nil {
		return err
	}

	return nil
}
