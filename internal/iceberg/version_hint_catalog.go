package iceberg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"

	"github.com/agnosticeng/icepq/internal/io"
	"github.com/agnosticeng/objstr"
	objstrerrs "github.com/agnosticeng/objstr/errors"
	"github.com/agnosticeng/objstr/utils"
	osutils "github.com/agnosticeng/objstr/utils"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
)

var (
	ErrConcurrentCommit                 = errors.New("concurrent commit")
	_                   table.CatalogIO = &VersionHintCatalog{}
)

type VersionHintCatalog struct {
	tableLocation *url.URL
}

func NewVersionHintCatalog(tableLocation string) (*VersionHintCatalog, error) {
	u, err := url.Parse(tableLocation)

	if err != nil {
		return nil, err
	}

	return &VersionHintCatalog{
		tableLocation: u,
	}, nil
}

func (cat *VersionHintCatalog) CreateTable(
	ctx context.Context,
	identifier table.Identifier,
	schema *iceberg.Schema,
	opts ...catalog.CreateTableOpt,
) (*table.Table, error) {
	var (
		os                  = objstr.FromContextOrDefault(ctx)
		versionHintLocation = cat.tableLocation.JoinPath("metadata", "version-hint.text")
	)

	_, err := os.ReadMetadata(ctx, versionHintLocation)

	if !errors.Is(err, objstrerrs.ErrObjectNotFound) {
		return nil, catalog.ErrTableAlreadyExists
	}

	b, err := table.NewMetadataBuilder()

	if err != nil {
		return nil, err
	}

	b, err = b.SetUUID(uuid.Must(uuid.NewV7()))

	if err != nil {
		return nil, err
	}

	b, err = b.SetLoc(cat.tableLocation.String())

	if err != nil {
		return nil, err
	}

	b, err = b.AddSchema(schema, schema.HighestFieldID(), true)

	if err != nil {
		return nil, err
	}

	b, err = b.AddPartitionSpec(iceberg.UnpartitionedSpec, true)

	if err != nil {
		return nil, err
	}

	b, err = b.AddSortOrder(&table.UnsortedSortOrder, true)

	if err != nil {
		return nil, err
	}

	b, err = b.SetFormatVersion(2)

	if err != nil {
		return nil, err
	}

	md, err := b.Build()

	if err != nil {
		return nil, err
	}

	var (
		mdName = metadataFileName(0)
		mdLoc  = cat.tableLocation.JoinPath("metadata", mdName)
	)

	if err := cat.writeMetadataFile(ctx, os, mdLoc, md); err != nil {
		return nil, err
	}

	if err := cat.writeVersionHint(ctx, os, "", mdName); err != nil {
		return nil, err
	}

	return table.New(
		[]string{},
		md,
		cat.tableLocation.String(),
		io.NewObjectStoreIO(os),
		cat,
	), nil
}

func (cat *VersionHintCatalog) LoadTable(ctx context.Context, identifier table.Identifier, props iceberg.Properties) (*table.Table, error) {
	var (
		os           = objstr.FromContextOrDefault(ctx)
		content, err = osutils.ReadObject(ctx, os, cat.tableLocation.JoinPath("metadata", "version-hint.text"))
	)

	if errors.Is(err, objstrerrs.ErrObjectNotFound) {
		return nil, catalog.ErrNoSuchTable
	}

	var mdLocation = cat.tableLocation.JoinPath("metadata", string(content))

	mdBytes, err := osutils.ReadObject(ctx, os, mdLocation)

	if err != nil {
		return nil, err
	}

	md, err := table.ParseMetadataBytes(mdBytes)

	if err != nil {
		return nil, err
	}

	return table.New(
		[]string{},
		md,
		cat.tableLocation.String(),
		io.NewObjectStoreIO(os),
		cat,
	), nil
}

func (cat *VersionHintCatalog) CommitTable(
	ctx context.Context,
	t *table.Table,
	requirements []table.Requirement,
	updates []table.Update,
) (table.Metadata, string, error) {
	var (
		os     = objstr.FromContextOrDefault(ctx)
		b, err = table.MetadataBuilderFromBase(t.Metadata())
	)

	for _, req := range requirements {
		if err := req.Validate(t.Metadata()); err != nil {
			return nil, "", err
		}
	}

	if err != nil {
		return nil, "", err
	}

	for _, update := range updates {
		if err := update.Apply(b); err != nil {
			return nil, "", err
		}
	}

	md, err := b.Build()

	if err != nil {
		return nil, "", err
	}

	var (
		mdName = metadataFileName(md.CurrentSnapshot().SequenceNumber)
		mdLoc  = cat.tableLocation.JoinPath("metadata", mdName)
	)

	if err := cat.writeMetadataFile(ctx, os, mdLoc, md); err != nil {
		return nil, "", err
	}

	if err := cat.writeVersionHint(ctx, os, "", mdName); err != nil {
		return nil, "", err
	}

	return md, mdLoc.String(), nil
}

func (cat *VersionHintCatalog) writeMetadataFile(ctx context.Context, os *objstr.ObjectStore, location *url.URL, md table.Metadata) error {
	js, err := json.Marshal(md)

	if err != nil {
		return err
	}

	return utils.CreateObject(
		ctx,
		os,
		location,
		js,
	)
}

func (cat *VersionHintCatalog) writeVersionHint(ctx context.Context, os *objstr.ObjectStore, expectedContent, newContent string) error {
	var versionHintLocation = cat.tableLocation.JoinPath("metadata", "version-hint.text")

	if len(expectedContent) != 0 {
		actualContent, err := osutils.ReadObject(ctx, os, versionHintLocation)

		if err != nil {
			return err
		}

		if string(actualContent) != expectedContent {
			return ErrConcurrentCommit
		}
	}

	return osutils.CreateObject(ctx, os, versionHintLocation, []byte(newContent))
}

func metadataFileName(sequenceNumber int64) string {
	return fmt.Sprintf("%012d-%s.metadata.json", sequenceNumber, uuid.Must(uuid.NewV7()))
}
