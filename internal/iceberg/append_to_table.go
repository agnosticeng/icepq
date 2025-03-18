package iceberg

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	pq "github.com/agnosticeng/icepq/internal/parquet"
	"github.com/agnosticeng/objstr"
	objstrerr "github.com/agnosticeng/objstr/errors"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/sourcegraph/conc/iter"
)

type AppendToTableOptions struct {
	ManifestUUID     uuid.UUID
	ManifestListUUID uuid.UUID
}

func AppendToTable(
	ctx context.Context,
	md table.Metadata,
	files []pq.File,
	opts AppendToTableOptions,
) error {
	var (
		os             = objstr.FromContextOrDefault(ctx)
		location, err  = url.Parse(md.Location())
		sequenceNumber int64
		parentSnapId   *int64
	)

	if err != nil {
		return err
	}

	if bytes.Equal(opts.ManifestUUID[:], uuid.Nil[:]) {
		opts.ManifestUUID = uuid.Must(uuid.NewV7())
	}

	if bytes.Equal(opts.ManifestListUUID[:], uuid.Nil[:]) {
		opts.ManifestListUUID = uuid.Must(uuid.NewV7())
	}

	if md.CurrentSnapshot() != nil {
		sequenceNumber = md.CurrentSnapshot().SequenceNumber + 1
		parentSnapId = &md.CurrentSnapshot().SnapshotID
	} else {
		sequenceNumber = 1
	}

	var (
		snapID               = time.Now().UnixMilli()
		manifestLocation     = location.JoinPath("metadata", fmt.Sprintf("man-%s.avro", opts.ManifestUUID.String()))
		manifestListLocation = location.JoinPath("metadata", fmt.Sprintf("snap-%s.avro", opts.ManifestListUUID.String()))
	)

	// ensure all files to be appended belong to the table data prefix
	for _, file := range files {
		if !strings.HasPrefix(file.URL().String(), location.JoinPath("data").String()) {
			return fmt.Errorf("file %s does not belongs to table at %s", file.URL().String(), location.String())
		}
	}

	schemas, err := iter.MapErr(files, func(f *pq.File) (*iceberg.Schema, error) {
		return pq.ToIcebergSchema(f.Metadata().Schema)
	})

	if err != nil {
		return err
	}

	// ensure schema of all new files is equals to current snapshot schema
	if !lo.EveryBy(schemas, func(sch *iceberg.Schema) bool {
		return md.CurrentSchema().Equals(sch)
	}) {
		return fmt.Errorf("incompatible schema")
	}

	dfs, err := iter.MapErr(files, func(f *pq.File) (iceberg.DataFile, error) {
		return pq.NewIcegergDataFile(*f)
	})

	if err != nil {
		return err
	}

	manEntries, err := iter.MapErr(dfs, func(df *iceberg.DataFile) (iceberg.ManifestEntry, error) {
		return iceberg.NewManifestEntryV2Builder(
			iceberg.EntryStatusADDED,
			snapID,
			*df,
		).Build(), nil
	})

	if err != nil {
		return err
	}

	if err := WriteManifest(
		ctx,
		2,
		manifestLocation,
		snapID,
		md.PartitionSpec(),
		md.CurrentSchema(),
		manEntries,
	); err != nil {
		return err
	}

	manMd, err := objstr.FromContextOrDefault(ctx).ReadMetadata(ctx, manifestLocation)

	if err != nil {
		return err
	}

	var man = iceberg.NewManifestV2Builder(
		manifestLocation.String(),
		int64(manMd.Size),
		int32(md.DefaultPartitionSpec()),
		iceberg.ManifestContentData,
		snapID,
	).Build()

	var manFiles []iceberg.ManifestFile

	if md.CurrentSnapshot() != nil {
		u, err := url.Parse(md.CurrentSnapshot().ManifestList)

		if err != nil {
			return err
		}

		prevFiles, err := ReadManifestList(ctx, u)

		if err != nil {
			return err
		}

		manFiles = append(manFiles, prevFiles...)
	}

	manFiles = append(manFiles, man)

	if err := WriteManifestList(
		ctx,
		2,
		manifestListLocation,
		snapID,
		parentSnapId,
		&sequenceNumber,
		manFiles,
	); err != nil {
		return err
	}

	newMDBuilder, err := table.MetadataBuilderFromBase(md)

	if err != nil {
		return err
	}

	var snap = table.Snapshot{}
	snap.SnapshotID = snapID
	snap.TimestampMs = time.Now().UnixMilli()
	snap.ManifestList = manifestListLocation.String()
	snap.Summary = &table.Summary{Operation: table.OpAppend}
	snap.SchemaID = &md.CurrentSchema().ID

	if md.CurrentSnapshot() != nil {
		snap.SequenceNumber = md.CurrentSnapshot().SequenceNumber + 1
	} else {
		snap.SequenceNumber = 1
	}

	newMDBuilder, err = newMDBuilder.AddSnapshot(&snap)

	if err != nil {
		return err
	}

	newMDBuilder, err = newMDBuilder.SetSnapshotRef(table.MainBranch, snap.SnapshotID, table.BranchRef)

	if err != nil {
		return err
	}

	var metadataLocation = location.JoinPath("metadata", metadataFileName(snap.SequenceNumber))

	_, err = os.ReadMetadata(ctx, metadataLocation)

	if errors.Is(err, objstrerr.ErrObjectNotFound) {
		return fmt.Errorf("metadata file already exists: %s", metadataLocation.String())
	}

	newTableMD, err := newMDBuilder.Build()

	if err != nil {
		return err
	}

	return WriteMetadata(ctx, metadataLocation, newTableMD)
}
