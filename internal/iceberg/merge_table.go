package iceberg

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/agnosticeng/icepq/internal/io"
	pq "github.com/agnosticeng/icepq/internal/parquet"
	"github.com/agnosticeng/objstr"
	objstrerr "github.com/agnosticeng/objstr/errors"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/sourcegraph/conc/iter"
)

type MergeOp struct {
	Inputs []pq.File
	Output pq.File
}

type MergeTableOptions struct {
	ManifestUUID     uuid.UUID
	ManifestListUUID uuid.UUID
}

func MergeTable(
	ctx context.Context,
	md table.Metadata,
	mergeOps []MergeOp,
	opts MergeTableOptions,
) error {
	var (
		os             = objstr.FromContextOrDefault(ctx)
		fs             = io.NewObjectStorageAdapter(os)
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
		inputFiles           = lo.FlatMap(mergeOps, func(op MergeOp, _ int) []pq.File { return op.Inputs })
		outputFiles          = lo.Map(mergeOps, func(op MergeOp, _ int) pq.File { return op.Output })
		allFiles             = lo.FlatMap(mergeOps, func(op MergeOp, _ int) []pq.File {
			return slices.Concat(op.Inputs, []pq.File{op.Output})
		})
	)

	// ensure all files to be appended belong to the table data prefix
	var errs = iter.Map(allFiles, func(f *pq.File) error {
		if !strings.HasPrefix(f.URL().String(), location.JoinPath("data").String()) {
			return fmt.Errorf("input file %s does not belongs to table at %s", f.URL().String(), location.String())
		}

		return nil
	})

	if err := errors.Join(errs...); err != nil {
		return err
	}

	schemas, err := iter.MapErr(outputFiles, func(f *pq.File) (*iceberg.Schema, error) {
		return pq.ToIcebergSchema(f.Metadata().Schema)
	})

	if err != nil {
		return err
	}

	// ensure schema of all new files is equals to current snapshot schema
	if !lo.EveryBy(schemas, func(sch *iceberg.Schema) bool {
		return md.CurrentSchema().Equals(sch)
	}) {
		return fmt.Errorf("not all provided Parquet files have the same schema")
	}

	var (
		untouchedManifestFiles []iceberg.ManifestFile          // manifest that don't contain any input files
		newManifestEntries     []iceberg.ManifestEntry         // existing data file entries in manifest that contain some input files
		inputFilesFound        = make([]bool, len(inputFiles)) // ensure each input file is actually part of the latest snapshot
	)

	manFiles, err := ReadManifestList(ctx, lo.Must(url.Parse(md.CurrentSnapshot().ManifestList)))

	if err != nil {
		return err
	}

	for _, manFile := range manFiles {
		entries, err := manFile.FetchEntries(fs, false)

		if err != nil {
			return err
		}

		var (
			touched          bool
			untouchedEntries []iceberg.ManifestEntry
		)

		for _, entry := range entries {
			_, i, ok := lo.FindIndexOf(inputFiles, func(f pq.File) bool {
				return f.URL().String() == entry.DataFile().FilePath()
			})

			// if entry data file is not in input files, add the the manifest to be created as "existing"
			if !ok {
				var b = iceberg.NewManifestEntryV2Builder(
					iceberg.EntryStatusEXISTING,
					entry.SnapshotID(),
					entry.DataFile(),
				)

				if i := entry.FileSequenceNum(); i != nil {
					b = b.FileSequenceNum(*i)
				}

				b = b.SequenceNum(entry.SequenceNum())
				var entry = b.Build()
				untouchedEntries = append(untouchedEntries, entry)
			} else {
				inputFilesFound[i] = true
				touched = true
			}
		}

		if !touched {
			untouchedManifestFiles = append(untouchedManifestFiles, manFile)
		} else {
			newManifestEntries = append(newManifestEntries, untouchedEntries...)
		}
	}

	for i, f := range inputFiles {
		if !inputFilesFound[i] {
			return fmt.Errorf("input file %s not found in any manifest", f.URL().String())
		}
	}

	for _, f := range outputFiles {
		df, err := pq.NewIcegergDataFile(f)

		if err != nil {
			return err
		}

		var entry = iceberg.NewManifestEntryV2Builder(
			iceberg.EntryStatusADDED,
			snapID,
			df,
		).Build()

		newManifestEntries = append(newManifestEntries, entry)
	}

	if err := WriteManifest(
		ctx,
		2,
		manifestLocation,
		snapID,
		md.PartitionSpec(),
		md.CurrentSchema(),
		newManifestEntries,
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

	if err := WriteManifestList(
		ctx,
		2,
		manifestListLocation,
		snapID,
		parentSnapId,
		&sequenceNumber,
		slices.Concat(untouchedManifestFiles, []iceberg.ManifestFile{man}),
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
