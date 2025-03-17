package iceberg

import (
	"context"
	"net/url"

	"github.com/agnosticeng/icepq/internal/io"
	"github.com/agnosticeng/objstr"
	"github.com/apache/iceberg-go"
	"github.com/sourcegraph/conc/iter"
)

func WriteManifest(
	ctx context.Context,
	version int,
	path *url.URL,
	snapId int64,
	partSpec iceberg.PartitionSpec,
	tableSchema *iceberg.Schema,
	entries []iceberg.ManifestEntry,
) error {
	var os = objstr.FromContextOrDefault(ctx)

	w, err := os.Writer(ctx, path)

	if err != nil {
		return err
	}

	mw, err := iceberg.NewManifestWriter(
		version,
		w,
		partSpec,
		tableSchema,
		snapId,
	)

	if err != nil {
		return err
	}

	for _, entry := range entries {
		if err := mw.Add(entry); err != nil {
			return err
		}
	}

	if err := mw.Close(); err != nil {
		return err
	}

	return w.Close()
}

func WriteManifestList(
	ctx context.Context,
	version int,
	path *url.URL,
	snapId int64,
	parentSnapId *int64,
	sequenceNumber *int64,
	files []iceberg.ManifestFile,
) error {
	var os = objstr.FromContextOrDefault(ctx)

	w, err := os.Writer(ctx, path)

	if err != nil {
		return err
	}

	if err := iceberg.WriteManifestList(
		version,
		w,
		snapId,
		parentSnapId,
		sequenceNumber,
		files,
	); err != nil {
		return err
	}

	return w.Close()
}

func ReadManifestList(ctx context.Context, path *url.URL) ([]iceberg.ManifestFile, error) {
	var os = objstr.FromContextOrDefault(ctx)

	r, err := os.Reader(ctx, path)

	if err != nil {
		return nil, err
	}

	return iceberg.ReadManifestList(r)
}

type ManifestWithEntries struct {
	Manifest iceberg.ManifestFile
	Entries  []iceberg.ManifestEntry
}

func FetchManifestsWithEntries(ctx context.Context, manifestList *url.URL) ([]ManifestWithEntries, error) {
	var fs = io.NewObjectStorageAdapter(objstr.FromContext(ctx))

	manFiles, err := ReadManifestList(ctx, manifestList)

	if err != nil {
		return nil, err
	}

	return iter.MapErr(manFiles, func(manFile *iceberg.ManifestFile) (ManifestWithEntries, error) {
		entries, err := (*manFile).FetchEntries(fs, false)

		if err != nil {
			return ManifestWithEntries{}, err
		}

		return ManifestWithEntries{
			Manifest: *manFile,
			Entries:  entries,
		}, nil
	})
}
