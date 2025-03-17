package parquet

import (
	"context"
	"net/url"

	"github.com/agnosticeng/icepq/internal/io"
	"github.com/agnosticeng/objstr"
	"github.com/agnosticeng/objstr/types"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/iceberg-go"
)

type File struct {
	r *file.Reader
	o *types.Object
}

func (f *File) URL() *url.URL {
	return f.o.URL
}

func (f *File) Close() error {
	return f.r.Close()
}

func OpenFile(ctx context.Context, u *url.URL) (File, error) {
	var os = objstr.FromContextOrDefault(ctx)

	md, err := os.ReadMetadata(ctx, u)

	if err != nil {
		return File{}, err
	}

	r, err := os.ReaderAt(ctx, u)

	if err != nil {
		return File{}, err
	}

	defer r.Close()

	pqr, err := file.NewParquetReader(io.NewReadSeekerAdapter(r, int64(md.Size)))

	if err != nil {
		return File{}, err
	}

	return File{
		r: pqr,
		o: &types.Object{URL: u, Metadata: md},
	}, nil
}

func OpenObject(ctx context.Context, o *types.Object) (File, error) {
	var os = objstr.FromContextOrDefault(ctx)

	r, err := os.ReaderAt(ctx, o.URL)

	if err != nil {
		return File{}, err
	}

	pqr, err := file.NewParquetReader(io.NewReadSeekerAdapter(r, int64(o.Metadata.Size)))

	if err != nil {
		return File{}, err
	}
	return File{
		r: pqr,
		o: o,
	}, nil
}

func (f *File) Metadata() *metadata.FileMetaData {
	return f.r.MetaData()
}

func NewIcegergDataFile(f File) (iceberg.DataFile, error) {
	b, err := iceberg.NewDataFileBuilder(
		iceberg.EntryContentData,
		f.URL().String(),
		iceberg.ParquetFile,
		nil,
		f.r.NumRows(),
		int64(f.o.Metadata.Size),
	)

	if err != nil {
		return nil, err
	}

	return b.Build(), nil
}
