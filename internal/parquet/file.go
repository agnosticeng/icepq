package parquet

import (
	"context"
	"net/url"

	"github.com/agnosticeng/objstr"
	"github.com/agnosticeng/objstr/types"
	"github.com/apache/iceberg-go"
	"github.com/parquet-go/parquet-go"
)

type File struct {
	r types.ReaderAt
	o *types.Object
	*parquet.File
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

	f, err := parquet.OpenFile(r, int64(md.Size))

	if err != nil {
		return File{}, err
	}

	return File{
		r:    r,
		o:    &types.Object{URL: u, Metadata: md},
		File: f,
	}, nil
}

func OpenObject(ctx context.Context, o *types.Object) (File, error) {
	var os = objstr.FromContextOrDefault(ctx)

	r, err := os.ReaderAt(ctx, o.URL)

	if err != nil {
		return File{}, err
	}

	defer r.Close()

	f, err := parquet.OpenFile(r, int64(o.Metadata.Size))

	if err != nil {
		return File{}, err
	}

	return File{
		r:    r,
		o:    o,
		File: f,
	}, nil
}

func NewIcegergDataFile(f File) (iceberg.DataFile, error) {
	b, err := iceberg.NewDataFileBuilder(
		iceberg.EntryContentData,
		f.URL().String(),
		iceberg.ParquetFile,
		nil,
		f.NumRows(),
		int64(f.Size()),
	)

	if err != nil {
		return nil, err
	}

	return b.Build(), nil
}
