package parquet

import (
	"context"
	"net/url"

	"github.com/agnosticeng/objstr"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

func FetchMetadata(ctx context.Context, u *url.URL, size int) (*format.FileMetaData, error) {
	var os = objstr.FromContextOrDefault(ctx)

	r, err := os.ReaderAt(ctx, u)

	if err != nil {
		return nil, err
	}

	defer r.Close()

	f, err := parquet.OpenFile(r, int64(size))

	if err != nil {
		return nil, err
	}

	return f.Metadata(), nil
}
