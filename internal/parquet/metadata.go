package parquet

import (
	"context"
	"net/url"

	"github.com/agnosticeng/icepq/internal/io"
	"github.com/agnosticeng/objstr"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
)

func FetchMetadata(ctx context.Context, u *url.URL, size int) (*metadata.FileMetaData, error) {
	var os = objstr.FromContextOrDefault(ctx)

	r, err := os.ReaderAt(ctx, u)

	if err != nil {
		return nil, err
	}

	defer r.Close()

	f, err := file.NewParquetReader(io.NewReadSeekerAdapter(r, int64(size)))

	if err != nil {
		return nil, err
	}

	return f.MetaData(), nil
}
