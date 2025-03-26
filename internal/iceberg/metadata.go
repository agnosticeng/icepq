package iceberg

import (
	"context"
	"net/url"
	"path/filepath"

	"github.com/agnosticeng/objstr"
	"github.com/agnosticeng/objstr/types"
	osutils "github.com/agnosticeng/objstr/utils"
	"github.com/apache/iceberg-go/table"
	"github.com/samber/lo"
	"github.com/sourcegraph/conc/iter"
)

type MetadataFile struct {
	table.Metadata
	Path string
}

func FetchAllMetadataFiles(ctx context.Context, location *url.URL) ([]*MetadataFile, error) {
	var (
		os           = objstr.FromContextOrDefault(ctx)
		metadataPath = location.JoinPath("metadata")
	)

	files, err := os.ListPrefix(ctx, metadataPath)

	if err != nil {
		return nil, err
	}

	if len(files) == 0 {
		return nil, nil
	}

	files = lo.Filter(files, func(f *types.Object, _ int) bool {
		b, _ := filepath.Match("*.metadata.json", filepath.Base(f.URL.Path))
		return b
	})

	return iter.MapErr(files, func(f **types.Object) (*MetadataFile, error) {
		mdBytes, err := osutils.ReadObject(ctx, os, (*f).URL)

		if err != nil {
			return nil, err
		}

		md, err := table.ParseMetadataBytes(mdBytes)

		if err != nil {
			return nil, err
		}

		return &MetadataFile{
			Metadata: md,
			Path:     (*f).URL.String(),
		}, nil
	})
}
