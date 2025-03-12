package iceberg

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"

	"github.com/agnosticeng/objstr"
	"github.com/agnosticeng/objstr/types"
	osutils "github.com/agnosticeng/objstr/utils"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/sourcegraph/conc/iter"
)

func WriteMetadata(
	ctx context.Context,
	path *url.URL,
	md table.Metadata,
) error {
	var os = objstr.FromContextOrDefault(ctx)

	js, err := json.Marshal(md)

	if err != nil {
		return err
	}

	return osutils.CreateObject(ctx, os, path, js)
}

func FetchLatestMetadata(ctx context.Context, location *url.URL) (table.Metadata, error) {
	mds, err := FetchAllMetadata(ctx, location)

	if err != nil {
		return nil, err
	}

	if len(mds) == 0 {
		return nil, nil
	}

	return mds[len(mds)-1], nil
}

func FetchAllMetadata(ctx context.Context, location *url.URL) ([]table.Metadata, error) {
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

	return iter.MapErr(files, func(f **types.Object) (table.Metadata, error) {
		mdBytes, err := osutils.ReadObject(ctx, os, (*f).URL)

		if err != nil {
			return nil, err
		}

		md, err := table.ParseMetadataBytes(mdBytes)

		if err != nil {
			return nil, err
		}

		return md, nil
	})
}

func metadataFileName(sequenceNumber int64) string {
	return fmt.Sprintf("%012d-%s.metadata.json", sequenceNumber, uuid.Must(uuid.NewV7()))
}
