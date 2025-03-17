package files

import (
	"encoding/json"
	"fmt"
	"net/url"

	ice "github.com/agnosticeng/icepq/internal/iceberg"
	"github.com/apache/iceberg-go/table"
	"github.com/samber/lo"
	"github.com/sourcegraph/conc/iter"
	"github.com/urfave/cli/v2"
	slogctx "github.com/veqryn/slog-context"
	_ "gocloud.dev/blob/s3blob"
)

type snapshotInfo struct {
	SequenceNumber int64
	SnapshotId     int64
	Path           string
	Manifests      []manifestInfo
}

type manifestInfo struct {
	Path      string
	DataFiles []dataFileInfo
}

type dataFileInfo struct {
	Path string
}

func Command() *cli.Command {
	return &cli.Command{
		Name:  "files",
		Usage: "files <location>",
		Action: func(ctx *cli.Context) error {
			var logger = slogctx.FromCtx(ctx.Context)

			location, err := url.Parse(ctx.Args().Get(0))

			if err != nil {
				return err
			}

			mds, err := ice.FetchAllMetadata(ctx.Context, location)

			if err != nil {
				return err
			}

			logger.Debug("metadata listing finished", "count", len(mds))

			var m = iter.Mapper[table.Metadata, *snapshotInfo]{
				MaxGoroutines: 100,
			}

			res, err := m.MapErr(mds, func(md *table.Metadata) (*snapshotInfo, error) {
				var snap = (*md).CurrentSnapshot()

				if snap == nil {
					return nil, nil
				}

				var snapInfo = snapshotInfo{
					Path:           snap.ManifestList,
					SequenceNumber: snap.SequenceNumber,
					SnapshotId:     snap.SnapshotID,
				}

				manFiles, err := ice.FetchManifestsWithEntries(ctx.Context, lo.Must(url.Parse(snap.ManifestList)))

				if err != nil {
					return nil, err
				}

				for _, manFile := range manFiles {
					var manInfo = manifestInfo{
						Path: manFile.Manifest.FilePath(),
					}

					for _, entry := range manFile.Entries {
						manInfo.DataFiles = append(manInfo.DataFiles, dataFileInfo{
							Path: entry.DataFile().FilePath(),
						})
					}

					snapInfo.Manifests = append(snapInfo.Manifests, manInfo)
				}

				fmt.Println("snap", snapInfo.Path)
				return &snapInfo, nil
			})

			js, _ := json.MarshalIndent(res, "", "    ")
			fmt.Println(string(js))
			return nil
		},
	}
}
