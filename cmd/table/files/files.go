package files

import (
	"encoding/json"
	"fmt"
	"net/url"

	ice "github.com/agnosticeng/icepq/internal/iceberg"
	"github.com/agnosticeng/icepq/internal/io"
	"github.com/agnosticeng/objstr"
	"github.com/sourcegraph/conc/iter"
	"github.com/urfave/cli/v2"
	slogctx "github.com/veqryn/slog-context"
	_ "gocloud.dev/blob/s3blob"
)

type snapshotInfo struct {
	MetadataPath   string
	SequenceNumber int64
	SnapshotId     int64
	SnapshotPath   string
	Manifests      []manifestInfo
}

type manifestInfo struct {
	Path      string
	Content   string
	DataFiles []dataFileInfo
}

type dataFileInfo struct {
	Path   string
	Status int
}

func Command() *cli.Command {
	return &cli.Command{
		Name:  "files",
		Usage: "files <location>",
		Flags: []cli.Flag{
			&cli.BoolFlag{Name: "all"},
			&cli.BoolFlag{Name: "compact"},
		},
		Action: func(ctx *cli.Context) error {
			var (
				logger = slogctx.FromCtx(ctx.Context)
				os     = objstr.FromContextOrDefault(ctx.Context)
				io     = io.NewObjectStoreIO(os)
			)

			location, err := url.Parse(ctx.Args().Get(0))

			if err != nil {
				return err
			}

			var mds []*ice.MetadataFile

			if !ctx.Bool("all") {
				cat, err := ice.NewVersionHintCatalog(location.String())

				if err != nil {
					return err
				}

				t, err := cat.LoadTable(ctx.Context, nil, nil)

				if err != nil {
					return err
				}

				mds = append(mds, &ice.MetadataFile{
					Metadata: t.Metadata(),
				})
			} else {
				mds, err = ice.FetchAllMetadataFiles(ctx.Context, location)
			}

			if err != nil {
				return err
			}

			logger.Debug("metadata listing finished", "count", len(mds))

			var m = iter.Mapper[*ice.MetadataFile, *snapshotInfo]{
				MaxGoroutines: 100,
			}

			res, err := m.MapErr(mds, func(md **ice.MetadataFile) (*snapshotInfo, error) {
				var snap = (*md).CurrentSnapshot()

				if snap == nil {
					return nil, nil
				}

				var snapInfo = snapshotInfo{
					MetadataPath:   (*md).Path,
					SnapshotPath:   snap.ManifestList,
					SequenceNumber: snap.SequenceNumber,
					SnapshotId:     snap.SnapshotID,
				}

				manFiles, err := snap.Manifests(io)

				if err != nil {
					return nil, err
				}

				for _, manFile := range manFiles {
					var manInfo = manifestInfo{
						Path:    manFile.FilePath(),
						Content: manFile.ManifestContent().String(),
					}

					entries, err := manFile.FetchEntries(io, false)

					if err != nil {
						return nil, err
					}

					for _, entry := range entries {
						manInfo.DataFiles = append(manInfo.DataFiles, dataFileInfo{
							Path:   entry.DataFile().FilePath(),
							Status: int(entry.Status()),
						})
					}

					snapInfo.Manifests = append(snapInfo.Manifests, manInfo)
				}

				return &snapInfo, nil
			})

			if ctx.Bool("compact") {
				for _, snapInfo := range res {
					for _, manInfo := range snapInfo.Manifests {
						for _, dataFileInfo := range manInfo.DataFiles {
							fmt.Println(dataFileInfo.Path)
						}
					}
				}
			} else {
				js, _ := json.MarshalIndent(res, "", "    ")
				fmt.Println(string(js))
			}

			return nil
		},
	}
}
