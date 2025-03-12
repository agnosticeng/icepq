package files

import (
	"encoding/json"
	"fmt"
	"net/url"

	ice "github.com/agnosticeng/icepq/internal/iceberg"
	"github.com/agnosticeng/icepq/internal/io"
	"github.com/agnosticeng/objstr"
	"github.com/samber/lo"
	"github.com/urfave/cli/v2"
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
			var (
				os  = objstr.FromContextOrDefault(ctx.Context)
				fs  = io.NewObjectStorageAdapter(os)
				res []snapshotInfo
			)

			location, err := url.Parse(ctx.Args().Get(0))

			if err != nil {
				return err
			}

			mds, err := ice.FetchAllMetadata(ctx.Context, location)

			if err != nil {
				return err
			}

			for _, md := range mds {
				if md.CurrentSnapshot() == nil {
					continue
				}

				var snapInfo = snapshotInfo{
					Path:           md.CurrentSnapshot().ManifestList,
					SequenceNumber: md.CurrentSnapshot().SequenceNumber,
					SnapshotId:     md.CurrentSnapshot().SnapshotID,
				}

				if md.CurrentSnapshot() == nil {
					continue
				}

				manFiles, err := ice.ReadManifestList(ctx.Context, lo.Must(url.Parse(md.CurrentSnapshot().ManifestList)))

				if err != nil {
					return err
				}

				for _, manFile := range manFiles {
					var manInfo = manifestInfo{
						Path: manFile.FilePath(),
					}

					entries, err := manFile.FetchEntries(fs, false)

					if err != nil {
						return err
					}

					for _, entry := range entries {
						manInfo.DataFiles = append(manInfo.DataFiles, dataFileInfo{
							Path: entry.DataFile().FilePath(),
						})
					}

					snapInfo.Manifests = append(snapInfo.Manifests, manInfo)
				}

				res = append(res, snapInfo)
			}

			js, _ := json.MarshalIndent(res, "", "    ")
			fmt.Println(string(js))
			return nil
		},
	}
}
