package reachable_files

import (
	"fmt"

	ice "github.com/agnosticeng/icepq/internal/iceberg"
	"github.com/agnosticeng/icepq/internal/io"
	"github.com/agnosticeng/objstr"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/sourcegraph/conc/iter"
	"github.com/urfave/cli/v2"
	_ "gocloud.dev/blob/s3blob"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:  "reachable-files",
		Usage: "<location>",
		Flags: []cli.Flag{
			&cli.BoolFlag{Name: "all-snapshots"},
			&cli.BoolFlag{Name: "data-only"},
		},
		Action: func(ctx *cli.Context) error {
			var (
				os             = objstr.FromContextOrDefault(ctx.Context)
				io             = io.NewObjectStoreIO(os)
				allSnapshots   = ctx.Bool("all-snapshots")
				dataOnly       = ctx.Bool("data-only")
				includeDeleted = ctx.Bool("include-deleted")
			)

			cat, err := ice.NewVersionHintCatalog(ctx.Args().Get(0))

			if err != nil {
				return err
			}

			t, err := cat.LoadTable(ctx.Context, nil, nil)

			if err != nil {
				return err
			}

			var snapshots []table.Snapshot

			if allSnapshots {
				snapshots = t.Metadata().Snapshots()
			} else {
				var snap = t.CurrentSnapshot()
				if snap != nil {
					snapshots = []table.Snapshot{*snap}
				}
			}

			sets, err := iter.MapErr(snapshots, func(snap *table.Snapshot) (mapset.Set[string], error) {
				var files = mapset.NewSet[string]()

				if !dataOnly {
					files.Add(snap.ManifestList)
				}

				mans, err := snap.Manifests(io)

				if err != nil {
					return nil, err
				}

				for _, man := range mans {
					if (man.ManifestContent() == iceberg.ManifestContentDeletes) && !includeDeleted {
						continue
					}

					if !dataOnly {
						files.Append(man.FilePath())
					}

					entries, err := man.FetchEntries(io, false)

					if err != nil {
						return nil, err
					}

					for _, entry := range entries {
						files.Add(entry.DataFile().FilePath())
					}
				}

				return files, nil
			})

			if err != nil {
				return err
			}

			var files = mapset.NewSet[string]()

			for _, set := range sets {
				files.Append(set.ToSlice()...)
			}

			if !dataOnly {
				files.Add(t.MetadataLocation())
			}

			for _, v := range files.ToSlice() {
				fmt.Println(v)
			}

			return nil
		},
	}
}
