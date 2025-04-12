package expire_snapshots

// import (
// 	"net/url"
// 	"time"

// 	ice "github.com/agnosticeng/icepq/internal/iceberg"
// 	"github.com/urfave/cli/v2"
// )

// func Command() *cli.Command {
// 	return &cli.Command{
// 		Name:  "expire-snapshots",
// 		Usage: "expire-snapshots <location>",
// 		Flags: []cli.Flag{
// 			&cli.StringSliceFlag{Name: "prop"},
// 			&cli.IntFlag{Name: "retain-last"},
// 			&cli.TimestampFlag{Name: "older-than", Layout: time.RFC3339},
// 		},
// 		Action: func(ctx *cli.Context) error {
// 			var (
// 				props         = ice.ParseProperties(ctx.StringSlice("prop"))
// 				location, err = url.Parse(ctx.Args().Get(0))
// 				olderThan     = ctx.Timestamp("older-than")
// 				retainLast    = ctx.Int("retain-last")
// 			)

// 			if err != nil {
// 				return err
// 			}

// 			cat, err := ice.NewVersionHintCatalog(location.String())

// 			if err != nil {
// 				return err
// 			}

// 			t, err := cat.LoadTable(ctx.Context, nil, props)

// 			if err != nil {
// 				return err
// 			}

// 			var tx = t.NewTransaction()

// 			if err := tx.ExpireSnapshots(&retainLast, olderThan); err != nil {
// 				return err
// 			}

// 			_, err = tx.Commit(ctx.Context)
// 			return err
// 		},
// 	}
// }
