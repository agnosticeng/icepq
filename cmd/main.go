package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/agnosticeng/cliutils"
	"github.com/agnosticeng/cnf"
	"github.com/agnosticeng/cnf/providers/env"
	"github.com/agnosticeng/icepq/cmd/avro"
	"github.com/agnosticeng/icepq/cmd/clickhouse"
	"github.com/agnosticeng/icepq/cmd/schema"
	"github.com/agnosticeng/icepq/cmd/table"
	objstrcli "github.com/agnosticeng/objstr/cli"
	"github.com/agnosticeng/panicsafe"
	"github.com/agnosticeng/slogcli"
	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.App{
		Name:  "icepq",
		Flags: slogcli.SlogFlags(),
		Before: cliutils.CombineBeforeFuncs(
			slogcli.SlogBefore,
			objstrcli.ObjStrBefore(cnf.WithProvider(env.NewEnvProvider("OBJSTR"))),
		),
		After: cliutils.CombineAfterFuncs(
			objstrcli.ObjStrAfter,
			slogcli.SlogAfter,
		),
		Commands: []*cli.Command{
			schema.Command(),
			table.Command(),
			avro.Command(),
			clickhouse.Command(),
		},
	}

	var err = panicsafe.Recover(func() error { return app.Run(os.Args) })

	if err != nil {
		slog.Error(fmt.Sprintf("%v", err))
		os.Exit(1)
	}
}
