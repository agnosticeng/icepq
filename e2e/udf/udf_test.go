package udf

import (
	"context"
	"embed"
	_ "embed"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/agnosticeng/icepq/e2e/common"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

//go:embed *.sql
var sqlFiles embed.FS

func TestRunQueries(t *testing.T) {
	var ctx, cancel = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	bundlePath, err := filepath.Abs(filepath.Join("..", "..", "tmp", "bundle.tar.gz"))
	require.NoError(t, err)

	common.WithDockerSetup(ctx, t, bundlePath, func(t *testing.T, setup *common.DockerSetup) {
		conn, err := setup.CreateClickhouseClient(ctx, t)
		require.NoError(t, err)

		// run SQL file queries
		entries, err := sqlFiles.ReadDir(".")
		require.NoError(t, err)

		for _, entry := range entries {
			t.Run(entry.Name(), func(t *testing.T) {
				content, err := os.ReadFile(entry.Name())
				require.NoError(t, err)
				queries := lo.Compact(strings.Split(string(content), ";;"))

				for i, query := range queries {
					var queryName = strconv.FormatInt(int64(i), 10)

					if match := regexp.MustCompile(`\s*--\s(.+)`).FindStringSubmatch(query); len(match) >= 2 {
						queryName = match[1]
					}

					t.Run(queryName, func(t *testing.T) {
						err = conn.Exec(ctx, query)
						require.NoError(t, err)
					})
				}
			})
		}
	})
}
