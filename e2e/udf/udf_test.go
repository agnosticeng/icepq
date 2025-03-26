package e2e

import (
	"context"
	"embed"
	_ "embed"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	miniotest "github.com/testcontainers/testcontainers-go/modules/minio"
	"github.com/testcontainers/testcontainers-go/network"
)

//go:embed *.sql
var sqlFiles embed.FS

func TestE2E(t *testing.T) {
	var ctx, cancel = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// setup network
	net, err := network.New(ctx)
	require.NoError(t, err)
	testcontainers.CleanupNetwork(t, net)

	// start minion container
	minioContainer, err := miniotest.Run(
		ctx,
		"minio/minio:RELEASE.2024-01-16T16-07-38Z",
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Networks: []string{net.Name},
				NetworkAliases: map[string][]string{
					net.Name: {"minio"},
				},
			},
		}),
		miniotest.WithUsername("minio"),
		miniotest.WithPassword("minio123"),
	)
	require.NoError(t, err)
	defer testcontainers.TerminateContainer(minioContainer)

	// create test bucket
	url, err := minioContainer.ConnectionString(ctx)
	require.NoError(t, err)
	minioClient, err := minio.New(url, &minio.Options{
		Creds:  credentials.NewStaticV4(minioContainer.Username, minioContainer.Password, ""),
		Secure: false,
	})
	require.NoError(t, err)
	err = minioClient.MakeBucket(ctx, "test", minio.MakeBucketOptions{})
	require.NoError(t, err)

	// start clickhouse-server container and mount UDF bundle
	bundlePath, err := filepath.Abs(filepath.Join("..", "..", "tmp", "bundle.tar.gz"))
	require.NoError(t, err)
	bundleReader, err := os.Open(bundlePath)
	require.NoError(t, err)
	defer bundleReader.Close()
	clickhouseContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:    "clickhouse/clickhouse-server:25.3",
			Networks: []string{net.Name},
			NetworkAliases: map[string][]string{
				net.Name: {"clickhouse"},
			},
			Env: map[string]string{
				"CLICKHOUSE_PASSWORD":           "test",
				"OBJSTR__S3__REGION":            "us-east-1",
				"OBJSTR__S3__ACCESS_KEY_ID":     "minio",
				"OBJSTR__S3__SECRET_ACCESS_KEY": "minio123",
				"OBJSTR__S3__DISABLE_SSL":       "true",
				"OBJSTR__S3__FORCE_PATH_STYLE":  "true",
				"OBJSTR__S3__ENDPOINT":          "http://minio:9000",
			},
			Files: []testcontainers.ContainerFile{
				{
					Reader:            bundleReader,
					HostFilePath:      bundlePath,
					ContainerFilePath: "/bundle.tar.gz",
					FileMode:          700,
				},
			},
		},
		Started: true,
	})
	require.NoError(t, err)
	defer testcontainers.TerminateContainer(clickhouseContainer)

	// install UDF bundle inside clickhouse-server container
	c, _, err := clickhouseContainer.Exec(ctx, []string{"tar", "-xvzf", "/bundle.tar.gz", "-C", "/"})
	require.NoError(t, err)
	require.Zero(t, c)

	// create clickhouse client
	clickhousPort, err := clickhouseContainer.MappedPort(ctx, "9000")
	require.NoError(t, err)
	opts, err := clickhouse.ParseDSN(fmt.Sprintf("tcp://default:test@localhost:%d/default", clickhousPort.Int()))
	require.NoError(t, err)
	conn, err := clickhouse.Open(opts)
	require.NoError(t, err)
	defer conn.Close()
	err = conn.Ping(ctx)
	require.NoError(t, err)
	ctx = clickhouse.Context(
		ctx,
		clickhouse.WithSettings(clickhouse.Settings{
			"send_logs_level": "debug",
		}),
		clickhouse.WithLogs(func(l *clickhouse.Log) {
			if strings.Contains(l.Text, "Executable generates stderr:") {
				t.Log(l.Text)
			}
		}),
	)

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
}
