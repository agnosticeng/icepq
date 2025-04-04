package common

import (
	"context"
	_ "embed"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	miniotest "github.com/testcontainers/testcontainers-go/modules/minio"
	"github.com/testcontainers/testcontainers-go/network"
)

type DockerSetup struct {
	Network    *testcontainers.DockerNetwork
	Minio      *miniotest.MinioContainer
	ClickHouse testcontainers.Container
}

func (setup *DockerSetup) CreateClickhouseClient(ctx context.Context, t *testing.T) (driver.Conn, error) {
	clickhousPort, err := setup.ClickHouse.MappedPort(ctx, "9000")

	if err != nil {
		return nil, err
	}

	opts, err := clickhouse.ParseDSN(fmt.Sprintf("tcp://default:test@localhost:%d/default", clickhousPort.Int()))

	if err != nil {
		return nil, err
	}

	conn, err := clickhouse.Open(opts)

	if err != nil {
		return nil, err
	}

	pingCtx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

pingLoop:
	for {
		select {
		case <-pingCtx.Done():
			return nil, pingCtx.Err()
		case <-time.After(time.Second):
			t.Log("Trying to ping ClickHouse server...")
			err = conn.Ping(ctx)
			if err == nil {
				break pingLoop
			}
			return nil, err
		}
	}

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

	return conn, nil
}

func WithDockerSetup(
	ctx context.Context,
	t *testing.T,
	bundlePath string,
	f func(*testing.T, *DockerSetup),
) {
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

	f(t, &DockerSetup{
		Network:    net,
		Minio:      minioContainer,
		ClickHouse: clickhouseContainer,
	})
}
