package udf

import (
	"context"
	_ "embed"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/agnosticeng/icepq/e2e/common"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

func TestSelect(t *testing.T) {
	var ctx, cancel = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	bundlePath, err := filepath.Abs(filepath.Join("..", "..", "tmp", "bundle.tar.gz"))
	require.NoError(t, err)

	common.WithDockerSetup(ctx, t, bundlePath, func(t *testing.T, setup *common.DockerSetup) {
		conn, err := setup.CreateClickhouseClient(ctx, t)
		require.NoError(t, err)
		require.NoError(t, conn.Exec(ctx, `system reload functions`))
		require.NoError(t, conn.Exec(ctx, `
			insert into table function s3('http://minio:9000/test/test_01/data/{_partition_id}.parquet', 'minio', 'minio123')
			partition by file
			select
				rowNumberInAllBlocks() % 10 as file,
				*
			from generateRandom('
				date Date,
				name String,
				value Float64,
				values Array(UInt64),
				metadata Map(String, String)
			')
			limit 100000
			settings s3_create_new_file_on_insert=true
		`))
		require.NoError(t, conn.Exec(ctx, `
			select icepq_append('s3://test/test_01', [
				'0.parquet',
				'1.parquet',
				'2.parquet',
				'3.parquet',
				'4.parquet',
				'5.parquet',
				'6.parquet',
				'7.parquet',
				'8.parquet',
				'9.parquet'
			])
		`))

		pythonContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:    "python",
				Networks: []string{setup.Network.Name},
				Cmd:      []string{"sleep", "infinity"},
			},
			Started: true,
		})
		require.NoError(t, err)
		defer testcontainers.TerminateContainer(pythonContainer)
		requirementsPath, err := filepath.Abs(filepath.Join(".", "requirements.txt"))
		require.NoError(t, err)
		scriptPath, err := filepath.Abs(filepath.Join(".", "test_select.py"))
		require.NoError(t, err)
		require.NoError(t, pythonContainer.CopyFileToContainer(ctx, requirementsPath, "/requirements.txt", 0x700))
		require.NoError(t, pythonContainer.CopyFileToContainer(ctx, scriptPath, "/test_select.py", 0x700))
		common.ExecInContainer(ctx, t, pythonContainer, []string{"pip", "install", "-r", "/requirements.txt"})
		t.Log("installing requirements")
		common.ExecInContainer(ctx, t, pythonContainer, []string{"python", "/test_select.py"})
		t.Log("running test script")
	})
}
