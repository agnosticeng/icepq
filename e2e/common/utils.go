package common

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/exec"
)

func ExecInContainer(ctx context.Context, t *testing.T, container testcontainers.Container, cmd []string) {
	c, output, err := container.Exec(ctx, cmd, exec.Multiplexed())

	if err != nil || c != 0 {
		content, err := io.ReadAll(output)
		require.NoError(t, err)
		t.Log("command output", string(content))
	}

	require.NoError(t, err)
	require.Zero(t, c)
}
