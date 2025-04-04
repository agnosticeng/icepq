package common

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

func ExecInContainer(ctx context.Context, t *testing.T, container testcontainers.Container, cmd []string) {
	c, _, err := container.Exec(ctx, []string{"pip", "install", "duckdb"})
	require.NoError(t, err)
	require.Zero(t, c)
}
