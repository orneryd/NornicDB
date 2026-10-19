package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRebaseNativeAPOCUUIDRemainsVolatile(t *testing.T) {
	exec := solR2CacheExecutor(t)
	first, err := exec.Execute(context.Background(), "RETURN apoc.create.uuid() AS value", nil)
	require.NoError(t, err)
	second, err := exec.Execute(context.Background(), "RETURN apoc.create.uuid() AS value", nil)
	require.NoError(t, err)
	require.Len(t, first.Rows, 1)
	require.Len(t, second.Rows, 1)
	require.NotEqual(t, first.Rows[0][0], second.Rows[0][0], "native APOC metadata must preserve the UUID cache exclusion")
}
