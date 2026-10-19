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

func TestRebaseDeterminismUsesCanonicalCommaReader(t *testing.T) {
	exec := solR2CacheExecutor(t)
	ctx := context.Background()
	for _, tc := range []struct {
		query     string
		cacheable bool
	}{
		{"RETURN datetime.truncate('day', datetime('2020-01-01T12:00:00Z')) AS value, 'a,b' AS marker", true},
		{"RETURN datetime.truncate('day' /* comma, inside comment */) AS value", false},
	} {
		t.Run(tc.query, func(t *testing.T) {
			require.Equal(t, tc.cacheable, exec.canUseResultCache(ctx, &QueryInfo{IsReadOnly: true}, tc.query))
			first, err := exec.Execute(ctx, tc.query, nil)
			require.NoError(t, err)
			second, err := exec.Execute(ctx, tc.query, nil)
			require.NoError(t, err)
			if tc.cacheable {
				require.Same(t, first, second)
			} else {
				require.NotSame(t, first, second)
			}
		})
	}
}
