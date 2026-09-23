package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnwindOptionalMatchUsesRowPropertyInPattern(t *testing.T) {
	executor, ctx := newUnitExecutor(t)
	_, err := executor.Execute(ctx, "CREATE (:Target {key: 1}), (:Target {key: 2})", nil)
	require.NoError(t, err)

	result, err := executor.Execute(ctx, `
		UNWIND [1, 2, 3] AS key
		OPTIONAL MATCH (target:Target {key: key})
		RETURN key, target.key AS matchedKey
		ORDER BY key
	`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{
		{int64(1), int64(1)},
		{int64(2), int64(2)},
		{int64(3), nil},
	}, result.Rows)
}
