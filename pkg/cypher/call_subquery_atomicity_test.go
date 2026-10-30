package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// CALL {} subqueries (and other internal executions) must not re-enter Execute()
// and start nested implicit transactions. Otherwise, a failure after the inner
// write can commit partial data even though the outer statement returns an error.
func TestCallSubquery_NoNestedImplicitTransactionOnError(t *testing.T) {
	store := newTestMemoryEngine(t)
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// This is intentionally unsupported: we execute an inner write, then fail during
	// post-processing (WITH after CALL {} isn't supported by processAfterCallSubquery).
	_, err := exec.Execute(ctx, `
		CALL { CREATE (n:Temp {id: '1'}) RETURN n }
		WITH n
		RETURN n
	`, nil)
	require.Error(t, err)

	// The inner CREATE must not have committed.
	result, err := exec.Execute(ctx, `MATCH (n:Temp {id: '1'}) RETURN count(n) AS cnt`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, int64(0), result.Rows[0][0])
}
