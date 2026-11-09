package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A malformed contract entry is a syntax error on every route that reads the
// statement without running it: EXPLAIN reports it, and nothing is stored.
func TestExplainRejectsMalformedContractEntries(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "contracts"))
	ctx := context.Background()
	for _, statement := range []string{
		"CREATE CONSTRAINT c2 FOR (n:Doc) REQUIRE { n.age > }",
		"CREATE CONSTRAINT c3 FOR (n:Doc) REQUIRE { n.age + }",
	} {
		_, err := executor.Execute(ctx, "EXPLAIN "+statement, nil)
		require.Error(t, err, statement)
		require.Contains(t, err.Error(), "syntax error", statement)
		_, err = executor.Execute(ctx, statement, nil)
		require.Error(t, err, statement)
	}
	result, err := executor.Execute(ctx, "SHOW CONSTRAINTS", nil)
	require.NoError(t, err)
	require.Empty(t, result.Rows)
}
