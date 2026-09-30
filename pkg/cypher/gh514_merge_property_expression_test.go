package cypher

// gh514_merge_property_expression_test.go — regression tests for the remaining
// #514 case: a MERGE property map expression over a variable bound by an
// earlier clause must store the VALUE, not the expression text, including in
// statements that also contain a SET. Also pins the general evaluator rule:
// an unevaluable expression is an error, never stored text.

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func newGh514Executor(t *testing.T) *StorageExecutor {
	t.Helper()
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "gh514")
	return NewStorageExecutor(store)
}

func TestGh514_MergePropertyValueAfterWith(t *testing.T) {
	exec := newGh514Executor(t)
	ctx := context.Background()

	result, err := exec.Execute(ctx, "WITH {id: 1} AS a MERGE (b:M {k: a.id}) RETURN b.k AS k", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows, "the value, not the text 'a.id'")

	stored, err := exec.Execute(ctx, "MATCH (b:M) RETURN b.k AS k", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, stored.Rows)
}

func TestGh514_MergePropertyValueAfterWithAndSet(t *testing.T) {
	exec := newGh514Executor(t)
	ctx := context.Background()

	// The issue's statement family: WITH binds a, the statement also has a SET.
	result, err := exec.Execute(ctx, "WITH {id: 7} AS a MERGE (b:M {k: a.id}) ON CREATE SET b.from = a.id RETURN b.k AS k", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows)

	stored, err := exec.Execute(ctx, "MATCH (b:M) RETURN b.k AS k, b.from AS f", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(7), int64(7)}}, stored.Rows)
}

func TestGh514_MergeBoundNodePropertyAfterSetWith(t *testing.T) {
	exec := newGh514Executor(t)
	result, err := exec.Execute(context.Background(), "MERGE (a:T {id: 1}) SET a.q = 1 WITH a MERGE (b:X {k: a.id}) RETURN b.k AS k", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	stored, err := exec.Execute(context.Background(), "MATCH (b:X) RETURN b.k AS k", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, stored.Rows)
}

func TestGh514_UnevaluableExpressionsAreErrorsNotText(t *testing.T) {
	exec := newGh514Executor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (p:P {id: 1, name: 'a'})", nil)
	require.NoError(t, err)

	for _, stmt := range []string{
		"MATCH (p:P) SET p.x = 1 +* 2 RETURN p.x AS x",
		"MATCH (p:P) SET p.x = p.name.. RETURN p.x AS x",
	} {
		_, err := exec.Execute(ctx, stmt, nil)
		require.Error(t, err, "statement must be rejected, not store text: %s", stmt)
	}

	// Nothing may have been stored by the rejected statements.
	stored, err := exec.Execute(ctx, "MATCH (p:P) RETURN properties(p) AS props", nil)
	require.NoError(t, err)
	props, ok := stored.Rows[0][0].(map[string]interface{})
	require.True(t, ok)
	_, hasX := props["x"]
	require.False(t, hasX, "no x property may exist after rejected SETs")
}
