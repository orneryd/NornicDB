package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCompileSimpleWhere_SupportedPatterns(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	node := &storage.Node{
		ID: storage.NodeID("n1"),
		Properties: map[string]interface{}{
			"sourceId": "src-1",
			"count":    int64(2),
			"name":     "alpha entity",
		},
	}
	ctx := context.Background()

	fn, ok := exec.getCompiledSimpleWhere(ctx, "n", expressionBraceView("n.sourceId IS NOT NULL"))
	require.True(t, ok)
	require.True(t, fn(node))

	fn, ok = exec.getCompiledSimpleWhere(ctx, "n", expressionBraceView("n.missing IS NULL"))
	require.True(t, ok)
	require.True(t, fn(node))

	fn, ok = exec.getCompiledSimpleWhere(ctx, "n", expressionBraceView("n.sourceId = 'src-1'"))
	require.True(t, ok)
	require.True(t, fn(node))

	fn, ok = exec.getCompiledSimpleWhere(ctx, "n", expressionBraceView("n.sourceId <> 'src-2'"))
	require.True(t, ok)
	require.True(t, fn(node))

	fn, ok = exec.getCompiledSimpleWhere(ctx, "n", expressionBraceView("n.sourceId != 'src-2'"))
	require.True(t, ok)
	require.True(t, fn(node))

	fn, ok = exec.getCompiledSimpleWhere(ctx, "n", expressionBraceView("n.sourceId IN ['src-1','src-3']"))
	require.True(t, ok)
	require.True(t, fn(node))

	fn, ok = exec.getCompiledSimpleWhere(ctx, "n", expressionBraceView("n.count >= 2 AND n.count < 3"))
	require.True(t, ok)
	require.True(t, fn(node))

	fn, ok = exec.getCompiledSimpleWhere(ctx, "n", expressionBraceView("n.name CONTAINS 'entity'"))
	require.True(t, ok)
	require.True(t, fn(node))

	fn, ok = exec.getCompiledSimpleWhere(ctx, "n", expressionBraceView("n.name STARTS WITH 'alpha' OR n.count > 99"))
	require.True(t, ok)
	require.True(t, fn(node))
}

func TestCompileSimpleWhere_UnsupportedPatterns(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	ctx := context.Background()

	_, ok := exec.getCompiledSimpleWhere(ctx, "n", expressionBraceView("size(n.sourceId) > 0"))
	require.False(t, ok)
}

func BenchmarkEvaluateWhereCompiledCandidatePredicate(b *testing.B) {
	exec := NewStorageExecutor(newTestMemoryEngine(b))
	ctx := context.Background()
	node := &storage.Node{
		ID:     "n1",
		Labels: []string{"Entity"},
		Properties: map[string]interface{}{
			"count": int64(42),
			"name":  "alpha entity",
		},
	}
	whereClause := "n:Entity AND n.count >= 40 AND n.name CONTAINS 'entity'"

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if !exec.evaluateWhere(ctx, node, "n", originalBraceView(whereClause)) {
			b.Fatal("predicate should match")
		}
	}
}
