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

	fixtureAdmission1524Context, fixtureAdmission1524 := expressionTestContext(ctx, "n.sourceId IS NOT NULL")
	_ = fixtureAdmission1524Context
	fn, ok := exec.getCompiledSimpleWhere(fixtureAdmission1524, fixtureAdmission1524Context, "n", "n.sourceId IS NOT NULL")
	require.True(t, ok)
	require.True(t, fn(node))

	fixtureAdmission1525Context, fixtureAdmission1525 := expressionTestContext(ctx, "n.missing IS NULL")
	_ = fixtureAdmission1525Context
	fn, ok = exec.getCompiledSimpleWhere(fixtureAdmission1525, fixtureAdmission1525Context, "n", "n.missing IS NULL")
	require.True(t, ok)
	require.True(t, fn(node))

	fixtureAdmission1526Context, fixtureAdmission1526 := expressionTestContext(ctx, "n.sourceId = 'src-1'")
	_ = fixtureAdmission1526Context
	fn, ok = exec.getCompiledSimpleWhere(fixtureAdmission1526, fixtureAdmission1526Context, "n", "n.sourceId = 'src-1'")
	require.True(t, ok)
	require.True(t, fn(node))

	fixtureAdmission1527Context, fixtureAdmission1527 := expressionTestContext(ctx, "n.sourceId <> 'src-2'")
	_ = fixtureAdmission1527Context
	fn, ok = exec.getCompiledSimpleWhere(fixtureAdmission1527, fixtureAdmission1527Context, "n", "n.sourceId <> 'src-2'")
	require.True(t, ok)
	require.True(t, fn(node))

	fixtureAdmission1528Context, fixtureAdmission1528 := expressionTestContext(ctx, "n.sourceId != 'src-2'")
	_ = fixtureAdmission1528Context
	fn, ok = exec.getCompiledSimpleWhere(fixtureAdmission1528, fixtureAdmission1528Context, "n", "n.sourceId != 'src-2'")
	require.True(t, ok)
	require.True(t, fn(node))

	fixtureAdmission1529Context, fixtureAdmission1529 := expressionTestContext(ctx, "n.sourceId IN ['src-1','src-3']")
	_ = fixtureAdmission1529Context
	fn, ok = exec.getCompiledSimpleWhere(fixtureAdmission1529, fixtureAdmission1529Context, "n", "n.sourceId IN ['src-1','src-3']")
	require.True(t, ok)
	require.True(t, fn(node))

	fixtureAdmission1530Context, fixtureAdmission1530 := expressionTestContext(ctx, "n.count >= 2 AND n.count < 3")
	_ = fixtureAdmission1530Context
	fn, ok = exec.getCompiledSimpleWhere(fixtureAdmission1530, fixtureAdmission1530Context, "n", "n.count >= 2 AND n.count < 3")
	require.True(t, ok)
	require.True(t, fn(node))

	fixtureAdmission1531Context, fixtureAdmission1531 := expressionTestContext(ctx, "n.name CONTAINS 'entity'")
	_ = fixtureAdmission1531Context
	fn, ok = exec.getCompiledSimpleWhere(fixtureAdmission1531, fixtureAdmission1531Context, "n", "n.name CONTAINS 'entity'")
	require.True(t, ok)
	require.True(t, fn(node))

	fixtureAdmission1532Context, fixtureAdmission1532 := expressionTestContext(ctx, "n.name STARTS WITH 'alpha' OR n.count > 99")
	_ = fixtureAdmission1532Context
	fn, ok = exec.getCompiledSimpleWhere(fixtureAdmission1532, fixtureAdmission1532Context, "n", "n.name STARTS WITH 'alpha' OR n.count > 99")
	require.True(t, ok)
	require.True(t, fn(node))
}

func TestCompileSimpleWhere_UnsupportedPatterns(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	ctx := context.Background()

	fixtureAdmission1533Context, fixtureAdmission1533 := expressionTestContext(ctx, "size(n.sourceId) > 0")
	_ = fixtureAdmission1533Context
	_, ok := exec.getCompiledSimpleWhere(fixtureAdmission1533, fixtureAdmission1533Context, "n", "size(n.sourceId) > 0")
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
	var fixtureAdmission932 admittedRelationshipStatement
	ctx, fixtureAdmission932 = expressionTestContext(ctx, whereClause)
	_ = fixtureAdmission932

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if !exec.evaluateWhere(fixtureAdmission932, ctx, node, "n", whereClause) {
			b.Fatal("predicate should match")
		}
	}
}
