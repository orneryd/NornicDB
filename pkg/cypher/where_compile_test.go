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

	fixtureAdmission1512Context, fixtureAdmission1512 := expressionTestContext(ctx, "n.sourceId IS NOT NULL")
	_ = fixtureAdmission1512Context
	fn, ok := exec.getCompiledSimpleWhere(fixtureAdmission1512, fixtureAdmission1512Context, "n", "n.sourceId IS NOT NULL")
	require.True(t, ok)
	require.True(t, fn(node))

	fixtureAdmission1513Context, fixtureAdmission1513 := expressionTestContext(ctx, "n.missing IS NULL")
	_ = fixtureAdmission1513Context
	fn, ok = exec.getCompiledSimpleWhere(fixtureAdmission1513, fixtureAdmission1513Context, "n", "n.missing IS NULL")
	require.True(t, ok)
	require.True(t, fn(node))

	fixtureAdmission1514Context, fixtureAdmission1514 := expressionTestContext(ctx, "n.sourceId = 'src-1'")
	_ = fixtureAdmission1514Context
	fn, ok = exec.getCompiledSimpleWhere(fixtureAdmission1514, fixtureAdmission1514Context, "n", "n.sourceId = 'src-1'")
	require.True(t, ok)
	require.True(t, fn(node))

	fixtureAdmission1515Context, fixtureAdmission1515 := expressionTestContext(ctx, "n.sourceId <> 'src-2'")
	_ = fixtureAdmission1515Context
	fn, ok = exec.getCompiledSimpleWhere(fixtureAdmission1515, fixtureAdmission1515Context, "n", "n.sourceId <> 'src-2'")
	require.True(t, ok)
	require.True(t, fn(node))

	fixtureAdmission1516Context, fixtureAdmission1516 := expressionTestContext(ctx, "n.sourceId != 'src-2'")
	_ = fixtureAdmission1516Context
	fn, ok = exec.getCompiledSimpleWhere(fixtureAdmission1516, fixtureAdmission1516Context, "n", "n.sourceId != 'src-2'")
	require.True(t, ok)
	require.True(t, fn(node))

	fixtureAdmission1517Context, fixtureAdmission1517 := expressionTestContext(ctx, "n.sourceId IN ['src-1','src-3']")
	_ = fixtureAdmission1517Context
	fn, ok = exec.getCompiledSimpleWhere(fixtureAdmission1517, fixtureAdmission1517Context, "n", "n.sourceId IN ['src-1','src-3']")
	require.True(t, ok)
	require.True(t, fn(node))

	fixtureAdmission1518Context, fixtureAdmission1518 := expressionTestContext(ctx, "n.count >= 2 AND n.count < 3")
	_ = fixtureAdmission1518Context
	fn, ok = exec.getCompiledSimpleWhere(fixtureAdmission1518, fixtureAdmission1518Context, "n", "n.count >= 2 AND n.count < 3")
	require.True(t, ok)
	require.True(t, fn(node))

	fixtureAdmission1519Context, fixtureAdmission1519 := expressionTestContext(ctx, "n.name CONTAINS 'entity'")
	_ = fixtureAdmission1519Context
	fn, ok = exec.getCompiledSimpleWhere(fixtureAdmission1519, fixtureAdmission1519Context, "n", "n.name CONTAINS 'entity'")
	require.True(t, ok)
	require.True(t, fn(node))

	fixtureAdmission1520Context, fixtureAdmission1520 := expressionTestContext(ctx, "n.name STARTS WITH 'alpha' OR n.count > 99")
	_ = fixtureAdmission1520Context
	fn, ok = exec.getCompiledSimpleWhere(fixtureAdmission1520, fixtureAdmission1520Context, "n", "n.name STARTS WITH 'alpha' OR n.count > 99")
	require.True(t, ok)
	require.True(t, fn(node))
}

func TestCompileSimpleWhere_UnsupportedPatterns(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	ctx := context.Background()

	fixtureAdmission1521Context, fixtureAdmission1521 := expressionTestContext(ctx, "size(n.sourceId) > 0")
	_ = fixtureAdmission1521Context
	_, ok := exec.getCompiledSimpleWhere(fixtureAdmission1521, fixtureAdmission1521Context, "n", "size(n.sourceId) > 0")
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
	var fixtureAdmission921 admittedRelationshipStatement
	ctx, fixtureAdmission921 = expressionTestContext(ctx, whereClause)
	_ = fixtureAdmission921

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if !exec.evaluateWhere(fixtureAdmission921, ctx, node, "n", whereClause) {
			b.Fatal("predicate should match")
		}
	}
}
