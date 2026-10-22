package cypher

import (
	"context"
	"strconv"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestEvaluateWhereForContext_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "where_ctx_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	aID, err := store.CreateNode(&storage.Node{ID: storage.NodeID("a"), Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Ada", "age": int64(30)}})
	require.NoError(t, err)
	bID, err := store.CreateNode(&storage.Node{ID: storage.NodeID("b"), Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob", "age": int64(25)}})
	require.NoError(t, err)
	err = store.CreateEdge(&storage.Edge{ID: storage.EdgeID("e1"), Type: "KNOWS", StartNode: aID, EndNode: bID, Properties: map[string]interface{}{}})
	require.NoError(t, err)

	nodes := map[string]*storage.Node{}
	nA, err := store.GetNode(aID)
	require.NoError(t, err)
	nB, err := store.GetNode(bID)
	require.NoError(t, err)
	nodes["a"] = nA
	nodes["b"] = nB

	fixtureAdmission704Context, fixtureAdmission704 := expressionTestContext(ctx, "")
	_ = fixtureAdmission704
	require.True(t, exec.evaluateWhereForContext(fixtureAdmission704, fixtureAdmission704Context, "", nodes))
	fixtureAdmission705Context, fixtureAdmission705 := expressionTestContext(ctx, "NOT a.age > 10")
	_ = fixtureAdmission705
	require.False(t, exec.evaluateWhereForContext(fixtureAdmission705, fixtureAdmission705Context, "NOT a.age > 10", nodes))
	fixtureAdmission706Context, fixtureAdmission706 := expressionTestContext(ctx, "a.age > 20 AND b.age < 30")
	_ = fixtureAdmission706
	require.True(t, exec.evaluateWhereForContext(fixtureAdmission706, fixtureAdmission706Context, "a.age > 20 AND b.age < 30", nodes))
	fixtureAdmission707Context, fixtureAdmission707 := expressionTestContext(ctx, "a.age < 20 OR b.age < 30")
	_ = fixtureAdmission707
	require.True(t, exec.evaluateWhereForContext(fixtureAdmission707, fixtureAdmission707Context, "a.age < 20 OR b.age < 30", nodes))

	// Single-variable clauses route through evaluateWhere.
	fixtureAdmission708Context, fixtureAdmission708 := expressionTestContext(ctx, "a.name = 'Ada'")
	_ = fixtureAdmission708
	require.True(t, exec.evaluateWhereForContext(fixtureAdmission708, fixtureAdmission708Context, "a.name = 'Ada'", nodes))
	fixtureAdmission709Context, fixtureAdmission709 := expressionTestContext(ctx, "a.name = 'Unknown'")
	_ = fixtureAdmission709
	require.False(t, exec.evaluateWhereForContext(fixtureAdmission709, fixtureAdmission709Context, "a.name = 'Unknown'", nodes))

	// Relationship existence predicates across two bound variables.
	fixtureAdmission710Context, fixtureAdmission710 := expressionTestContext(ctx, "(a)-[:KNOWS]->(b)")
	_ = fixtureAdmission710
	require.True(t, exec.evaluateWhereForContext(fixtureAdmission710, fixtureAdmission710Context, "(a)-[:KNOWS]->(b)", nodes))
	fixtureAdmission711Context, fixtureAdmission711 := expressionTestContext(ctx, "(b)<-[:KNOWS]-(a)")
	_ = fixtureAdmission711
	require.True(t, exec.evaluateWhereForContext(fixtureAdmission711, fixtureAdmission711Context, "(b)<-[:KNOWS]-(a)", nodes))
	fixtureAdmission712Context, fixtureAdmission712 := expressionTestContext(ctx, "(a)-[:LIKES]->(b)")
	_ = fixtureAdmission712
	require.False(t, exec.evaluateWhereForContext(fixtureAdmission712, fixtureAdmission712Context, "(a)-[:LIKES]->(b)", nodes))

	// Missing node in a relationship predicate should return false.
	fixtureAdmission713Context, fixtureAdmission713 := expressionTestContext(ctx, "(a)-[:KNOWS]->(c)")
	_ = fixtureAdmission713
	require.False(t, exec.evaluateWhereForContext(fixtureAdmission713, fixtureAdmission713Context, "(a)-[:KNOWS]->(c)", nodes))

	// Fallback expression evaluation branch.
	fixtureAdmission714Context, fixtureAdmission714 := expressionTestContext(ctx, "1 = 1")
	_ = fixtureAdmission714
	require.True(t, exec.evaluateWhereForContext(fixtureAdmission714, fixtureAdmission714Context, "1 = 1", nodes))
	fixtureAdmission715Context, fixtureAdmission715 := expressionTestContext(ctx, "'x'")
	_ = fixtureAdmission715
	require.False(t, exec.evaluateWhereForContext(fixtureAdmission715, fixtureAdmission715Context, "'x'", nodes))
}

func TestIsNumericLiteral_Branches(t *testing.T) {
	require.False(t, isNumericLiteral(""))
	require.True(t, isNumericLiteral("12"))
	require.True(t, isNumericLiteral("-3.14"))
	require.False(t, isNumericLiteral("12a"))
}

func BenchmarkExecuteMultiMatchCompiledWhere(b *testing.B) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(b), "multi_match_where_bench")
	exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	for i := 0; i < 64; i++ {
		_, err := store.CreateNode(&storage.Node{
			ID:         storage.NodeID("where-" + strconv.Itoa(i)),
			Labels:     []string{"MultiWhereBench"},
			Properties: map[string]interface{}{"age": int64(i)},
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	ctx := context.Background()
	query := "MATCH (a:MultiWhereBench), (b:MultiWhereBench) WHERE a.age >= 32 AND b.age >= 32 RETURN count(*) AS c"
	result, err := exec.Execute(ctx, query, nil)
	if err != nil {
		b.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0][0] != int64(1024) {
		b.Fatalf("got result %v, want count 1024", result.Rows)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := exec.Execute(ctx, query, nil); err != nil {
			b.Fatal(err)
		}
	}
}
