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

	fixtureAdmission696Context, fixtureAdmission696 := expressionTestContext(ctx, "")
	_ = fixtureAdmission696
	require.True(t, exec.evaluateWhereForContext(fixtureAdmission696, fixtureAdmission696Context, "", nodes))
	fixtureAdmission697Context, fixtureAdmission697 := expressionTestContext(ctx, "NOT a.age > 10")
	_ = fixtureAdmission697
	require.False(t, exec.evaluateWhereForContext(fixtureAdmission697, fixtureAdmission697Context, "NOT a.age > 10", nodes))
	fixtureAdmission698Context, fixtureAdmission698 := expressionTestContext(ctx, "a.age > 20 AND b.age < 30")
	_ = fixtureAdmission698
	require.True(t, exec.evaluateWhereForContext(fixtureAdmission698, fixtureAdmission698Context, "a.age > 20 AND b.age < 30", nodes))
	fixtureAdmission699Context, fixtureAdmission699 := expressionTestContext(ctx, "a.age < 20 OR b.age < 30")
	_ = fixtureAdmission699
	require.True(t, exec.evaluateWhereForContext(fixtureAdmission699, fixtureAdmission699Context, "a.age < 20 OR b.age < 30", nodes))

	// Single-variable clauses route through evaluateWhere.
	fixtureAdmission700Context, fixtureAdmission700 := expressionTestContext(ctx, "a.name = 'Ada'")
	_ = fixtureAdmission700
	require.True(t, exec.evaluateWhereForContext(fixtureAdmission700, fixtureAdmission700Context, "a.name = 'Ada'", nodes))
	fixtureAdmission701Context, fixtureAdmission701 := expressionTestContext(ctx, "a.name = 'Unknown'")
	_ = fixtureAdmission701
	require.False(t, exec.evaluateWhereForContext(fixtureAdmission701, fixtureAdmission701Context, "a.name = 'Unknown'", nodes))

	// Relationship existence predicates across two bound variables.
	fixtureAdmission702Context, fixtureAdmission702 := expressionTestContext(ctx, "(a)-[:KNOWS]->(b)")
	_ = fixtureAdmission702
	require.True(t, exec.evaluateWhereForContext(fixtureAdmission702, fixtureAdmission702Context, "(a)-[:KNOWS]->(b)", nodes))
	fixtureAdmission703Context, fixtureAdmission703 := expressionTestContext(ctx, "(b)<-[:KNOWS]-(a)")
	_ = fixtureAdmission703
	require.True(t, exec.evaluateWhereForContext(fixtureAdmission703, fixtureAdmission703Context, "(b)<-[:KNOWS]-(a)", nodes))
	fixtureAdmission704Context, fixtureAdmission704 := expressionTestContext(ctx, "(a)-[:LIKES]->(b)")
	_ = fixtureAdmission704
	require.False(t, exec.evaluateWhereForContext(fixtureAdmission704, fixtureAdmission704Context, "(a)-[:LIKES]->(b)", nodes))

	// Missing node in a relationship predicate should return false.
	fixtureAdmission705Context, fixtureAdmission705 := expressionTestContext(ctx, "(a)-[:KNOWS]->(c)")
	_ = fixtureAdmission705
	require.False(t, exec.evaluateWhereForContext(fixtureAdmission705, fixtureAdmission705Context, "(a)-[:KNOWS]->(c)", nodes))

	// Fallback expression evaluation branch.
	fixtureAdmission706Context, fixtureAdmission706 := expressionTestContext(ctx, "1 = 1")
	_ = fixtureAdmission706
	require.True(t, exec.evaluateWhereForContext(fixtureAdmission706, fixtureAdmission706Context, "1 = 1", nodes))
	fixtureAdmission707Context, fixtureAdmission707 := expressionTestContext(ctx, "'x'")
	_ = fixtureAdmission707
	require.False(t, exec.evaluateWhereForContext(fixtureAdmission707, fixtureAdmission707Context, "'x'", nodes))
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
