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

	require.True(t, exec.evaluateWhereForContext(ctx, "", nodes))
	require.False(t, exec.evaluateWhereForContext(ctx, "NOT a.age > 10", nodes))
	require.True(t, exec.evaluateWhereForContext(ctx, "a.age > 20 AND b.age < 30", nodes))
	require.True(t, exec.evaluateWhereForContext(ctx, "a.age < 20 OR b.age < 30", nodes))

	// Single-variable clauses route through evaluateWhere.
	require.True(t, exec.evaluateWhereForContext(ctx, "a.name = 'Ada'", nodes))
	require.False(t, exec.evaluateWhereForContext(ctx, "a.name = 'Unknown'", nodes))

	// Relationship existence predicates across two bound variables.
	require.True(t, exec.evaluateWhereForContext(ctx, "(a)-[:KNOWS]->(b)", nodes))
	require.True(t, exec.evaluateWhereForContext(ctx, "(b)<-[:KNOWS]-(a)", nodes))
	require.False(t, exec.evaluateWhereForContext(ctx, "(a)-[:LIKES]->(b)", nodes))

	// Missing node in a relationship predicate should return false.
	require.False(t, exec.evaluateWhereForContext(ctx, "(a)-[:KNOWS]->(c)", nodes))

	// Fallback expression evaluation branch.
	require.True(t, exec.evaluateWhereForContext(ctx, "1 = 1", nodes))
	require.False(t, exec.evaluateWhereForContext(ctx, "'x'", nodes))
}

func TestIsNumericLiteral_Branches(t *testing.T) {
	require.False(t, isNumericLiteral(""))
	require.True(t, isNumericLiteral("12"))
	require.True(t, isNumericLiteral("-3.14"))
	require.False(t, isNumericLiteral("12a"))
}

func BenchmarkExecuteMultiMatchCompiledWhere(b *testing.B) {
	store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "multi_match_where_bench")
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
