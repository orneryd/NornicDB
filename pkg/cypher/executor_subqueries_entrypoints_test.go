package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestExecuteMatchWithCallProcedure_ErrorAndEmptyPaths(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "subq_call_proc_cov"))
	ctx := context.Background()

	_, err := exec.executeMatchWithCallProcedure(ctx, "RETURN 1")
	require.ErrorContains(t, err, "CALL not found")

	_, err = exec.executeMatchWithCallProcedure(ctx, "CALL db.labels()")
	require.ErrorContains(t, err, "MATCH not found before CALL")

	res, err := exec.executeMatchWithCallProcedure(ctx, "MATCH n CALL db.labels()")
	require.NoError(t, err)
	require.Empty(t, res.Rows)

	// No seed nodes: should return deterministic columns from YIELD.
	res, err = exec.executeMatchWithCallProcedure(ctx,
		"MATCH (n:Missing) CALL db.index.vector.queryNodes('idx', 10, n.embedding) YIELD node, score")
	require.NoError(t, err)
	require.Equal(t, []string{"node", "score"}, res.Columns)
	require.Empty(t, res.Rows)
}

func TestSubstituteBoundVariablesInCall_MoreValueTypes(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	node := &storage.Node{
		ID: "n1",
		Properties: map[string]interface{}{
			"embedding": []float64{0.1, 0.2},
			"title":     "hello",
			"age":       int64(7),
			"active":    true,
			"arr":       []interface{}{1, 2.5, int64(3)},
		},
	}

	call := "CALL p.demo(n.embedding, n.title, n.age, n.active, n.arr, 'n.title')"
	out := exec.substituteBoundVariablesInCall(call, map[string]*storage.Node{"n": node}, nil)

	require.True(t, strings.Contains(out, "[0.1, 0.2]"))
	require.True(t, strings.Contains(out, "'hello'"))
	require.True(t, strings.Contains(out, "7"))
	require.True(t, strings.Contains(out, "true"))
	require.True(t, strings.Contains(out, "[1, 2.5, 3]"))
	require.True(t, strings.Contains(out, "'n.title'"), "quoted token must not be substituted")
}

func TestExecuteMatchWithCallSubquery_ErrorAndHappyPaths(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "subq_call_entry_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	matched, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n", nil)
	require.NoError(t, err)
	require.Empty(t, matched.Rows)

	standalone, err := exec.Execute(ctx, "CALL { RETURN 1 AS x } RETURN x", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, standalone.Rows)

	// No outer seeds should still preserve trailing projection semantics.
	emptyRes, err := exec.Execute(ctx,
		"MATCH (n:Missing) CALL { WITH n RETURN n.name AS name } RETURN name", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"name"}, emptyRes.Columns)
	require.Empty(t, emptyRes.Rows)

	_, err = store.CreateNode(&storage.Node{ID: "p1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "alice"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "p2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "bob"}})
	require.NoError(t, err)

	res, err := exec.Execute(ctx,
		"MATCH (n:Person) CALL { WITH n RETURN n.name AS name } RETURN name ORDER BY name", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"name"}, res.Columns)
	require.Len(t, res.Rows, 2)
	require.Equal(t, "alice", res.Rows[0][0])
	require.Equal(t, "bob", res.Rows[1][0])
}
