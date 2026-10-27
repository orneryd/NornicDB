package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestExecuteCallTail_GuardsAndEmptySeedRows(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call_tail_exec_guards"))
	ctx := context.Background()

	_, err := exec.executeCallTail(ctx, nil, "RETURN 1 AS v")
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires seed result")

	seed := &ExecuteResult{Columns: []string{"v"}, Rows: [][]interface{}{{int64(1)}}}
	res, err := exec.executeCallTail(ctx, seed, "   ")
	require.NoError(t, err)
	require.Same(t, seed, res)

	emptySeed := &ExecuteResult{Columns: []string{"v"}, Rows: [][]interface{}{}}
	res, err = exec.executeCallTail(ctx, emptySeed, "WITH v RETURN v AS renamed ORDER BY renamed")
	require.NoError(t, err)
	require.Equal(t, []string{"renamed"}, res.Columns)
	require.Empty(t, res.Rows)

	res, err = exec.executeCallTail(ctx, emptySeed, "WITH v")
	require.NoError(t, err)
	require.Empty(t, res.Columns)
	require.Empty(t, res.Rows)
}

func TestExecuteCallTailParallel_BranchesAndErrorPropagation(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call_tail_exec_parallel"))
	ctx := context.Background()
	seed := &ExecuteResult{Columns: []string{"v"}, Rows: [][]interface{}{{int64(2)}, {int64(1)}}}

	res, ok, err := exec.executeCallTailParallel(ctx, seed, "RETURN v AS out", nil, nil)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, res)

	single := &ExecuteResult{Columns: []string{"v"}, Rows: [][]interface{}{{int64(2)}}}
	res, ok, err = exec.executeCallTailParallel(ctx, single, "RETURN v AS out", []int{0}, nil)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, res)

	res, ok, err = exec.executeCallTailParallel(ctx, seed, "RETURN v AS out", []int{0}, []string{"renamed"})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []string{"renamed"}, res.Columns)
	require.Equal(t, [][]interface{}{{int64(2)}, {int64(1)}}, res.Rows)

	_, ok, err = exec.executeCallTailParallel(ctx, seed, "RETURN (", []int{0}, nil)
	require.True(t, ok)
	require.Error(t, err)
}

func TestExecuteCallTailSingleRow_NodeAndScalarBindings(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "call_tail_exec_single")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         storage.NodeID("n1"),
		Labels:     []string{"N"},
		Properties: map[string]interface{}{"id": "n1", "name": "first"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)

	seedCols := []string{"n", "score"}
	res, err := exec.executeCallTailSingleRow(
		ctx,
		seedCols,
		[]interface{}{node, int64(7)},
		"MATCH (n) RETURN n.id AS id, score",
		[]int{0, 1},
		[]string{"id", "score"},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"id", "score"}, res.Columns)
	require.Equal(t, [][]interface{}{{"n1", int64(7)}}, res.Rows)

	res, err = exec.executeCallTailSingleRow(
		ctx,
		seedCols,
		[]interface{}{node, int64(8)},
		"RETURN n.id AS id, score",
		[]int{0, 1},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"n1", int64(8)}}, res.Rows)

	res, err = exec.executeCallTailSingleRow(
		ctx,
		seedCols,
		[]interface{}{node},
		"RETURN 1 AS one",
		[]int{2},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, []string{"one"}, res.Columns)
	require.Equal(t, [][]interface{}{{int64(1)}}, res.Rows)
}

func TestSplitCallAndTail_AndExpectedReturnColumns(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "headers"))
	split := splitCallAndTail(t.Context(), "MATCH (n) RETURN n")
	require.Equal(t, "MATCH (n) RETURN n", split.callOnly)
	require.Empty(t, split.tail)

	split = splitCallAndTail(t.Context(), "CALL db.labels() YIELD label CREATE (:Seen {name:label})")
	require.Equal(t, "CALL db.labels() YIELD label", split.callOnly)
	require.Equal(t, "CREATE (:Seen {name:label})", split.tail)

	split = splitCallAndTail(t.Context(), "CALL db.labels() YIELD label ORDER BY label")
	require.Equal(t, "CALL db.labels() YIELD label ORDER BY label", split.callOnly)
	require.Empty(t, split.tail)

	cols := exec.StatementColumns("MATCH (n) WITH n RETURN n AS node, count(*) AS c ORDER BY c DESC SKIP 1 LIMIT 2")
	require.Equal(t, []string{"node", "c"}, cols)

	_, err := exec.StatementResultColumns("WITH n RETURN n AS node, count(*) AS c", RawSource)
	require.ErrorContains(t, err, "Variable `n` not defined")
	requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")

	cols = exec.StatementColumns("WITH n")
	require.Nil(t, cols)
}
