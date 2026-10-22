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

	fixtureAdmission120Context, fixtureAdmission120 := statementTestContext(ctx, "RETURN 1 AS v")
	_ = fixtureAdmission120
	_, err := exec.executeCallTail(fixtureAdmission120, fixtureAdmission120Context, nil, "RETURN 1 AS v")
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires seed result")

	seed := &ExecuteResult{Columns: []string{"v"}, Rows: [][]interface{}{{int64(1)}}}
	fixtureAdmission121Context, fixtureAdmission121 := statementTestContext(ctx, "   ")
	_ = fixtureAdmission121
	res, err := exec.executeCallTail(fixtureAdmission121, fixtureAdmission121Context, seed, "   ")
	require.NoError(t, err)
	require.Same(t, seed, res)

	emptySeed := &ExecuteResult{Columns: []string{"v"}, Rows: [][]interface{}{}}
	fixtureAdmission122Context, fixtureAdmission122 := statementTestContext(ctx, "WITH v RETURN v AS renamed ORDER BY renamed")
	_ = fixtureAdmission122
	res, err = exec.executeCallTail(fixtureAdmission122, fixtureAdmission122Context, emptySeed, "WITH v RETURN v AS renamed ORDER BY renamed")
	require.NoError(t, err)
	require.Equal(t, []string{"renamed"}, res.Columns)
	require.Empty(t, res.Rows)

	fixtureAdmission123Context, fixtureAdmission123 := statementTestContext(ctx, "WITH v")
	_ = fixtureAdmission123
	res, err = exec.executeCallTail(fixtureAdmission123, fixtureAdmission123Context, emptySeed, "WITH v")
	require.NoError(t, err)
	require.Empty(t, res.Columns)
	require.Empty(t, res.Rows)
}

func TestExecuteCallTailParallel_BranchesAndErrorPropagation(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call_tail_exec_parallel"))
	ctx := context.Background()
	seed := &ExecuteResult{Columns: []string{"v"}, Rows: [][]interface{}{{int64(2)}, {int64(1)}}}

	fixtureAdmission124Context, fixtureAdmission124 := statementTestContext(ctx, "RETURN v AS out")
	_ = fixtureAdmission124
	res, ok, err := exec.executeCallTailParallel(fixtureAdmission124, fixtureAdmission124Context, seed, "RETURN v AS out", nil, nil)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, res)

	single := &ExecuteResult{Columns: []string{"v"}, Rows: [][]interface{}{{int64(2)}}}
	fixtureAdmission125Context, fixtureAdmission125 := statementTestContext(ctx, "RETURN v AS out")
	_ = fixtureAdmission125
	res, ok, err = exec.executeCallTailParallel(fixtureAdmission125, fixtureAdmission125Context, single, "RETURN v AS out", []int{0}, nil)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, res)

	fixtureAdmission126Context, fixtureAdmission126 := statementTestContext(ctx, "RETURN v AS out")
	_ = fixtureAdmission126
	res, ok, err = exec.executeCallTailParallel(fixtureAdmission126, fixtureAdmission126Context, seed, "RETURN v AS out", []int{0}, []string{"renamed"})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []string{"renamed"}, res.Columns)
	require.Equal(t, [][]interface{}{{int64(2)}, {int64(1)}}, res.Rows)

	fixtureAdmission127Context, fixtureAdmission127 := statementTestContext(ctx, "RETURN (")
	_ = fixtureAdmission127
	_, ok, err = exec.executeCallTailParallel(fixtureAdmission127, fixtureAdmission127Context, seed, "RETURN (", []int{0}, nil)
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
	fixtureAdmission128Context, fixtureAdmission128 := statementTestContext(ctx, "MATCH (n) RETURN n.id AS id, score")
	_ = fixtureAdmission128
	res, err := exec.executeCallTailSingleRow(fixtureAdmission128,
		fixtureAdmission128Context,
		seedCols,
		[]interface{}{node, int64(7)},
		"MATCH (n) RETURN n.id AS id, score",
		[]int{0, 1},
		[]string{"id", "score"},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"id", "score"}, res.Columns)
	require.Equal(t, [][]interface{}{{"n1", int64(7)}}, res.Rows)

	fixtureAdmission129Context, fixtureAdmission129 := statementTestContext(ctx, "RETURN n.id AS id, score")
	_ = fixtureAdmission129
	res, err = exec.executeCallTailSingleRow(fixtureAdmission129,
		fixtureAdmission129Context,
		seedCols,
		[]interface{}{node, int64(8)},
		"RETURN n.id AS id, score",
		[]int{0, 1},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"n1", int64(8)}}, res.Rows)

	fixtureAdmission130Context, fixtureAdmission130 := statementTestContext(ctx, "RETURN 1 AS one")
	_ = fixtureAdmission130
	res, err = exec.executeCallTailSingleRow(fixtureAdmission130,
		fixtureAdmission130Context,
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
	split := splitCallAndTail("MATCH (n) RETURN n")
	require.Equal(t, "MATCH (n) RETURN n", split.callOnly)
	require.Empty(t, split.tail)

	split = splitCallAndTail("CALL db.labels() YIELD label CREATE (:Seen {name:label})")
	require.Equal(t, "CALL db.labels() YIELD label", split.callOnly)
	require.Equal(t, "CREATE (:Seen {name:label})", split.tail)

	split = splitCallAndTail("CALL db.labels() YIELD label ORDER BY label")
	require.Equal(t, "CALL db.labels() YIELD label ORDER BY label", split.callOnly)
	require.Empty(t, split.tail)

	cols := expectedReturnColumnsFromTail("WITH n RETURN n AS node, count(*) AS c ORDER BY c DESC SKIP 1 LIMIT 2")
	require.Equal(t, []string{"node", "c"}, cols)

	cols = expectedReturnColumnsFromTail("WITH n")
	require.Nil(t, cols)
}
