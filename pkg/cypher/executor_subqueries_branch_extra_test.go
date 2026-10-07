package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestExecutorSubqueryHelpers_ExtraBranches(t *testing.T) {
	lhs, rhs, ok := splitTopLevelEqualityCallSubquery("a = b")
	require.True(t, ok)
	require.Equal(t, "a", lhs)
	require.Equal(t, "b", rhs)

	_, _, ok = splitTopLevelEqualityCallSubquery("a = ")
	require.False(t, ok)
	_, _, ok = splitTopLevelEqualityCallSubquery("(a = b)")
	require.False(t, ok)

	idx := indexTopLevelKeywordCallSubquery("`RETURN` RETURN x", "RETURN")
	require.Equal(t, 9, idx)
	idx = indexTopLevelKeywordCallSubquery("[1,2,RETURN] RETURN x", "RETURN")
	require.Equal(t, 13, idx)

	require.True(t, containsStandaloneIdentifier("x + y", "x"))
	require.False(t, containsStandaloneIdentifier("foo.x", "x"))
	require.False(t, containsStandaloneIdentifier("xx", "x"))
}

func TestExecuteChainedCallSubquery_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "chain_call")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	seed := &ExecuteResult{Columns: []string{"seed"}, Rows: [][]interface{}{{int64(1)}, {int64(2)}}}

	_, err := exec.sharedSeededCallForTest(ctx, seed, "CALL { }")
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty body")

	batched, err := exec.sharedSeededCallForTest(ctx, seed, "CALL { RETURN 1 AS x } IN TRANSACTIONS OF 2 ROWS RETURN x")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}, {int64(1)}}, batched.Rows)

	res, err := exec.sharedSeededCallForTest(ctx, seed, "CALL { RETURN 7 AS x } RETURN x")
	require.NoError(t, err)
	require.Equal(t, []string{"x"}, res.Columns)
	require.Len(t, res.Rows, 2)
	require.EqualValues(t, 7, res.Rows[0][0])
	require.EqualValues(t, 7, res.Rows[1][0])

	res, err = exec.sharedSeededCallForTest(ctx, seed, "CALL { WITH seed RETURN seed AS x } RETURN x")
	require.NoError(t, err)
	require.Equal(t, []string{"x"}, res.Columns)
	require.Len(t, res.Rows, 2)
	require.EqualValues(t, int64(1), res.Rows[0][0])
	require.EqualValues(t, int64(2), res.Rows[1][0])

	_, err = exec.sharedSeededCallForTest(ctx, seed, "CALL { WITH seed }")
	require.Error(t, err)
	require.Contains(t, err.Error(), "WITH must be followed by a query clause")

	// USE branch should route through scoped resolution and still execute.
	res, err = exec.sharedSeededCallForTest(ctx, seed, "CALL { USE missing_db RETURN 1 AS x } RETURN x")
	require.NoError(t, err)
	require.Equal(t, []string{"x"}, res.Columns)
	require.Len(t, res.Rows, 2)
}

func TestExecuteChainedCallSubquery_ImplicitScalarCorrelationOptionalAggregate(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "chain_call_implicit")
	exec := NewStorageExecutor(store)
	ctx := withParams(context.Background(), map[string]interface{}{"min_amount": int64(100)})

	_, err := store.CreateNode(&storage.Node{ID: "o1", Labels: []string{"Order"}, Properties: map[string]interface{}{"owner_id": "a1", "order_id": "ORD-001", "amount": int64(125)}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "o2", Labels: []string{"Order"}, Properties: map[string]interface{}{"owner_id": "a2", "order_id": "ORD-002", "amount": int64(90)}})
	require.NoError(t, err)

	seed := &ExecuteResult{
		Columns: []string{"person_id", "person_name"},
		Rows: [][]interface{}{
			{"a1", "Alice"},
			{"a2", "Bob"},
		},
	}

	res, err := exec.sharedSeededCallForTest(ctx, seed, "CALL { OPTIONAL MATCH (o:Order) WHERE o.owner_id = person_id AND o.amount >= $min_amount RETURN collect(o.order_id) AS order_ids, count(o) AS order_count } RETURN person_id, person_name, order_ids, order_count")
	require.NoError(t, err)
	require.Equal(t, []string{"person_id", "person_name", "order_ids", "order_count"}, res.Columns)
	require.Len(t, res.Rows, 2)

	rowByID := make(map[string][]interface{}, len(res.Rows))
	for _, row := range res.Rows {
		require.Len(t, row, 4)
		id, ok := row[0].(string)
		require.True(t, ok)
		rowByID[id] = row
	}
	require.Contains(t, rowByID, "a1")
	require.Contains(t, rowByID, "a2")

	a1Orders, ok := rowByID["a1"][2].([]interface{})
	require.True(t, ok)
	require.Len(t, a1Orders, 1)
	require.Equal(t, "ORD-001", a1Orders[0])
	require.EqualValues(t, 1, rowByID["a1"][3])

	a2Orders, ok := rowByID["a2"][2].([]interface{})
	require.True(t, ok)
	require.Len(t, a2Orders, 0)
	require.EqualValues(t, 0, rowByID["a2"][3])
}

func TestSeedNodesFromOuterMatch_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "seed_outer")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{ID: "s1", Labels: []string{"Seed"}, Properties: map[string]interface{}{"name": "A"}})
	require.NoError(t, err)

	nodes, err := exec.seedNodesFromOuterMatch(ctx, "MATCH (seed:Seed {name:'A'})", "seed")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, storage.NodeID("s1"), nodes[0].ID)

	_, err = exec.seedNodesFromOuterMatch(ctx, "MATCH (seed:Seed) RETURN 1", "missing")
	require.Error(t, err)
}
