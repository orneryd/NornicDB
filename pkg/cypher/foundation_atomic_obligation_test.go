package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The auth view has a deliberately narrower public population: nodes join the
// actual store transaction, while query lifecycle/schema/action ownership is
// refused. Exercise those refusals, rather than inventing logical owner tokens.
func FoundationRunAtomicConsumer(t *testing.T, view, committed storage.Engine, consumer string) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewTracedEngine(storage.NewNamespacedEngine(view, "nornic")))
	run := func(query string) *ExecuteResult {
		r, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		return r
	}
	switch consumer {
	case "rows", "identity":
		r := run("MATCH (n:FoundationLogical) RETURN n.v,id(n) LIMIT 1")
		require.Len(t, r.Rows, 1)
		require.Equal(t, int64(7), r.Rows[0][0])
		require.NotEmpty(t, r.Rows[0][1])
	case "visualization":
		r := run("CALL db.schema.visualization()")
		require.NotEmpty(t, r.Rows[0][0])
	case "membership", "mutation", "observed-node":
		if consumer == "observed-node" {
			before, err := view.GetNode("nornic:logical")
			require.NoError(t, err)
			after := *before
			after.Properties = map[string]interface{}{"v": int64(8)}
			require.NoError(t, storage.UpdateObservedNode(view, before, &after))
			r := run("MATCH (n:FoundationLogical) RETURN n.v")
			require.Equal(t, int64(8), r.Rows[0][0])
			unchanged, err := committed.GetNode("nornic:logical")
			require.NoError(t, err)
			require.Equal(t, int64(7), unchanged.Properties["v"])
		} else {
			run("CREATE (:FoundationAtomicPending)")
			nodes, err := committed.GetNodesByLabel("FoundationAtomicPending")
			require.NoError(t, err)
			require.Empty(t, nodes)
			r := run("MATCH(n:FoundationAtomicPending) RETURN n")
			require.Len(t, r.Rows, 1)
		}
	case "control":
		for _, query := range []string{"BEGIN", "COMMIT", "ROLLBACK"} {
			_, err := exec.Execute(ctx, query, nil)
			require.Error(t, err)
		}
	case "schema":
		_, err := exec.Execute(ctx, "CREATE INDEX atomic_refused FOR(n:FoundationLogical) ON(n.v)", nil)
		require.ErrorIs(t, err, storage.ErrNotImplemented)
		r, err := NewStorageExecutor(committed).Execute(ctx, "SHOW INDEXES YIELD name WHERE name='atomic_refused' RETURN name", nil)
		require.NoError(t, err)
		require.Empty(t, r.Rows)
	case "actions":
		_, err := exec.Execute(ctx, "MATCH(n:FoundationLogical) CALL kp.suppress(n) RETURN n", nil)
		require.Error(t, err)
		nodes, err := committed.GetNodesByLabel("FoundationLogical")
		require.NoError(t, err)
		require.Len(t, nodes, 1)
	case "periodic":
		result, err := exec.Execute(ctx, "CALL apoc.periodic.iterate('RETURN 1 AS x','CREATE (:FoundationAtomicBatch)',{batchSize:1}) YIELD total,failedBatches RETURN total,failedBatches", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1), int64(0)}}, result.Rows)
		nodes, err := view.GetNodesByLabel("FoundationAtomicBatch")
		require.NoError(t, err)
		require.Len(t, nodes, 1)
		nodes, err = committed.GetNodesByLabel("FoundationAtomicBatch")
		require.NoError(t, err)
		require.Empty(t, nodes, "atomic store callback remains the sole publication owner")
	case "lease":
		lease, release := storage.BeginQueryRevealScopeContext(ctx, committed, false)
		defer release()
		_, err := exec.Execute(lease, "RETURN 1", nil)
		require.NoError(t, err, "incoming live lease is usable; auth does not claim a separate carried lease")
	case "mode":
		r := run("CALL apoc.cypher.run('CREATE (:FoundationAtomicPending) RETURN 1 AS v',{}) YIELD value RETURN value")
		mode, ok := ResolvedStatementQueryType(r.Metadata)
		require.True(t, ok)
		require.Contains(t, []string{"w", "rw"}, mode)
	case "limits":
		provider := &fakeLimitedEngine{Engine: view, checker: fakeQueryLimitChecker{limits: fakeMaxResults(2)}}
		limited := NewStorageExecutor(provider)
		for _, limit := range []int{2, 3} {
			provider.checker = fakeQueryLimitChecker{limits: fakeMaxResults(limit)}
			r, err := limited.Execute(ctx, "UNWIND [1,2,3,4] AS x RETURN x", nil)
			require.NoError(t, err)
			require.Len(t, r.Rows, limit)
		}
	case "observed-edge":
		// The auth store contract adapts nodes only. Ordinary edge operations are
		// exact operations of the issued Engine, and have no auth transaction claim.
		before := &storage.Edge{ID: "nornic:edge", StartNode: "nornic:logical", EndNode: "nornic:logical", Type: "R"}
		require.NoError(t, view.CreateEdge(before))
		after := *before
		after.Properties = map[string]interface{}{"v": int64(2)}
		require.NoError(t, storage.UpdateObservedEdge(view, before, &after))
		got, err := view.GetEdge("nornic:edge")
		require.NoError(t, err)
		require.Equal(t, int64(2), got.Properties["v"])
	default:
		t.Fatalf("unclassified auth public consumer %s", consumer)
	}
}
