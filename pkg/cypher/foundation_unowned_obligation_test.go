package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// An active replicated engine owns command publication and refuses native
// transaction control. It must be exercised in that real mode, not with a nil
// replicator artificially wrapped around a native transaction adapter.
func FoundationRunUnownedConsumer(t *testing.T, engine storage.Engine, consumer string) {
	ctx := context.Background()
	exec := NewStorageExecutor(engine)
	run := func(query string) *ExecuteResult {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		return result
	}
	switch consumer {
	case "rows", "identity":
		result := run("MATCH(n:FoundationLogical) RETURN n.v,id(n)")
		require.Len(t, result.Rows, 1)
		require.Equal(t, int64(7), result.Rows[0][0])
		require.NotEmpty(t, result.Rows[0][1])
	case "visualization":
		result := run("CALL db.schema.visualization()")
		require.NotEmpty(t, result.Rows[0][0])
	case "membership", "mutation":
		run("CREATE (:FoundationLogicalWrite)")
		nodes, err := engine.GetNodesByLabel("FoundationLogicalWrite")
		require.NoError(t, err)
		require.Len(t, nodes, 1)
	case "control":
		_, native := storage.CommitOwnerForEngine(engine)
		require.False(t, native)
		for _, command := range []string{"BEGIN", "COMMIT", "ROLLBACK"} {
			_, err := exec.Execute(ctx, command, nil)
			require.Error(t, err)
		}
	case "periodic":
		run("CALL apoc.periodic.iterate('UNWIND [1,2] AS x RETURN x','CREATE (:FoundationLogicalWrite)',{batchSize:1}) YIELD total RETURN total")
		nodes, err := engine.GetNodesByLabel("FoundationLogicalWrite")
		require.NoError(t, err)
		require.Len(t, nodes, 2)
	case "actions":
		_, err := exec.Execute(ctx, "MATCH(n:FoundationLogical) CALL kp.suppress(n) RETURN n", nil)
		require.Error(t, err, "replication supplies no exact manual-suppression command owner")
		nodes, err := engine.GetNodesByLabel("FoundationLogical")
		require.NoError(t, err)
		require.Len(t, nodes, 1)
	case "schema":
		run("CREATE INDEX logical_idx FOR(n:FoundationLogical) ON(n.v)")
		result := run("SHOW INDEXES YIELD name WHERE name='logical_idx' RETURN name")
		require.Len(t, result.Rows, 1)
	case "lease":
		lease, release := storage.BeginQueryRevealScopeContext(ctx, engine, false)
		defer release()
		_, err := exec.Execute(lease, "RETURN 1", nil)
		require.NoError(t, err)
	case "mode":
		result := run("CALL apoc.cypher.run('CREATE (:FoundationLogicalWrite) RETURN 1 AS v',{}) YIELD value RETURN value")
		mode, ok := ResolvedStatementQueryType(result.Metadata)
		require.True(t, ok)
		require.Contains(t, []string{"w", "rw"}, mode)
	case "limits":
		provider := &fakeLimitedEngine{Engine: engine, checker: fakeQueryLimitChecker{limits: fakeMaxResults(2)}}
		limited := NewStorageExecutor(provider)
		for _, limit := range []int{2, 3} {
			provider.checker = fakeQueryLimitChecker{limits: fakeMaxResults(limit)}
			result, err := limited.Execute(ctx, "UNWIND [1,2,3,4] AS x RETURN x", nil)
			require.NoError(t, err)
			require.Len(t, result.Rows, limit)
		}
	case "observed-node":
		before, err := engine.GetNode("logical")
		require.NoError(t, err)
		after := *before
		after.Properties = map[string]interface{}{"v": int64(8)}
		require.NoError(t, storage.UpdateObservedNode(engine, before, &after))
		result := run("MATCH(n:FoundationLogical) RETURN n.v")
		require.Equal(t, int64(8), result.Rows[0][0])
	case "observed-edge":
		before := &storage.Edge{ID: "edge", StartNode: "logical", EndNode: "logical", Type: "R"}
		require.NoError(t, engine.CreateEdge(before))
		after := *before
		after.Properties = map[string]interface{}{"v": int64(2)}
		require.NoError(t, storage.UpdateObservedEdge(engine, before, &after))
		got, err := engine.GetEdge("edge")
		require.NoError(t, err)
		require.Equal(t, int64(2), got.Properties["v"])
	default:
		t.Fatalf("unclassified replicated public consumer %s", consumer)
	}
}
