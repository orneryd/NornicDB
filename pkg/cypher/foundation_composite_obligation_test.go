package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func FoundationIssueCompositeFixture(t *testing.T) FoundationPublicFixture {
	owner, committed := newProcedureAdmissionExecutor(t, true)
	_, err := committed.CreateNode(&storage.Node{ID: "logical", Labels: []string{"FoundationLogical"}, Properties: map[string]interface{}{"v": int64(7)}})
	require.NoError(t, err)
	_, err = owner.Execute(context.Background(), "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		if owner.txContext != nil && owner.txContext.active {
			_, _ = owner.Execute(context.Background(), "ROLLBACK", nil)
		}
	})
	issued, err := owner.FunctionStorage("")
	require.NoError(t, err)
	f := FoundationPublicFixture{Engine: issued, Owner: owner, Committed: committed, NodeID: "logical"}
	f.Consume = func(t *testing.T, consumer string) { foundationCompositeConsumer(t, f, consumer) }
	return f
}

// Composite public graph operations require a constituent target. The adapter
// exercises that real route and its coordinator, rather than issuing invalid
// unqualified MATCH/DDL against the composite and calling refusal success.
func foundationCompositeConsumer(t *testing.T, f FoundationPublicFixture, consumer string) {
	ctx := context.Background()
	member := f.Owner.cloneForStorage(f.Engine)
	target := func(query string) *ExecuteResult {
		result, err := member.Execute(ctx, "USE admission_comp.one "+query, nil)
		require.NoError(t, err)
		return result
	}
	switch consumer {
	case "rows", "identity":
		r := target("MATCH(n:FoundationLogical) RETURN n.v,id(n)")
		require.Len(t, r.Rows, 1)
		require.Equal(t, int64(7), r.Rows[0][0])
		require.NotEmpty(t, r.Rows[0][1])
	case "visualization":
		r, err := member.callDbSchemaVisualizationWithContext(ctx)
		require.NoError(t, err)
		require.NotEmpty(t, r.Rows[0][0])
	case "membership", "mutation":
		target("CREATE (:FoundationLogicalWrite) RETURN 1")
		nodes, err := f.Committed.GetNodesByLabel("FoundationLogicalWrite")
		require.NoError(t, err)
		require.Empty(t, nodes)
		_, err = f.Owner.Execute(ctx, "ROLLBACK", nil)
		require.NoError(t, err)
		nodes, err = f.Committed.GetNodesByLabel("FoundationLogicalWrite")
		require.NoError(t, err)
		require.Empty(t, nodes)
		_, err = NewStorageExecutor(f.Engine).Execute(ctx, "RETURN 1", nil)
		require.ErrorIs(t, err, storage.ErrTransactionClosed)
	case "control":
		reconstructed := NewStorageExecutor(f.Engine)
		for _, command := range []string{"BEGIN", "COMMIT", "ROLLBACK"} {
			_, err := reconstructed.Execute(ctx, command, nil)
			require.Error(t, err)
		}
		require.True(t, f.Owner.txContext.active)
	case "periodic":
		_, err := member.Execute(ctx, "USE admission_comp.one CALL apoc.periodic.iterate('RETURN 1 AS x','CREATE (:FoundationLogicalWrite)',{batchSize:1}) YIELD total RETURN total", nil)
		require.ErrorContains(t, err, "unsupported query type in transaction")
		nodes, err := f.Committed.GetNodesByLabel("FoundationLogicalWrite")
		require.NoError(t, err)
		require.Empty(t, nodes)
	case "actions":
		target("MATCH(n:FoundationLogical) CALL kp.suppress(n) RETURN n")
		_, err := f.Owner.Execute(ctx, "COMMIT", nil)
		require.NoError(t, err)
		nodes, err := f.Committed.GetNodesByLabel("FoundationLogical")
		require.NoError(t, err)
		require.Empty(t, nodes)
	case "schema":
		target("CREATE INDEX logical_idx FOR(n:FoundationLogical) ON(n.v)")
		_, err := f.Owner.Execute(ctx, "COMMIT", nil)
		require.NoError(t, err)
		result, err := NewStorageExecutor(f.Committed).Execute(ctx, "SHOW INDEXES YIELD name WHERE name='logical_idx' RETURN name", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	case "lease", "mode", "limits", "observed-node", "observed-edge":
		const name = "foundation.compositeConsumer"
		cleanupAdmissionProcedure(t, name)
		require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: name, Mode: ProcedureModeWrite, Returns: []ProcedureColumn{{Name: "v", Type: "INTEGER"}}}, func(ctx context.Context, caller *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
			view, err := caller.FunctionStorage("")
			if err != nil {
				return nil, err
			}
			if consumer == "observed-node" {
				before, err := view.GetNode("logical")
				if err != nil {
					return nil, err
				}
				after := *before
				after.Properties = map[string]interface{}{"v": int64(8)}
				if err := storage.UpdateObservedNode(view, before, &after); err != nil {
					return nil, err
				}
				got, err := view.GetNode("logical")
				require.NoError(t, err)
				require.Equal(t, int64(8), got.Properties["v"])
			} else if consumer == "observed-edge" {
				before := &storage.Edge{ID: "edge", StartNode: "logical", EndNode: "logical", Type: "R"}
				if err := view.CreateEdge(before); err != nil {
					return nil, err
				}
				after := *before
				after.Properties = map[string]interface{}{"v": int64(2)}
				if err := storage.UpdateObservedEdge(view, before, &after); err != nil {
					return nil, err
				}
				got, err := view.GetEdge("edge")
				require.NoError(t, err)
				require.Equal(t, int64(2), got.Properties["v"])
			} else {
				FoundationRunPublicConsumer(t, FoundationPublicFixture{Engine: view, Owner: f.Owner, Committed: f.Committed, NodeID: "logical", LeaseContext: ctx}, consumer)
			}
			return &ExecuteResult{Columns: []string{"v"}, Rows: [][]interface{}{{int64(1)}}, Stats: &QueryStats{}}, nil
		}))
		target("CALL " + name + "() YIELD v RETURN v")
		_, err := f.Owner.Execute(ctx, "ROLLBACK", nil)
		require.NoError(t, err)
		node, err := f.Committed.GetNode("logical")
		require.NoError(t, err)
		require.Equal(t, int64(7), node.Properties["v"])
	default:
		t.Fatalf("unclassified composite consumer %s", consumer)
	}
}
