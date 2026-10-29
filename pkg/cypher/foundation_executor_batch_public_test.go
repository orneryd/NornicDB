package cypher_test

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/internal/enginecontract"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Author: Lane #1644 (Codex). The lane chose the additive error API and
// compatibility wrapper under Nornic2's batch/point and compatibility invariants.
type executorBatchContextReader interface {
	BatchGetNodesContext(context.Context, []storage.NodeID) (map[storage.NodeID]*storage.Node, error)
}

func TestFoundationPublicExecutorBatchIssuedView(t *testing.T) {
	for _, word := range []string{"", "T", "W", "TW", "WT", "TWT", "WTW"} {
		t.Run("wrappers="+word, func(t *testing.T) {
			base := storage.NewMemoryEngine()
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			tenant := storage.NewNamespacedEngine(base, "tenant")
			for _, key := range []string{"changed", "gone"} {
				_, err := tenant.CreateNode(&storage.Node{ID: storage.NodeID(key), Labels: []string{"ExecutorBatch"}, Properties: map[string]interface{}{"key": key, "value": "before"}})
				require.NoError(t, err)
			}
			var engine storage.Engine = tenant
			for _, token := range word {
				if token == 'T' {
					engine = storage.NewTracedEngine(engine)
				} else {
					wal, err := storage.NewWAL(t.TempDir(), nil)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, wal.Close()) })
					engine = storage.NewWALEngine(engine, wal)
				}
			}
			executor := cypher.NewStorageExecutor(engine)
			ctx := context.Background()
			for _, query := range []string{"BEGIN", "MATCH (n:ExecutorBatch {key:'changed'}) SET n.value='pending' RETURN n.value", "CREATE (:ExecutorBatch {key:'fresh',value:'pending'})", "MATCH (n:ExecutorBatch {key:'gone'}) DELETE n"} {
				_, err := executor.Execute(ctx, query, nil)
				require.NoError(t, err, query)
			}
			t.Cleanup(func() { _, _ = executor.Execute(ctx, "ROLLBACK", nil) })
			issued, err := executor.FunctionStorage("")
			require.NoError(t, err)
			nodes, err := issued.GetNodesByLabel("ExecutorBatch")
			require.NoError(t, err)
			var fresh storage.NodeID
			for _, node := range nodes {
				if node.Properties["key"] == "fresh" {
					fresh = node.ID
				}
			}
			require.NotEmpty(t, fresh)
			ids := []storage.NodeID{"changed", fresh, "gone", "missing"}
			want := map[storage.NodeID]string{"changed": "pending", fresh: "pending"}
			for id, value := range want {
				node, err := issued.GetNode(id)
				require.NoError(t, err)
				require.Equal(t, value, node.Properties["value"], "issued point setup")
			}
			_, err = issued.GetNode("gone")
			require.ErrorIs(t, err, storage.ErrNotFound)
			values := func(nodes map[storage.NodeID]*storage.Node) map[storage.NodeID]string {
				result := map[storage.NodeID]string{}
				for id, node := range nodes {
					result[id], _ = node.Properties["value"].(string)
				}
				return result
			}
			// This call intentionally verifies the compatibility API embedders retain.
			assert.Equal(t, want, values(executor.BatchGetNodes(ids)), "compatibility API must preserve the issued BEGIN view")
			reader, ok := interface{}(executor).(executorBatchContextReader)
			if assert.True(t, ok, "the additive error-returning API is present") {
				got, err := reader.BatchGetNodesContext(ctx, ids)
				assert.NoError(t, err)
				assert.Equal(t, want, values(got), "error API must preserve the same issued BEGIN view")
				enginecontract.NodeBatchPoint(t, ids, func(request []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
					return reader.BatchGetNodesContext(ctx, request)
				}, issued.GetNode)
				enginecontract.NodeBatchPoint(t, ids, func(request []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
					return executor.BatchGetNodes(request), nil
				}, issued.GetNode)
			}
			_, err = executor.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, err)
			after, err := cypher.NewStorageExecutor(tenant).Execute(ctx, "MATCH (n:ExecutorBatch {key:'fresh'}) RETURN n", nil)
			require.NoError(t, err)
			require.Empty(t, after.Rows)
		})
	}
}

type executorPointFailureView struct {
	storage.Engine
	failed  storage.NodeID
	failure error
	reads   int
}

func (*executorPointFailureView) Namespace() string { return "tenant" }
func (view *executorPointFailureView) GetNode(id storage.NodeID) (*storage.Node, error) {
	view.reads++
	if id == view.failed {
		return nil, view.failure
	}
	return view.Engine.GetNode(id)
}

func TestFoundationPublicExecutorBatchPointFailure(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	tenant := storage.NewNamespacedEngine(base, "tenant")
	for _, id := range []storage.NodeID{"before", "after"} {
		_, err := tenant.CreateNode(&storage.Node{ID: id, Properties: map[string]interface{}{"id": string(id)}})
		require.NoError(t, err)
	}
	cause := errors.New("point admission failed")
	view := &executorPointFailureView{Engine: tenant, failed: "failed", failure: cause}
	executor := cypher.NewStorageExecutor(view)
	ids := []storage.NodeID{"before", "failed", "missing", "after"}
	point, err := view.GetNode("before")
	require.NoError(t, err)
	_, err = view.GetNode("failed")
	require.ErrorIs(t, err, cause)
	// Intentional compatibility oracle: a failed ID is omitted, later successes survive.
	compatible := executor.BatchGetNodes(ids)
	require.Len(t, compatible, 2)
	assert.Equal(t, point, compatible["before"])
	assert.Contains(t, compatible, storage.NodeID("after"))
	reader, ok := interface{}(executor).(executorBatchContextReader)
	if !assert.True(t, ok, "the additive error-returning API is present") {
		return
	}
	got, err := reader.BatchGetNodesContext(context.Background(), ids)
	assert.ErrorIs(t, err, cause)
	assert.Len(t, got, 2)
	assert.Equal(t, compatible, got)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	view.reads = 0
	got, err = reader.BatchGetNodesContext(cancelled, ids)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, got)
	assert.Zero(t, view.reads)
}
