package cypher_test

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Author: Lane #1644 (Codex). The oracle is the issued transaction's public
// point-read outcome, with independently seeded pending and snapshot values.
func TestFoundationPublicIssuedBatchSnapshot(t *testing.T) {
	for _, word := range []string{"", "T", "W", "TW", "WT", "TWT", "WTW"} {
		t.Run("wrappers="+word, func(t *testing.T) {
			base := storage.NewMemoryEngine()
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			tenant := storage.NewNamespacedEngine(base, "tenant")
			for _, key := range []string{"changed", "gone", "stable"} {
				_, err := tenant.CreateNode(&storage.Node{ID: storage.NodeID(key), Labels: []string{"IssuedBatch"}, Properties: map[string]interface{}{"key": key, "value": "before"}})
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
			for _, query := range []string{
				"BEGIN",
				"MATCH (n:IssuedBatch {key:'changed'}) SET n.value='pending' RETURN n.value",
				"CREATE (:IssuedBatch {key:'fresh',value:'pending'})",
				"MATCH (n:IssuedBatch {key:'gone'}) DELETE n",
			} {
				_, err := executor.Execute(ctx, query, nil)
				require.NoError(t, err, query)
			}
			t.Cleanup(func() { _, _ = executor.Execute(ctx, "ROLLBACK", nil) })
			issued, err := executor.FunctionStorage("")
			require.NoError(t, err)
			nodes, err := issued.GetNodesByLabel("IssuedBatch")
			require.NoError(t, err)
			var fresh storage.NodeID
			for _, node := range nodes {
				if node.Properties["key"] == "fresh" {
					fresh = node.ID
				}
			}
			require.NotEmpty(t, fresh)
			peer, err := tenant.GetNode("stable")
			require.NoError(t, err)
			peer.Properties["value"] = "later"
			require.NoError(t, tenant.UpdateNode(peer))
			want := map[storage.NodeID]string{"changed": "pending", fresh: "pending", "stable": "before"}
			ids := []storage.NodeID{"changed", fresh, "stable", "gone", "missing"}
			for id, value := range want {
				node, err := issued.GetNode(id)
				require.NoError(t, err)
				require.Equal(t, value, node.Properties["value"], "point-read setup control")
			}
			_, err = issued.GetNode("gone")
			require.ErrorIs(t, err, storage.ErrNotFound)
			for _, contextual := range []bool{false, true} {
				var got map[storage.NodeID]*storage.Node
				if contextual {
					got, err = storage.BatchGetNodesWithContext(ctx, issued, ids)
				} else {
					got, err = issued.BatchGetNodes(ids)
				}
				if assert.NoError(t, err, "contextual=%v", contextual) {
					values := map[storage.NodeID]string{}
					for id, node := range got {
						values[id], _ = node.Properties["value"].(string)
					}
					assert.Equal(t, want, values, "batch must preserve pending, deleted and pinned bodies; contextual=%v", contextual)
				}
			}
			_, err = executor.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, err)
			_, err = issued.GetNode("changed")
			require.ErrorIs(t, err, storage.ErrTransactionClosed, "point-read lifecycle control")
			_, err = issued.BatchGetNodes(ids)
			assert.ErrorIs(t, err, storage.ErrTransactionClosed, "retained batch must keep the ended owner")
			_, err = storage.BatchGetNodesWithContext(ctx, issued, ids)
			assert.ErrorIs(t, err, storage.ErrTransactionClosed, "retained contextual batch must keep the ended owner")
			result, err := cypher.NewStorageExecutor(tenant).Execute(ctx, "MATCH (n:IssuedBatch {key:'fresh'}) RETURN n", nil)
			require.NoError(t, err)
			require.Empty(t, result.Rows, "rollback must not publish the pending batch member")
		})
	}
}
