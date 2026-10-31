package multidb

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A managed write that follows an acknowledged, unflushed commit of the same
// entity is charged as the last write: after the flush the cached usage equals
// a recount of the stored data, for a delete as for an update.
func TestManagedWriteAfterBufferedCommitKeepsUsage(t *testing.T) {
	for _, kind := range []string{"node", "relationship"} {
		for _, op := range []string{"delete", "update"} {
			t.Run(fmt.Sprintf("%s/%s", kind, op), func(t *testing.T) {
				engine, err := runtimeFixtureOwner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour}))
				require.NoError(t, err)
				manager, err := NewDatabaseManager(t.Context(), engine, nil)
				require.NoError(t, err)
				t.Cleanup(func() { _ = manager.Close() })
				store, err := manager.GetStorage("nornic")
				require.NoError(t, err)
				tracker := store.(*sizeTrackingEngine)
				for _, id := range []storage.NodeID{"a", "b", "n"} {
					_, err = store.CreateNode(&storage.Node{ID: id})
					require.NoError(t, err)
				}
				require.NoError(t, store.CreateEdge(&storage.Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "LINK"}))
				require.NoError(t, engine.FlushWriteBehind(nil))

				tx, err := engine.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, tx.SetImplicit(true))
				acknowledged := map[string]any{"v": "acknowledged and much longer than the later body"}
				if kind == "node" {
					err = tx.UpdateGraphNode(storage.NodeUpdateRequest{After: &storage.Node{ID: "nornic:n", Properties: acknowledged}, Policy: storage.ClientNodeEdits})
				} else {
					err = tx.UpdateEdge(storage.EdgeUpdateRequest{After: &storage.Edge{ID: "nornic:r", StartNode: "nornic:a", EndNode: "nornic:b", Type: "LINK", Properties: acknowledged}})
				}
				require.NoError(t, err)
				require.NoError(t, tracker.CommitTransaction(context.Background(), tx))
				manager.GetStorageSize(context.Background(), "nornic")

				later := map[string]any{"v": "later"}
				switch {
				case kind == "node" && op == "delete":
					err = store.DeleteNode("n")
				case kind == "node":
					err = store.UpdateNode(storage.NodeUpdateRequest{After: &storage.Node{ID: "n", Properties: later}})
				case op == "delete":
					err = store.DeleteEdge("r")
				default:
					err = store.UpdateEdge(storage.EdgeUpdateRequest{After: &storage.Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "LINK", Properties: later}})
				}
				require.NoError(t, err)
				require.NoError(t, engine.FlushWriteBehind(nil))

				wantNodes, wantEdges, err := manager.calculateStorageSizeFromEngine(context.Background(), tracker.Engine)
				require.NoError(t, err)
				total, nodes, edges := manager.GetStorageSize(context.Background(), "nornic")
				require.Equal(t, []int64{wantNodes + wantEdges, wantNodes, wantEdges}, []int64{total, nodes, edges},
					"cached usage after the flush equals a recount")
			})
		}
	}
}
