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

// Byte accounting charges the body storage holds. Initialization sizes the
// acknowledged write-behind body while the commit is still unflushed, and a
// later update or delete releases the charge of that same body, so the cached
// size stays equal to a full recount, whether the entity is visible or hidden.
func TestStorageSizeTracksAcknowledgedBodies(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, kind := range []string{"node", "edge"} {
			for _, hidden := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/write-behind=%v/hidden=%v", kind, buffered, hidden), func(t *testing.T) {
					base, err := runtimeFixtureOwner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
						DataDir: t.TempDir(), WriteBehind: buffered, WriteBehindInterval: time.Hour,
					}))
					require.NoError(t, err)
					manager, err := NewDatabaseManager(t.Context(), base, nil)
					require.NoError(t, err)
					t.Cleanup(func() { _ = manager.Close() })
					engine, err := manager.GetStorage("nornic")
					require.NoError(t, err)
					tracker := engine.(*sizeTrackingEngine)
					requireRecount := func(step string) {
						t.Helper()
						wantNodes, wantEdges, err := manager.calculateStorageSizeFromEngine(tracker.Engine)
						require.NoError(t, err)
						total, nodes, edges := manager.GetStorageSize("nornic")
						require.Equal(t, []int64{wantNodes + wantEdges, wantNodes, wantEdges}, []int64{total, nodes, edges}, step)
					}
					// write stages one change to the entity through an implicit
					// transaction committed by the tracker.
					write := func(change func(tx *storage.BadgerTransaction) error) {
						t.Helper()
						tx, err := base.BeginTransaction()
						require.NoError(t, err)
						require.NoError(t, tx.SetNamespace("nornic"))
						require.NoError(t, tx.SetImplicit(true))
						require.NoError(t, change(tx))
						require.NoError(t, tracker.CommitTransaction(context.Background(), tx))
					}
					setPayload := func(payload []any) func(tx *storage.BadgerTransaction) error {
						return func(tx *storage.BadgerTransaction) error {
							if kind == "node" {
								node, err := tx.GetStoredNode("nornic:n")
								if err != nil {
									return err
								}
								node.Properties["payload"] = payload
								return tx.UpdateGraphNode(storage.NodeUpdateRequest{After: node, Policy: storage.ClientNodeEdits})
							}
							edge, err := tx.GetStoredEdge("nornic:r")
							if err != nil {
								return err
							}
							edge.Properties["payload"] = payload
							return tx.UpdateEdge(storage.EdgeUpdateRequest{After: edge})
						}
					}

					for _, id := range []storage.NodeID{"a", "b"} {
						_, err = engine.CreateNode(&storage.Node{ID: id})
						require.NoError(t, err)
					}
					if kind == "node" {
						_, err = engine.CreateNode(&storage.Node{ID: "n", Properties: map[string]any{"payload": []any{"stored"}}})
					} else {
						err = engine.CreateEdge(&storage.Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "LINK", Properties: map[string]any{"payload": []any{"stored"}}})
					}
					require.NoError(t, err)
					require.NoError(t, base.FlushWriteBehind())
					// An acknowledged rewrite: until a flush, stored reads, and so
					// initialization, meet it in the write-behind overlay.
					write(setPayload([]any{"stored"}))
					initialized, err := manager.storageSizeInitialized("nornic")
					require.NoError(t, err)
					require.False(t, initialized)
					requireRecount("initialization before the flush")

					write(setPayload([]any{"stored", "more"}))
					requireRecount("update")
					if hidden {
						if kind == "node" {
							require.NoError(t, base.SetManualSuppression(storage.StoredEntityNodeKind, "nornic:n", true))
						} else {
							require.NoError(t, base.SetManualSuppression(storage.StoredEntityEdgeKind, "nornic:r", true))
						}
					}
					write(func(tx *storage.BadgerTransaction) error {
						if kind == "node" {
							return tx.DeleteGraphNode("nornic:n", storage.CascadeEdges)
						}
						return tx.DeleteEdge("nornic:r")
					})
					requireRecount("delete")
					require.NoError(t, base.FlushWriteBehind())
					requireRecount("flush")
				})
			}
		}
	}
}
