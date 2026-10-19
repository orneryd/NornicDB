package multidb

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Every replacement contributes its final stored after-image minus its hidden
// prior body, once per ID. Check raw transaction and direct API routes against
// a fresh stored scan, including the second, unchanged write at exact capacity.
func TestTransactionQuotaReplacementAccounting(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(fmt.Sprint("buffered=", buffered), func(t *testing.T) {
			base, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), WriteBehind: buffered})
			require.NoError(t, err)
			manager, err := NewDatabaseManager(base, nil)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, manager.Close()) })
			for _, entity := range []string{"node", "edge"} {
				for _, operation := range []string{"update", "create-set", "delete-recreate", "remove", "direct-update"} {
					for _, quota := range []string{"count", "bytes"} {
						t.Run(entity+"/"+operation+"/"+quota, func(t *testing.T) {
							db := entity + "_" + strings.ReplaceAll(operation, "-", "_") + "_" + quota
							require.NoError(t, manager.CreateDatabase(db))
							engine, err := manager.GetStorage(db)
							require.NoError(t, err)
							tracker := engine.(*sizeTrackingEngine)
							for _, id := range []storage.NodeID{"a", "b"} {
								_, err := engine.CreateNode(&storage.Node{ID: id})
								require.NoError(t, err)
							}
							properties := map[string]any{"payload": strings.Repeat("x", 2048)}
							if entity == "node" {
								_, err = engine.CreateNode(&storage.Node{ID: "n", Labels: []string{"Doc"}, Properties: properties})
								require.NoError(t, err)
								require.NoError(t, base.SetNodeManuallySuppressed(storage.NodeID(db+":n"), true))
							} else {
								require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "LINK", Properties: properties}))
								require.NoError(t, base.SetEdgeManuallySuppressed(storage.EdgeID(db+":e"), true))
							}
							require.NoError(t, base.FlushWriteBehind())
							wantNodes, err := engine.NodeCount()
							require.NoError(t, err)
							wantEdges, err := engine.EdgeCount()
							require.NoError(t, err)
							for attempt := 0; attempt < 2; attempt++ {
								before, _, _ := manager.GetStorageSize(db)
								limits := &Limits{Storage: StorageLimits{MaxNodes: wantNodes, MaxEdges: wantEdges}}
								if quota == "bytes" {
									limits = &Limits{Storage: StorageLimits{MaxBytes: before}}
								}
								require.NoError(t, manager.SetDatabaseLimits(db, limits))
								props := map[string]any{"payload": "small"}
								if operation == "remove" {
									props = nil
								}
								if operation == "direct-update" {
									if entity == "node" {
										node, err := storage.GetStoredNode(engine, "n")
										require.NoError(t, err)
										node.Properties = props
										require.NoError(t, engine.UpdateNode(node))
									} else {
										edge, err := storage.GetStoredEdge(engine, "e")
										require.NoError(t, err)
										edge.Properties = props
										require.NoError(t, engine.UpdateEdge(edge))
									}
								} else {
									tx, err := base.BeginTransaction()
									require.NoError(t, err)
									t.Cleanup(func() { _ = tx.Rollback() })
									require.NoError(t, tx.SetNamespace(db))
									if entity == "node" {
										id := storage.NodeID(db + ":n")
										node, err := tx.GetStoredNode(id)
										require.NoError(t, err)
										if operation == "create-set" || operation == "delete-recreate" {
											require.NoError(t, tx.DeleteNode(id))
											node = &storage.Node{ID: id, Labels: []string{"Doc"}}
											_, err = tx.CreateNode(node)
											require.NoError(t, err)
										}
										node.Properties = props
										node.UpdatedAt = time.Now()
										require.NoError(t, tx.UpdateNode(node))
									} else {
										id := storage.EdgeID(db + ":e")
										edge, err := tx.GetStoredEdge(id)
										require.NoError(t, err)
										if operation == "create-set" || operation == "delete-recreate" {
											require.NoError(t, tx.DeleteEdge(id))
											edge = &storage.Edge{ID: id, StartNode: storage.NodeID(db + ":a"), EndNode: storage.NodeID(db + ":b"), Type: "LINK"}
											require.NoError(t, tx.CreateEdge(storage.CopyEdge(edge)))
										}
										edge.Properties = props
										edge.UpdatedAt = time.Now()
										require.NoError(t, tx.UpdateEdge(edge))
									}
									require.NoError(t, tracker.CommitTransaction(context.Background(), tx))
								}
								require.NoError(t, base.FlushWriteBehind())
								count, err := engine.NodeCount()
								require.NoError(t, err)
								require.Equal(t, wantNodes, count)
								count, err = engine.EdgeCount()
								require.NoError(t, err)
								require.Equal(t, wantEdges, count)
								nodes, edges, err := tracker.checker.calculateCurrentStorageSize(tracker.Engine)
								require.NoError(t, err)
								total, gotNodes, gotEdges := manager.GetStorageSize(db)
								require.Equal(t, nodes, gotNodes)
								require.Equal(t, edges, gotEdges)
								require.Equal(t, nodes+edges, total)
								require.LessOrEqual(t, total, before)
								if attempt == 1 {
									require.Equal(t, before, total, "unchanged payload does not consume quota")
								}
							}
						})
					}
				}
			}
		})
	}
}
