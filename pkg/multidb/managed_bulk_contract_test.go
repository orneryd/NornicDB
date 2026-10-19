package multidb

import (
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestManagedBulkPreservesReplacementContract(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, entity := range []string{"node", "edge"} {
			for _, hidden := range []bool{false, true} {
				for _, quota := range []string{"count", "bytes"} {
					t.Run(fmt.Sprintf("buffered=%v/%s/hidden=%v/%s", buffered, entity, hidden, quota), func(t *testing.T) {
						base, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), WriteBehind: buffered})
						require.NoError(t, err)
						manager, err := NewDatabaseManager(base, nil)
						require.NoError(t, err)
						t.Cleanup(func() { require.NoError(t, manager.Close()) })
						const db = "bulk_accounting"
						require.NoError(t, manager.CreateDatabase(db))
						engine, err := manager.GetStorage(db)
						require.NoError(t, err)
						for _, id := range []storage.NodeID{"a", "b", "n"} {
							_, err = engine.CreateNode(&storage.Node{ID: id, Properties: map[string]any{"payload": strings.Repeat("x", 2048)}})
							require.NoError(t, err)
						}
						require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "LINK", Properties: map[string]any{"payload": strings.Repeat("x", 2048)}}))
						if hidden {
							if entity == "node" {
								require.NoError(t, base.SetNodeManuallySuppressed(db+":n", true))
							} else {
								require.NoError(t, base.SetEdgeManuallySuppressed(db+":e", true))
							}
						}
						before, _, _ := manager.GetStorageSize(db)
						limits := &Limits{Storage: StorageLimits{MaxNodes: 3, MaxEdges: 1}}
						if quota == "bytes" {
							limits = &Limits{Storage: StorageLimits{MaxBytes: before}}
						}
						require.NoError(t, manager.SetDatabaseLimits(db, limits))
						if entity == "node" {
							err = engine.BulkCreateNodes([]*storage.Node{{ID: "n", Properties: map[string]any{"payload": "small"}}})
						} else {
							err = engine.BulkCreateEdges([]*storage.Edge{{ID: "e", StartNode: "a", EndNode: "b", Type: "LINK", Properties: map[string]any{"payload": "small"}}})
						}
						// Relationship creation supports same-ID hidden replacement;
						// a node's physical ID remains unique, hidden or visible.
						replaced := hidden && entity == "edge"
						if replaced {
							require.NoError(t, err)
						} else {
							require.ErrorIs(t, err, storage.ErrAlreadyExists)
						}
						for replay := 0; replay < 2; replay++ {
							if replay == 1 {
								require.NoError(t, base.FlushWriteBehind())
							}
							nodes, edges, err := manager.calculateStorageSizeFromEngine(engine)
							require.NoError(t, err)
							after, _, _ := manager.GetStorageSize(db)
							require.Equal(t, nodes+edges, after)
							nc, err := engine.NodeCount()
							require.NoError(t, err)
							require.EqualValues(t, 3, nc)
							ec, err := engine.EdgeCount()
							require.NoError(t, err)
							require.EqualValues(t, 1, ec)
							want := strings.Repeat("x", 2048)
							if replaced {
								want = "small"
							}
							if entity == "node" {
								node, err := storage.GetStoredNode(engine, "n")
								require.NoError(t, err)
								require.Equal(t, want, node.Properties["payload"])
							} else {
								edge, err := storage.GetStoredEdge(engine, "e")
								require.NoError(t, err)
								require.Equal(t, want, edge.Properties["payload"])
							}
						}
					})
				}
			}
		}
	}
}
