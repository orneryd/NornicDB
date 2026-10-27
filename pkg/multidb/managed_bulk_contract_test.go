package multidb

import (
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
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
						base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), WriteBehind: buffered}))
						require.NoError(t, err)
						manager, err := NewDatabaseManager(t.Context(), base, nil)
						require.NoError(t, err)
						t.Cleanup(func() { require.NoError(t, manager.Close()) })
						const db = "bulk_accounting"
						require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: db}))
						engine, err := manager.GetStorage(db)
						require.NoError(t, err)
						for _, id := range []storage.NodeID{"a", "b", "n"} {
							_, err = engine.CreateNode(&storage.Node{ID: id, Properties: map[string]any{"payload": strings.Repeat("x", 2048)}})
							require.NoError(t, err)
						}
						require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "LINK", Properties: map[string]any{"payload": strings.Repeat("x", 2048)}}))
						if hidden {
							if entity == "node" {
								require.NoError(t, base.SetManualSuppression(storage.StoredEntityNodeKind, string(db+":n"), true))
							} else {
								require.NoError(t, base.SetManualSuppression(storage.StoredEntityEdgeKind, string(db+":e"), true))
							}
						}
						before, _, _ := manager.GetStorageSize(db)
						limits := &Limits{Storage: StorageLimits{MaxNodes: 3, MaxEdges: 1}}
						if quota == "bytes" {
							limits = &Limits{Storage: StorageLimits{MaxBytes: before}}
						}
						require.NoError(t, manager.SetDatabaseLimits(t.Context(), db, limits))
						if entity == "node" {
							err = engine.BulkCreateNodes([]*storage.Node{{ID: "n", Properties: map[string]any{"payload": "small"}}})
						} else {
							err = engine.BulkCreateEdges([]*storage.Edge{{ID: "e", StartNode: "a", EndNode: "b", Type: "LINK", Properties: map[string]any{"payload": "small"}}})
						}
						// Released create identity is physical: a different final
						// image cannot overwrite even a hidden same-ID edge.
						require.ErrorIs(t, err, storage.ErrAlreadyExists)
						if hidden && entity == "edge" {
							prior, readErr := storage.GetStoredEdge(engine, "e")
							require.NoError(t, readErr)
							require.NoError(t, engine.BulkCreateEdges([]*storage.Edge{prior}), "the identical hidden image remains an idempotent no-op at quota")
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
