package search

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The startup build restores the saved image and its capacity before live
// writes are checked against it; a write never runs the population itself.
func TestNodePopulationRestoresCapacityBeforeFirstMutation(t *testing.T) {
	for _, backend := range []string{"v1", "v2"} {
		for _, traced := range []bool{false, true} {
			for _, operation := range []string{"new_node", "index_node", "remove_node", "index_edge", "remove_edge"} {
				t.Run(fmt.Sprintf("%s/traced=%t/%s", backend, traced, operation), func(t *testing.T) {
					engine := &authoritativeSourceEngine{MemoryEngine: fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))}
					t.Cleanup(func() { require.NoError(t, engine.Close()) })
					var source storage.Engine = engine
					if traced {
						source = storage.NewTracedEngine(engine)
					}
					a := admissionNode("nornic:a", "aterm", []float32{1, 0, 0}, "named")
					_, err := engine.CreateNode(a)
					require.NoError(t, err)
					dir := t.TempDir()
					seed := configureNodeAdmissionService(t, source, dir, backend, &ServiceOptions{BM25MemoryMaxBytes: 1 << 20})
					require.NoError(t, seed.BuildIndexes(context.Background()))
					budget := seed.bm25ResidentBytes
					require.Positive(t, budget)
					require.NoError(t, seed.Close())
					if operation != "new_node" {
						budget--
					}
					svc := configureNodeAdmissionService(t, source, dir, backend, &ServiceOptions{BM25MemoryMaxBytes: budget})
					svc.EnableRelationshipSearch(nil)
					existing := &storage.Edge{ID: "r", Type: "REL", Properties: map[string]any{"text": "oldedge"}}
					svc.IndexRelationship(existing)
					b := admissionNode("nornic:b", "bterm", []float32{0, 1, 0}, "named")
					// The startup build restores the saved image and charges it
					// before any live write is checked; writes never populate.
					buildErr := svc.BuildIndexes(context.Background())
					if operation == "new_node" {
						require.NoError(t, buildErr, "the saved image fits the budget exactly")
						assert.True(t, svc.IsReady())
						require.ErrorIs(t, svc.IndexNode(b), ErrIndexMemoryBudgetExceeded, "the first write is checked against the restored capacity")
						assert.Equal(t, budget, svc.bm25ResidentBytes)
						assert.Contains(t, svc.indexCapacityByNode, "nornic:a")
						assert.NotContains(t, svc.indexCapacityByNode, "nornic:b")
						assert.NotContains(t, svc.nodeNamedVector, "nornic:b")
						assert.Empty(t, svc.fulltext().Search("bterm", 10))
						assert.NotEmpty(t, svc.fulltext().Search("aterm", 10))
						return
					}
					require.ErrorIs(t, buildErr, ErrIndexMemoryBudgetExceeded, "an oversized saved image is refused by the build")
					assert.False(t, svc.IsReady())
					assert.False(t, svc.nodeSourcesCompleteLocked())
					assert.Contains(t, svc.indexCapacityByNode, "nornic:a")
					assert.Greater(t, svc.bm25ResidentBytes, budget, "an oversized installed population stays charged")
					// After the failed build a write applies directly, checked against
					// the charged capacity: a BM25 addition is refused; a removal, or an
					// edge vector the BM25 budget does not cover, applies.
					switch operation {
					case "index_node":
						require.ErrorIs(t, svc.IndexNode(b), ErrIndexMemoryBudgetExceeded)
						assert.NotContains(t, svc.nodeNamedVector, "nornic:b")
						assert.Empty(t, svc.fulltext().Search("bterm", 10))
					case "index_edge":
						require.NoError(t, svc.IndexEdge(&storage.Edge{ID: "next", Type: "REL", Properties: map[string]any{"text": "newedge", "v": []float32{0, 1, 0}}}))
						assert.Contains(t, svc.edgePropVector, "next")
					case "remove_node":
						require.NoError(t, svc.RemoveNode(a.ID))
						assert.NotContains(t, svc.indexCapacityByNode, "nornic:a")
						assert.Empty(t, svc.fulltext().Search("aterm", 10))
					case "remove_edge":
						require.NoError(t, svc.RemoveEdge(existing.ID))
						rs := svc.relationshipSearch()
						rs.mu.RLock()
						assert.NotContains(t, rs.entries, "r")
						rs.mu.RUnlock()
					}
				})
			}
		}
	}
}
