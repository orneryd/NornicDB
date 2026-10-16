package search

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNodePopulationRestoresCapacityBeforeFirstMutation(t *testing.T) {
	for _, backend := range []string{"v1", "v2"} {
		for _, traced := range []bool{false, true} {
			for _, operation := range []string{"new_node", "index_node", "remove_node", "index_edge", "remove_edge"} {
				t.Run(fmt.Sprintf("%s/traced=%t/%s", backend, traced, operation), func(t *testing.T) {
					engine := &authoritativeSourceEngine{MemoryEngine: storage.NewMemoryEngine()}
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
					switch operation {
					case "new_node", "index_node":
						err = svc.IndexNode(b)
					case "remove_node":
						err = svc.RemoveNode(a.ID)
					case "index_edge":
						err = svc.IndexEdge(&storage.Edge{ID: "next", Type: "REL", Properties: map[string]any{"text": "newedge", "v": []float32{0, 1, 0}}})
					case "remove_edge":
						err = svc.RemoveEdge(existing.ID)
					}
					require.ErrorIs(t, err, ErrIndexMemoryBudgetExceeded)
					assert.False(t, svc.IsReady())
					assert.Contains(t, svc.indexCapacityByNode, "nornic:a")
					assert.NotContains(t, svc.indexCapacityByNode, "nornic:b")
					assert.NotContains(t, svc.nodeNamedVector, "nornic:b")
					assert.Empty(t, svc.edgePropVector)
					assert.Empty(t, svc.fulltext().Search("bterm", 10))
					assert.NotEmpty(t, svc.fulltext().Search("aterm", 10))
					rs := svc.relationshipSearch()
					rs.mu.RLock()
					assert.Contains(t, rs.entries, "r", "failed admission must precede RemoveRelationship")
					assert.NotContains(t, rs.entries, "next", "failed admission must precede IndexRelationship")
					rs.mu.RUnlock()
					if operation == "new_node" {
						assert.Equal(t, budget, svc.bm25ResidentBytes)
						assert.True(t, svc.nodeSourcesCompleteLocked(), "the old admitted population survives a rejected new write")
					} else {
						assert.Greater(t, svc.bm25ResidentBytes, budget, "an oversized installed population stays charged")
						assert.False(t, svc.nodeSourcesCompleteLocked())
						svc.persistMu.Lock()
						timer := svc.persistTimer
						svc.persistMu.Unlock()
						assert.Nil(t, timer, "failed initial admission must precede scheduling persistence")
					}
				})
			}
		}
	}
}
