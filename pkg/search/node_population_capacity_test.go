package search

import (
	"context"
	"fmt"
	"testing"
	"time"

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

// The budget is only known once a population has admitted NODE, so on a
// budgeted service a write waits for that admission; an unbudgeted write never
// waits (F-I86, orchestrator decision on budgets).
func TestNodePopulationWriteWaitsOnlyWhenBudgeted(t *testing.T) {
	for _, budgeted := range []bool{false, true} {
		t.Run(fmt.Sprintf("budgeted=%t", budgeted), func(t *testing.T) {
			engine := &traversalGatedEngine{MemoryEngine: storage.NewMemoryEngine()}
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			var options *ServiceOptions
			if budgeted {
				options = &ServiceOptions{BM25MemoryMaxBytes: 1 << 30}
			}
			svc := configureNodeAdmissionService(t, engine, t.TempDir(), "v2", options)
			entered, release := engine.arm()
			defer release()

			a := admissionNode("nornic:a", "aterm", []float32{1, 0, 0}, "named")
			_, err := engine.CreateNode(a)
			require.NoError(t, err)
			written := make(chan error, 1)
			go func() { written <- svc.IndexNode(a) }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("the write did not start the population")
			}
			if !budgeted {
				select {
				case err := <-written:
					require.NoError(t, err)
				case <-time.After(time.Second):
					t.Fatal("an unbudgeted write waited for the population")
				}
				resp, err := svc.Search(context.Background(), "aterm", nil, &SearchOptions{Limit: 10})
				require.NoError(t, err)
				require.Len(t, resp.Results, 1, "the unbudgeted write is searchable at once")
				return
			}
			select {
			case err := <-written:
				t.Fatalf("a budgeted write returned (%v) before its budget was known", err)
			case <-time.After(200 * time.Millisecond):
			}
			release()
			select {
			case err := <-written:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("the budgeted write did not finish after NODE admission")
			}
			resp, err := svc.Search(context.Background(), "aterm", nil, &SearchOptions{Limit: 10})
			require.NoError(t, err)
			require.Len(t, resp.Results, 1)
		})
	}
}
