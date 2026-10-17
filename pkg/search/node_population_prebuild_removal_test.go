package search

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// prebuildSourceMembership reports what the node sources hold for a node,
// read before any Search hydration: its BM25 document, its named vector entry
// in the active vector source, and its named-vector query metadata.
func prebuildSourceMembership(svc *Service, id string) (text, vector, metadata bool) {
	svc.indexMu.RLock()
	defer svc.indexMu.RUnlock()
	_, text = svc.fulltext().GetDocument(id)
	key := id + "-named-v"
	svc.mu.RLock()
	if svc.vectorFileStore != nil {
		vector = svc.vectorFileStore.Has(key)
	} else if svc.vectorIndex != nil {
		vector = svc.vectorIndex.HasVector(key)
	}
	svc.mu.RUnlock()
	_, metadata = svc.nodeNamedVector[id]
	return text, vector, metadata
}

// A node write before the first population retires the saved node images:
// a node removed or emptied then stays gone, whatever later writes do before
// the population (another node's vector or text, several writes), and the
// population, from an explicit build or from the persist, completes the
// sources from storage. Membership is checked on the sources themselves
// before any query, then through public keyword and vector queries.
func TestNodePopulationKeepsRemovalsBeforeFirstBuild(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	for _, backend := range []string{"v1", "v2"} {
		for _, first := range []string{"removed", "emptied"} {
			for _, then := range []string{"nothing", "vector_write", "text_write", "several_writes"} {
				for _, route := range []string{"explicit_build", "persist"} {
					t.Run(fmt.Sprintf("%s/%s/%s/%s", backend, first, then, route), func(t *testing.T) {
						ctx := context.Background()
						engine := &authoritativeSourceEngine{MemoryEngine: storage.NewMemoryEngine()}
						t.Cleanup(func() { require.NoError(t, engine.Close()) })
						dir := t.TempDir()
						a := admissionNode("nornic:a", "aterm", []float32{1, 0, 0}, "named")
						b := admissionNode("nornic:b", "bterm", []float32{0, 1, 0}, "named")
						for _, n := range []*storage.Node{a, b} {
							_, err := engine.CreateNode(n)
							require.NoError(t, err)
						}
						seed := configureNodeAdmissionService(t, engine, dir, backend, nil)
						require.NoError(t, seed.BuildIndexes(ctx))
						require.NoError(t, seed.Close())

						svc := configureNodeAdmissionService(t, engine, dir, backend, nil)
						switch first {
						case "removed":
							require.NoError(t, engine.DeleteNode(a.ID))
							require.NoError(t, svc.RemoveNode(a.ID))
						case "emptied":
							a.Labels, a.Properties, a.NamedEmbeddings = nil, nil, nil
							require.NoError(t, engine.UpdateNode(a))
							require.NoError(t, svc.IndexNode(a))
						}
						index := func(n *storage.Node) {
							_, err := engine.CreateNode(n)
							require.NoError(t, err)
							require.NoError(t, svc.IndexNode(n))
						}
						want := map[string]string{"bterm": "nornic:b"}
						switch then {
						case "vector_write":
							index(admissionNode("nornic:c", "cterm", []float32{0, 0, 1}, "named"))
							want["cterm"] = "nornic:c"
						case "text_write":
							index(admissionNode("nornic:d", "dterm", nil, "text_only"))
							want["dterm"] = "nornic:d"
						case "several_writes":
							index(admissionNode("nornic:c", "cterm", []float32{0, 0, 1}, "named"))
							index(admissionNode("nornic:d", "dterm", nil, "text_only"))
							b.Properties["text"] = "bnewterm"
							require.NoError(t, engine.UpdateNode(b))
							require.NoError(t, svc.IndexNode(b))
							delete(want, "bterm")
							want["cterm"], want["bnewterm"] = "nornic:c", "nornic:b"
						}
						if route == "persist" {
							svc.PersistIndexesToDisk()
							require.NoError(t, svc.Close())
							svc = configureNodeAdmissionService(t, engine, dir, backend, nil)
						}
						require.NoError(t, svc.BuildIndexes(ctx))

						text, vector, metadata := prebuildSourceMembership(svc, "nornic:a")
						assert.False(t, text, "the BM25 source must not hold A")
						assert.False(t, vector, "the vector source must not hold A")
						assert.False(t, metadata, "the query metadata must not hold A")
						text, vector, metadata = prebuildSourceMembership(svc, "nornic:b")
						assert.True(t, text && vector && metadata, "B stays in every source")

						for _, term := range []string{"aterm", "bterm"} {
							if _, kept := want[term]; kept {
								continue
							}
							resp, err := svc.Search(ctx, term, nil, &SearchOptions{Limit: 10})
							require.NoError(t, err)
							assert.Empty(t, resp.Results, term)
						}
						for term, id := range want {
							resp, err := svc.Search(ctx, term, nil, &SearchOptions{Limit: 10})
							require.NoError(t, err)
							if assert.Len(t, resp.Results, 1, term) {
								assert.Equal(t, id, resp.Results[0].ID)
							}
						}
						hits, err := svc.VectorQueryNodes(ctx, []float32{1, 0, 0}, VectorQuerySpec{Property: "v", Similarity: "dot", Limit: 10})
						require.NoError(t, err)
						for _, hit := range hits {
							assert.NotEqual(t, "nornic:a", hit.ID, "the old vector of A is gone")
						}
					})
				}
			}
		}
	}
}
