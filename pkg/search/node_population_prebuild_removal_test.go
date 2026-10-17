package search

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A live write before the first population that takes indexed content away
// (a node emptied of its text and vectors, or removed) leaves nothing to
// remove from the empty resident indexes. The population that follows, from
// an explicit build or from the persist, must not restore the saved images
// over it: the node's old terms and vectors stay gone, and B is kept.
func TestNodePopulationKeepsRemovalsBeforeFirstBuild(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	for _, backend := range []string{"v1", "v2"} {
		for _, mutation := range []string{"emptied", "removed"} {
			for _, route := range []string{"explicit_build", "persist"} {
				t.Run(fmt.Sprintf("%s/%s/%s", backend, mutation, route), func(t *testing.T) {
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
					switch mutation {
					case "emptied":
						a.Labels, a.Properties, a.NamedEmbeddings = nil, nil, nil
						require.NoError(t, engine.UpdateNode(a))
						require.NoError(t, svc.IndexNode(a))
					case "removed":
						require.NoError(t, engine.DeleteNode(a.ID))
						require.NoError(t, svc.RemoveNode(a.ID))
					}
					if route == "persist" {
						svc.PersistIndexesToDisk()
						require.NoError(t, svc.Close())
						svc = configureNodeAdmissionService(t, engine, dir, backend, nil)
					}
					require.NoError(t, svc.BuildIndexes(ctx))

					resp, err := svc.Search(ctx, "aterm", nil, &SearchOptions{Limit: 10})
					require.NoError(t, err)
					assert.Empty(t, resp.Results, "the old terms of A are gone")
					resp, err = svc.Search(ctx, "bterm", nil, &SearchOptions{Limit: 10})
					require.NoError(t, err)
					if assert.Len(t, resp.Results, 1) {
						assert.Equal(t, "nornic:b", resp.Results[0].ID)
					}
					hits, err := svc.VectorQueryNodes(ctx, []float32{1, 0, 0}, VectorQuerySpec{Property: "v", Similarity: "dot", Limit: 10})
					require.NoError(t, err)
					for _, hit := range hits {
						assert.NotEqual(t, "nornic:a", hit.ID, "the old vector of A is gone")
					}
					svc.indexMu.RLock()
					assert.NotContains(t, svc.nodeNamedVector, "nornic:a")
					assert.Contains(t, svc.nodeNamedVector, "nornic:b")
					svc.indexMu.RUnlock()
				})
			}
		}
	}
}
