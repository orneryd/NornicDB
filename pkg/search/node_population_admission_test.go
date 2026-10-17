package search

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func configureNodeAdmissionService(t *testing.T, source storage.Engine, dir, backend string, options *ServiceOptions) *Service {
	t.Helper()
	svc := NewServiceWithDimensionsAndBM25EngineAndOptions(source, 3, backend, options)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	svc.FixVectorDimensions()
	svc.SetFulltextIndexPath(filepath.Join(dir, "bm25"))
	svc.SetVectorIndexPath(filepath.Join(dir, "vectors"))
	svc.SetHNSWIndexPath(filepath.Join(dir, "hnsw"))
	svc.SetPersistenceEnabled(true)
	return svc
}

func admissionNode(id, text string, value []float32, shape string) *storage.Node {
	n := &storage.Node{ID: storage.NodeID(id), Labels: []string{"Doc"}, Properties: map[string]any{"text": text}}
	switch shape {
	case "named":
		n.NamedEmbeddings = map[string][]float32{"v": value}
	case "property":
		n.Properties["v"] = value
	case "chunks":
		n.ChunkEmbeddings = [][]float32{value}
	}
	return n
}

func TestNodePopulationFirstMutationPreservesRestart(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	for _, backend := range []string{"v1", "v2"} {
		for _, traced := range []bool{false, true} {
			for _, operation := range []string{"update", "remove", "text_only", "named", "property", "chunks"} {
				t.Run(fmt.Sprintf("%s/traced=%t/%s", backend, traced, operation), func(t *testing.T) {
					ctx := context.Background()
					engine := &authoritativeSourceEngine{MemoryEngine: storage.NewMemoryEngine()}
					t.Cleanup(func() { require.NoError(t, engine.Close()) })
					var source storage.Engine = engine
					if traced {
						source = storage.NewTracedEngine(engine)
					}
					dir := t.TempDir()
					a := admissionNode("nornic:a", "aterm", []float32{1, 0, 0}, "named")
					_, err := engine.CreateNode(a)
					require.NoError(t, err)
					seed := configureNodeAdmissionService(t, source, dir, backend, nil)
					require.NoError(t, seed.BuildIndexes(ctx))
					require.NoError(t, seed.Close())
					live := configureNodeAdmissionService(t, source, dir, backend, nil)
					wantText := map[string]string{"aterm": "nornic:a"}
					switch operation {
					case "update":
						a.Properties["text"] = "updatedterm"
						a.NamedEmbeddings["v"] = []float32{0, 1, 0}
						require.NoError(t, engine.UpdateNode(a))
						require.NoError(t, live.IndexNode(a))
						delete(wantText, "aterm")
						wantText["updatedterm"] = "nornic:a"
					case "remove":
						require.NoError(t, engine.DeleteNode(a.ID))
						require.NoError(t, live.RemoveNode(a.ID))
						delete(wantText, "aterm")
					default:
						b := admissionNode("nornic:b", "bterm", []float32{0, 1, 0}, operation)
						_, err := engine.CreateNode(b)
						require.NoError(t, err)
						require.NoError(t, live.IndexNode(b))
						wantText["bterm"] = "nornic:b"
					}
					// No middle Build: the mutation itself must have admitted a
					// complete initial population before its immediate saved image.
					live.PersistIndexesToDisk()
					require.NoError(t, live.Close())
					restored := configureNodeAdmissionService(t, source, dir, backend, nil)
					engine.nodeReads = 0
					engine.denyNodeRead = true
					require.NoError(t, restored.BuildIndexes(ctx))
					assert.Zero(t, engine.nodeReads)
					for term, id := range wantText {
						resp, err := restored.Search(ctx, term, nil, &SearchOptions{Limit: 10})
						require.NoError(t, err)
						if assert.Len(t, resp.Results, 1) {
							assert.Equal(t, id, resp.Results[0].ID)
						}
					}
					if operation == "remove" || operation == "update" {
						resp, err := restored.Search(ctx, "aterm", nil, &SearchOptions{Limit: 10})
						require.NoError(t, err)
						assert.Empty(t, resp.Results)
					}
					if operation == "remove" {
						assert.NotContains(t, restored.nodeNamedVector, "nornic:a")
						assert.NotContains(t, restored.indexCapacityByNode, "nornic:a")
					} else {
						value := []float32{1, 0, 0}
						if operation == "update" {
							value = []float32{0, 1, 0}
						}
						hits, err := restored.VectorQueryNodes(ctx, value, VectorQuerySpec{Label: "Doc", Property: "v", Similarity: "dot", Limit: 10})
						require.NoError(t, err)
						require.NotEmpty(t, hits)
						assert.Equal(t, "nornic:a", hits[0].ID)
						assert.InDelta(t, 1, hits[0].Score, 1e-6)
						assert.Contains(t, restored.indexCapacityByNode, "nornic:a")
						if operation != "update" && operation != "text_only" {
							graph, err := restored.getOrCreateHNSWIndex(ctx, 3)
							require.NoError(t, err)
							candidates, err := graph.Search(ctx, []float32{0, 1, 0}, 1, -1)
							require.NoError(t, err)
							require.Len(t, candidates, 1)
							assert.Equal(t, "nornic:b", normalizeVectorResultIDToNodeID(candidates[0].ID))
						}
					}
				})
			}
		}
	}
}

func TestNodePopulationUninitializedPersistAndClosePreserveArtifacts(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	for _, backend := range []string{"v1", "v2"} {
		t.Run(backend, func(t *testing.T) {
			engine := storage.NewMemoryEngine()
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			dir := t.TempDir()
			_, err := engine.CreateNode(admissionNode("nornic:a", "aterm", []float32{1, 0, 0}, "named"))
			require.NoError(t, err)
			seed := configureNodeAdmissionService(t, engine, dir, backend, nil)
			require.NoError(t, seed.BuildIndexes(context.Background()))
			require.NoError(t, seed.Close())
			paths := []string{"bm25", "vectors.vec", "vectors.meta", "hnsw", "build_settings"}
			before := map[string][]byte{}
			for _, p := range paths {
				value, err := os.ReadFile(filepath.Join(dir, p))
				require.NoError(t, err)
				before[p] = value
			}
			fresh := configureNodeAdmissionService(t, engine, dir, backend, nil)
			fresh.PersistIndexesToDisk()
			require.Nil(t, fresh.vectorFileStore)
			require.NoError(t, fresh.Close())
			for _, p := range paths {
				value, err := os.ReadFile(filepath.Join(dir, p))
				require.NoError(t, err)
				assert.Equal(t, before[p], value, p)
			}
		})
	}
}
