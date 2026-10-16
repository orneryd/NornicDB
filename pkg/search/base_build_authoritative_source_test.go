package search

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type authoritativeSourceEngine struct {
	*storage.MemoryEngine
	nodeReads    int
	denyNodeRead bool
}

func (e *authoritativeSourceEngine) IterateNodes(fn func(*storage.Node) bool) error {
	e.nodeReads++
	if e.denyNodeRead {
		return errors.New("valid fresh artifacts must skip node traversal")
	}
	return e.MemoryEngine.IterateNodes(fn)
}

func (e *authoritativeSourceEngine) AllNodes() ([]*storage.Node, error) {
	e.nodeReads++
	if e.denyNodeRead {
		return nil, errors.New("valid fresh artifacts must skip node traversal")
	}
	return e.MemoryEngine.AllNodes()
}

func TestBaseBuildRespectsAuthoritativeMemoryBeforeDiskRestore(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	for _, backend := range []string{"v1", "v2"} {
		for _, traced := range []bool{false, true} {
			for _, kind := range []string{"update", "replace_membership", "changed_text", "active_source", "unchanged", "fresh_restart"} {
				t.Run(fmt.Sprintf("%s/traced=%t/%s", backend, traced, kind), func(t *testing.T) {
					ctx := context.Background()
					engine := &authoritativeSourceEngine{MemoryEngine: storage.NewMemoryEngine()}
					t.Cleanup(func() { require.NoError(t, engine.Close()) })
					var source storage.Engine = engine
					if traced {
						source = storage.NewTracedEngine(engine)
					}
					_, iterator := source.(NodeIterator)
					require.Equal(t, !traced, iterator)
					node := &storage.Node{ID: "nornic:a", Labels: []string{"Doc"}, Properties: map[string]any{"text": "shared"}, NamedEmbeddings: map[string][]float32{"v": {1, 0, 0}}}
					_, err := engine.CreateNode(node)
					require.NoError(t, err)
					dir := t.TempDir()
					makeService := func() *Service {
						svc := NewServiceWithDimensionsAndBM25EngineAndOptions(source, 3, backend, nil)
						t.Cleanup(func() { require.NoError(t, svc.Close()) })
						svc.FixVectorDimensions()
						svc.SetFulltextIndexPath(filepath.Join(dir, "bm25"))
						svc.SetVectorIndexPath(filepath.Join(dir, "vectors"))
						svc.SetHNSWIndexPath(filepath.Join(dir, "hnsw"))
						svc.SetPersistenceEnabled(true)
						return svc
					}
					svc := makeService()
					require.NoError(t, svc.BuildIndexes(ctx))
					require.Greater(t, svc.fulltext().Count(), 0)
					require.NotNil(t, svc.vectorFileStore)
					require.Equal(t, 1, svc.vectorFileStore.Count())
					currentID := "nornic:a"
					current := []float32{1, 0, 0}
					if kind == "fresh_restart" {
						require.NoError(t, svc.Close())
						svc = makeService()
						engine.nodeReads = 0
						engine.denyNodeRead = true
					} else {
						if kind != "active_source" {
							svc.SetPersistenceEnabled(false)
							require.Nil(t, svc.vectorFileStore)
						}
						if kind == "update" || kind == "changed_text" {
							current = []float32{0, 1, 0}
							node.NamedEmbeddings = map[string][]float32{"v": current}
							if kind == "changed_text" {
								node.Properties["text"] = "currentterm"
							}
							require.NoError(t, engine.UpdateNode(node))
							require.NoError(t, svc.IndexNode(node))
						} else if kind == "replace_membership" || kind == "active_source" {
							require.NoError(t, engine.DeleteNode(node.ID))
							require.NoError(t, svc.RemoveNode(node.ID))
							currentID, current = "nornic:replacement", []float32{0, 1, 0}
							node = &storage.Node{ID: storage.NodeID(currentID), Labels: []string{"Doc"}, Properties: map[string]any{"text": "shared"}, NamedEmbeddings: map[string][]float32{"v": current}}
							_, err := engine.CreateNode(node)
							require.NoError(t, err)
							require.NoError(t, svc.IndexNode(node))
						}
						stored, ok := svc.getVectorForCypher(currentID + "-named-v")
						require.True(t, ok)
						require.Equal(t, current, stored)
						svc.SetPersistenceEnabled(true)
						// No IndexNode or other migration call after enable: rebuild
						// must itself select the known current source.
					}
					require.NoError(t, svc.BuildIndexes(ctx))
					require.True(t, svc.IsReady())
					if kind == "changed_text" {
						hits := svc.fulltext().Search("currentterm", 1)
						require.Len(t, hits, 1)
						assert.Equal(t, currentID, hits[0].ID)
					}
					if kind == "fresh_restart" {
						assert.Zero(t, engine.nodeReads)
					}
					hits, err := svc.VectorQueryNodes(ctx, current, VectorQuerySpec{Label: "Doc", Property: "v", Similarity: "dot", Limit: 1})
					require.NoError(t, err)
					if assert.Len(t, hits, 1) {
						assert.Equal(t, currentID, hits[0].ID)
						assert.InDelta(t, 1, hits[0].Score, 1e-6, "old file coordinates must not replace current memory")
					}
					require.NotNil(t, svc.vectorFileStore)
					stored, ok := svc.vectorFileStore.GetVector(currentID + "-named-v")
					assert.True(t, ok)
					assert.Equal(t, current, stored)
					assert.Zero(t, svc.vectorIndex.Count(), "file publication must retire duplicate resident payload")
					assert.Zero(t, svc.vectorResidentBytes)
					assert.Contains(t, svc.nodeNamedVector, currentID)
					assert.Contains(t, svc.indexCapacityByNode, currentID)
					graph, err := svc.getOrCreateHNSWIndex(ctx, 3)
					require.NoError(t, err)
					require.NotNil(t, graph)
					candidates, err := graph.Search(ctx, current, 1, -1)
					require.NoError(t, err)
					if assert.Len(t, candidates, 1) {
						assert.Equal(t, currentID+"-named-v", candidates[0].ID)
					}
					if kind == "replace_membership" || kind == "active_source" {
						assert.False(t, svc.vectorFileStore.Has("nornic:a-named-v"))
						assert.NotContains(t, svc.nodeNamedVector, "nornic:a")
					}
				})
			}
		}
	}
}
