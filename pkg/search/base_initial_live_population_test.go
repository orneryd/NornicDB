package search

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBaseInitialPopulationIncludesLiveWritesBeforeFirstBuild(t *testing.T) {
	for _, backend := range []string{"v1", "v2"} {
		for _, traced := range []bool{false, true} {
			for _, kind := range []string{"live_before_first_build", "fresh_restore", "already_built"} {
				t.Run(fmt.Sprintf("%s/traced=%t/%s", backend, traced, kind), func(t *testing.T) {
					ctx := context.Background()
					engine := &authoritativeSourceEngine{MemoryEngine: fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))}
					t.Cleanup(func() { require.NoError(t, engine.Close()) })
					var source storage.Engine = engine
					if traced {
						source = storage.NewTracedEngine(engine)
					}
					_, iterator := source.(NodeIterator)
					require.Equal(t, !traced, iterator)
					a := &storage.Node{ID: "nornic:a", Labels: []string{"Doc"}, Properties: map[string]any{"text": "aterm"}, NamedEmbeddings: map[string][]float32{"v": {1, 0, 0}}}
					_, err := engine.CreateNode(a)
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
					seed := makeService()
					require.NoError(t, seed.BuildIndexes(ctx))
					require.NoError(t, seed.Close())
					svc := makeService()
					if kind == "already_built" {
						require.NoError(t, svc.BuildIndexes(ctx))
					}
					if kind != "fresh_restore" {
						b := &storage.Node{ID: "nornic:b", Labels: []string{"Doc"}, Properties: map[string]any{"text": "bterm"}, NamedEmbeddings: map[string][]float32{"v": {0, 1, 0}}}
						_, err := engine.CreateNode(b)
						require.NoError(t, err)
						require.NoError(t, svc.IndexNode(b))
					}
					engine.nodeReads = 0
					engine.denyNodeRead = kind != "live_before_first_build"
					require.NoError(t, svc.BuildIndexes(ctx))
					require.True(t, svc.IsReady())
					if engine.denyNodeRead {
						assert.Zero(t, engine.nodeReads)
					}
					for _, text := range []string{"aterm", "bterm"} {
						if text == "bterm" && kind == "fresh_restore" {
							continue
						}
						resp, err := svc.Search(ctx, text, nil, &SearchOptions{Limit: 10})
						require.NoError(t, err)
						if assert.Len(t, resp.Results, 1, "complete public text population for %s", text) {
							assert.Equal(t, "nornic:"+text[:1], resp.Results[0].ID)
						}
					}
					wantCount := 2
					if kind == "fresh_restore" {
						wantCount = 1
					}
					hits, err := svc.VectorQueryNodes(ctx, []float32{1, 0, 0}, VectorQuerySpec{Label: "Doc", Property: "v", Similarity: "dot", Limit: 10})
					require.NoError(t, err)
					if assert.Len(t, hits, wantCount, "complete exact node metadata population") {
						assert.Equal(t, "nornic:a", hits[0].ID)
						assert.InDelta(t, 1, hits[0].Score, 1e-6)
					}
					require.NotNil(t, svc.vectorFileStore)
					assert.Equal(t, wantCount, svc.vectorFileStore.Count())
					assert.Zero(t, svc.vectorIndex.Count())
					assert.Zero(t, svc.vectorResidentBytes)
					assert.Contains(t, svc.nodeLabels, "nornic:a")
					assert.Contains(t, svc.nodeNamedVector, "nornic:a")
					assert.Contains(t, svc.indexCapacityByNode, "nornic:a")
					assert.Greater(t, svc.indexCapacityByNode["nornic:a"].vectorMetadata, int64(0))
				})
			}
		}
	}
}
