package search_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSearchOriginFirstLazyReadDrop(t *testing.T) {
	for _, test := range []struct{ persisted, recreate, drop bool }{{false, false, true}, {false, true, true}, {true, false, true}, {true, true, true}, {true, false, false}} {
		persisted, recreate := test.persisted, test.recreate
		t.Run(fmt.Sprintf("persisted=%t/recreate=%t/drop=%t", persisted, recreate, test.drop), func(t *testing.T) {
			cfg := nornicdb.DefaultConfig()
			cfg.Memory.EmbeddingEnabled = false
			cfg.Memory.EmbeddingDimensions = 3
			cfg.Memory.DecayEnabled = false
			cfg.Memory.AutoLinksEnabled = false
			cfg.Memory.SearchBM25Enabled = true
			cfg.Memory.SearchVectorEnabled = true
			cfg.Memory.SearchBM25Warming = "lazy"
			cfg.Memory.SearchVectorWarming = "lazy"
			db, err := nornicdb.Open(t.TempDir(), cfg)
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			ctx := context.Background()
			engine := db.GetBaseStorageForManager()
			if !test.drop {
				isolated := storage.NewMemoryEngine()
				t.Cleanup(func() { _ = isolated.Close() })
				engine = isolated
			}
			store := storage.NewNamespacedEngine(engine, "lazy_origin")
			svc, err := db.EnsureSearchIndexes(ctx, "lazy_origin", store, nornicdb.SearchIndexBuildWait)
			require.NoError(t, err)
			require.False(t, svc.IsReady())
			exec := cypher.NewStorageExecutor(store)
			exec.SetSearchService(svc)
			create := "CREATE VECTOR INDEX lazyvec FOR (n:Doc) ON (n.embedding)"
			_, err = exec.Execute(ctx, create, nil)
			require.NoError(t, err)
			_, err = store.CreateNode(&storage.Node{ID: "source", Labels: []string{"Doc"}, Properties: map[string]any{"content": "lazyword", "embedding": []float32{1, 0, 0}, "other": []float32{0, 1, 0}}})
			require.NoError(t, err)
			if !test.drop {
				_, err = exec.Execute(ctx, "CREATE VECTOR INDEX unaffected FOR (n:Unused) ON (n.embedding)", nil)
				require.NoError(t, err)
				_, err = exec.Execute(ctx, "CREATE VECTOR INDEX rel_vec FOR ()-[r:LINK]-() ON (r.embedding)", nil)
				require.NoError(t, err)
				_, err = store.CreateNode(&storage.Node{ID: "end", Properties: map[string]any{}})
				require.NoError(t, err)
				require.NoError(t, store.CreateEdge(&storage.Edge{ID: "link", StartNode: "source", EndNode: "end", Type: "LINK", Properties: map[string]any{"embedding": []float32{1, 0, 0}}}))
			}
			if persisted {
				dir := t.TempDir()
				configure := func(s *search.Service) {
					s.SetPersistenceEnabled(true)
					s.SetFulltextIndexPath(filepath.Join(dir, "bm25"))
					s.SetVectorIndexPath(filepath.Join(dir, "vectors"))
					s.SetHNSWIndexPath(filepath.Join(dir, "hnsw"))
				}
				baseline := search.NewService(store, 3, nil)
				configure(baseline)
				require.NoError(t, baseline.BuildIndexes(ctx))
				require.Equal(t, 1, baseline.CountPropertyVectorEntries("embedding"))
				require.NoError(t, baseline.Close())
				configure(svc)
			}
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			search.HoldWarmDispatchForTest(svc, entered, release)
			done := make(chan error, 1)
			go func() { _, err := svc.Search(ctx, "lazyword", nil, search.DefaultSearchOptions()); done <- err }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("first lazy dispatch not reached")
			}
			dropName := "lazyvec"
			if !test.drop {
				dropName = "unaffected"
			}
			_, err = exec.Execute(ctx, "DROP INDEX "+dropName, nil)
			require.NoError(t, err)
			if recreate {
				_, err = exec.Execute(ctx, create, nil)
				require.NoError(t, err)
			}
			unblock()
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(10 * time.Second):
				t.Fatal("lazy read did not finish")
			}
			require.True(t, svc.IsReady())
			if test.drop {
				require.Zero(t, svc.CountPropertyVectorEntries("embedding"), "first-read work acquired its origin after dispatch")
			} else {
				require.Equal(t, 1, svc.CountPropertyVectorEntries("embedding"))
				result, err := exec.Execute(ctx, "CALL db.index.vector.queryRelationships('rel_vec', 1, [1.0,0.0,0.0]) YIELD relationship, score RETURN relationship, score", nil)
				require.NoError(t, err)
				require.Len(t, result.Rows, 1, "persisted node baseline skipped the relationship installer")
			}
			require.Equal(t, 1, svc.CountPropertyVectorEntries("other"))
		})
	}
}
