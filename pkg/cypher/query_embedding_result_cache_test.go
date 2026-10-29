package cypher

import (
	"context"
	"reflect"
	"runtime"
	"testing"
	"weak"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestQueryEmbeddingPlainCacheProviderReplacement(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "review"))
	ctx := context.Background()
	const query = "CALL db.index.vector.embed('alpha') YIELD embedding RETURN embedding"
	exec.SetEmbedder(&stubVectorEmbedder{vec: []float32{1, 0}})
	first, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, []float32{1, 0}, toFloat32Slice(first.Rows[0][0]))
	exec.SetEmbedder(&stubVectorEmbedder{vec: []float32{0, 1}})
	second, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, []float32{0, 1}, toFloat32Slice(second.Rows[0][0]), "a new query after replacement must use the new provider")
}

func TestQueryEmbeddingResultCacheOverlappingPublication(t *testing.T) {
	for _, fabricQuery := range []bool{false, true} {
		name := "plain"
		if fabricQuery {
			name = "fabric"
		}
		t.Run(name, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "overlap"))
			query := "CALL db.index.vector.embed('alpha') YIELD embedding RETURN embedding"
			if fabricQuery {
				exec, _ = compositeTestFixture(t)
				query = "CALL { USE comp.a " + query + " } RETURN embedding"
			}
			old := &blockingEmbedder{started: make(chan struct{}, 1), release: make(chan struct{}), result: []float32{1, 0}}
			exec.SetEmbedder(old)
			finished := make(chan *ExecuteResult, 1)
			failed := make(chan error, 1)
			go func() {
				result, err := exec.Execute(context.Background(), query, nil)
				finished <- result
				failed <- err
			}()
			<-old.started
			exec.SetEmbedder(&stubVectorEmbedder{vec: []float32{0, 1}})
			ready, err := exec.Execute(context.Background(), query, nil)
			require.NoError(t, err)
			require.Equal(t, []float32{0, 1}, toFloat32Slice(ready.Rows[0][0]))
			close(old.release)
			require.Equal(t, []float32{1, 0}, toFloat32Slice((<-finished).Rows[0][0]))
			require.NoError(t, <-failed)
			ready, err = exec.Execute(context.Background(), query, nil)
			require.NoError(t, err)
			require.Equal(t, []float32{0, 1}, toFloat32Slice(ready.Rows[0][0]), "old completion must retain its pre-execution generation")
		})
	}
}

func TestQueryEmbeddingPublicationReclaimsTransientScopes(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "transient")
	base := NewStorageExecutor(store)
	var states []weak.Pointer[queryEmbedderState]
	var services []weak.Pointer[search.Service]
	for range 24 {
		base.SetEmbedder(&stubVectorEmbedder{vec: []float32{1, 0}})
		states = append(states, weak.Make(base.currentQueryEmbedder()))
		_, err := base.Execute(context.Background(), "CALL db.index.vector.embed('alpha')", nil)
		require.NoError(t, err)
	}
	base.SetEmbedder(nil)
	for range 24 {
		scope := NewStorageExecutor(store)
		scope.ShareEmbedderFrom(base)
		svc := search.NewService(store, 2, nil)
		scope.SetSearchService(svc)
		services = append(services, weak.Make(svc))
		require.NoError(t, svc.Close())
	}
	runtime.GC()
	runtime.GC()
	live := search.NewService(store, 2, nil)
	defer live.Close()
	base.SetSearchService(live)
	for _, ref := range states {
		require.Nil(t, ref.Value(), "replaced providers and embedding caches must be collected")
	}
	for _, ref := range services {
		require.Nil(t, ref.Value(), "closed scope services must be collected")
	}
	// Weak subscribers can be collected while their registry keys still leak.
	// Check the owner's retained collection entries as well as the referents.
	owner := reflect.ValueOf(base.queryEmbedder).Elem()
	for i := range owner.NumField() {
		field := owner.Field(i)
		if field.Kind() == reflect.Map || field.Kind() == reflect.Slice {
			require.LessOrEqual(t, field.Len(), 1, "transient scopes must not accumulate owner bookkeeping")
		}
	}
	runtime.KeepAlive(base)
}

func TestQueryEmbeddingFabricCacheProviderReplacement(t *testing.T) {
	exec, manager := compositeTestFixture(t)
	store, err := manager.GetStorage(manager.DefaultDatabaseName())
	require.NoError(t, err)
	exec.SetSearchService(search.NewService(store, 2, nil))
	ctx := context.Background()
	const query = "CALL { USE comp.a CALL db.index.vector.embed('alpha') YIELD embedding RETURN embedding } RETURN embedding"
	exec.SetEmbedder(&stubVectorEmbedder{vec: []float32{1, 0}})
	first, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, []float32{1, 0}, toFloat32Slice(first.Rows[0][0]))
	exec.SetEmbedder(&stubVectorEmbedder{vec: []float32{0, 1}})
	second, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, []float32{0, 1}, toFloat32Slice(second.Rows[0][0]), "Fabric's outer cache must expire when its shared provider changes")
}

func TestQueryEmbeddingFabricCachedStartupFallback(t *testing.T) {
	exec, manager := compositeTestFixture(t)
	store, err := manager.GetStorage("shard_a")
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "doc", Labels: []string{"Doc"}, Properties: map[string]interface{}{"content": "alpha"}, ChunkEmbeddings: [][]float32{{1, 0}}})
	require.NoError(t, err)
	svc := search.NewService(store, 2, nil)
	require.NoError(t, svc.BuildIndexes(context.Background()))
	exec.SetSearchService(svc)
	const query = "CALL { USE comp.a CALL db.rretrieve({query: 'alpha', limit: 5}) YIELD fallback_reason RETURN fallback_reason } RETURN fallback_reason"
	first, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.NotEmpty(t, first.Rows)
	require.Equal(t, "no_embedder", first.Rows[0][0])
	exec.SetEmbedder(&stubVectorEmbedder{vec: []float32{1, 0}})
	second, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.NotEmpty(t, second.Rows)
	require.Equal(t, "", second.Rows[0][0], "the cached pre-ready fallback must expire at readiness")
}
