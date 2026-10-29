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

// Both cases run one composite statement twice on one executor and require the
// second execution's row to differ from the first's, so they share one contract:
// the outer Fabric result cache must not serve the row the pre-change
// execution wrote. They assert different rows and reach that cache through
// different keys, so each case owns rows the other does not check.
//
//   - provider replacement: the statement caches its own embedding result, and
//     its key carries the embedder publication generation, so replacing the
//     provider already keeps the second execution out of the first's entry.
//   - cached startup fallback: the statement caches db.rretrieve's page, whose
//     key carries the search service instance and index revision and no
//     embedder generation. Expiry here rests on the first, embedder-less search
//     building the shard_a index (advancing its revision) before the ready,
//     embedder-aware search runs without the fallback reason.
func TestQueryEmbeddingFabricCacheAcrossProviderChange(t *testing.T) {
	const (
		embeddingQuery = "CALL { USE comp.a CALL db.index.vector.embed('alpha') YIELD embedding RETURN embedding } RETURN embedding"
		retrievalQuery = "CALL { USE comp.a CALL db.rretrieve({query: 'alpha', limit: 5}) YIELD fallback_reason RETURN fallback_reason } RETURN fallback_reason"
	)
	for _, testCase := range []struct {
		name       string
		query      string
		database   string
		nextVector []float32
		// prepare wires the statement's search service and, for a query that
		// needs one before its first execution, publishes the pre-change
		// provider.
		prepare func(t *testing.T, exec *StorageExecutor, store storage.Engine)
		// requireRow checks the row of the execution that follows the provider
		// replacement.
		requireRow func(t *testing.T, row interface{})
	}{
		{
			name:       "provider replacement",
			query:      embeddingQuery,
			nextVector: []float32{0, 1},
			prepare: func(t *testing.T, exec *StorageExecutor, store storage.Engine) {
				exec.SetSearchService(search.NewService(store, 2, nil))
				exec.SetEmbedder(&stubVectorEmbedder{vec: []float32{1, 0}})
				first, err := exec.Execute(context.Background(), embeddingQuery, nil)
				require.NoError(t, err)
				require.Equal(t, []float32{1, 0}, toFloat32Slice(first.Rows[0][0]))
			},
			requireRow: func(t *testing.T, row interface{}) {
				require.Equal(t, []float32{0, 1}, toFloat32Slice(row),
					"Fabric's outer cache must expire when its shared provider changes")
			},
		},
		{
			name:       "cached startup fallback",
			query:      retrievalQuery,
			database:   "shard_a",
			nextVector: []float32{1, 0},
			prepare: func(t *testing.T, exec *StorageExecutor, store storage.Engine) {
				_, err := store.CreateNode(&storage.Node{ID: "doc", Labels: []string{"Doc"}, Properties: map[string]interface{}{"content": "alpha"}, ChunkEmbeddings: [][]float32{{1, 0}}})
				require.NoError(t, err)
				svc := search.NewService(store, 2, nil)
				require.NoError(t, svc.BuildIndexes(context.Background()))
				exec.SetSearchService(svc)
				first, err := exec.Execute(context.Background(), retrievalQuery, nil)
				require.NoError(t, err)
				require.NotEmpty(t, first.Rows)
				require.Equal(t, "no_embedder", first.Rows[0][0])
			},
			requireRow: func(t *testing.T, row interface{}) {
				require.Equal(t, "", row, "the cached pre-ready fallback must expire at readiness")
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			exec, manager := compositeTestFixture(t)
			database := testCase.database
			if database == "" {
				database = manager.DefaultDatabaseName()
			}
			store, err := manager.GetStorage(database)
			require.NoError(t, err)
			testCase.prepare(t, exec, store)

			exec.SetEmbedder(&stubVectorEmbedder{vec: testCase.nextVector})
			second, err := exec.Execute(context.Background(), testCase.query, nil)
			require.NoError(t, err)
			require.NotEmpty(t, second.Rows)
			testCase.requireRow(t, second.Rows[0][0])
		})
	}
}
