package search

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestServiceOptionsSearchResultCachePolicy(t *testing.T) {
	defaultService := NewServiceWithDimensionsAndBM25Engine(storage.NewMemoryEngine(), 3, "v2")
	require.NotNil(t, defaultService.resultCache)
	require.Equal(t, 1000, defaultService.resultCache.maxSize)
	require.Equal(t, 5*time.Minute, defaultService.resultCache.ttl)

	disabled := NewServiceWithDimensionsAndBM25EngineAndOptions(storage.NewMemoryEngine(), 3, "v2", &ServiceOptions{
		DatabaseID:               "alpha",
		SearchResultCacheEntries: 0,
		SearchResultCacheTTL:     time.Minute,
	})
	require.NotNil(t, disabled.resultCache)
	require.Equal(t, 0, disabled.resultCache.maxSize)

	configured := NewServiceWithDimensionsAndBM25EngineAndOptions(storage.NewMemoryEngine(), 3, "v2", &ServiceOptions{
		DatabaseID:               "beta",
		SearchResultCacheEntries: 12,
		SearchResultCacheTTL:     2 * time.Minute,
	})
	require.Equal(t, 12, configured.resultCache.maxSize)
	require.Equal(t, 2*time.Minute, configured.resultCache.ttl)
}

func TestServiceOptionsIndexCapacityPolicy(t *testing.T) {
	service := NewServiceWithDimensionsAndBM25EngineAndOptions(storage.NewMemoryEngine(), 3, "v2", &ServiceOptions{
		DatabaseID:             "capacity",
		BM25MemoryMaxBytes:     1 << 20,
		VectorMemoryMaxBytes:   2 << 20,
		MetadataMemoryMaxBytes: 3 << 20,
		BM25StorageMode:        "memory",
		VectorStorageMode:      "disk",
	})

	bm25Max, vectorMax, metadataMax, bm25Storage, vectorStorage := service.IndexCapacityPolicy()
	require.Equal(t, int64(1<<20), bm25Max)
	require.Equal(t, int64(2<<20), vectorMax)
	require.Equal(t, int64(3<<20), metadataMax)
	require.Equal(t, "memory", bm25Storage)
	require.Equal(t, "disk", vectorStorage)
}

func TestVectorStorageModeControlsFileStore(t *testing.T) {
	for _, test := range []struct {
		mode          string
		wantFileStore bool
	}{
		{mode: "memory", wantFileStore: false},
		{mode: "disk", wantFileStore: true},
	} {
		t.Run(test.mode, func(t *testing.T) {
			service := NewServiceWithDimensionsAndBM25EngineAndOptions(storage.NewMemoryEngine(), 3, "v2", &ServiceOptions{VectorStorageMode: test.mode})
			service.SetPersistenceEnabled(true)
			service.SetVectorIndexPath(t.TempDir() + "/vectors")

			service.indexMu.Lock()
			service.ensureBuildVectorFileStore()
			service.indexMu.Unlock()

			require.Equal(t, test.wantFileStore, service.vectorFileStore != nil)
			require.NoError(t, service.Close())
		})
	}
}

func TestIndexCapacityBudgetsRejectBeforeMutation(t *testing.T) {
	tests := []struct {
		name    string
		options ServiceOptions
		node    *storage.Node
	}{
		{
			name:    "bm25 payload",
			options: ServiceOptions{BM25MemoryMaxBytes: 4},
			node:    &storage.Node{ID: "document", Properties: map[string]any{"text": "larger than four bytes"}},
		},
		{
			name:    "vector payload",
			options: ServiceOptions{VectorMemoryMaxBytes: 16, VectorStorageMode: "memory"},
			node:    &storage.Node{ID: "vector", ChunkEmbeddings: [][]float32{{1, 2, 3}}},
		},
		{
			name:    "index metadata",
			options: ServiceOptions{MetadataMemoryMaxBytes: 1},
			node:    &storage.Node{ID: "metadata", Labels: []string{"Document"}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := NewServiceWithDimensionsAndBM25EngineAndOptions(storage.NewMemoryEngine(), 3, "v2", &test.options)
			err := service.IndexNode(test.node)
			require.Error(t, err)
			require.True(t, errors.Is(err, ErrIndexMemoryBudgetExceeded))
			require.Equal(t, 0, service.fulltextIndex.Count())
			require.Equal(t, 0, service.vectorIndex.Count())
		})
	}
}

func TestBuildIndexesEnforcesCapacityBudgets(t *testing.T) {
	engine := storage.NewMemoryEngine()
	_, err := engine.CreateNode(&storage.Node{ID: "nornic:document", Properties: map[string]any{"text": "larger than four bytes"}})
	require.NoError(t, err)
	service := NewServiceWithDimensionsAndBM25EngineAndOptions(engine, 3, "v2", &ServiceOptions{BM25MemoryMaxBytes: 4})

	err = service.BuildIndexes(context.Background())
	require.ErrorIs(t, err, ErrIndexMemoryBudgetExceeded)
}

func TestDiskVectorStorageRequiresPersistencePath(t *testing.T) {
	service := NewServiceWithDimensionsAndBM25EngineAndOptions(storage.NewMemoryEngine(), 3, "v2", &ServiceOptions{VectorStorageMode: "disk"})

	err := service.BuildIndexes(context.Background())
	require.ErrorContains(t, err, "vector disk storage requires")

	disabled := NewServiceWithDimensionsAndBM25EngineAndOptions(storage.NewMemoryEngine(), 3, "v2", &ServiceOptions{VectorStorageMode: "disk"})
	disabled.SetIndexFlags(true, false)
	require.NoError(t, disabled.BuildIndexes(context.Background()))
}

func TestIndexCapacityBudgetsCoverRelationshipVectors(t *testing.T) {
	service := NewServiceWithDimensionsAndBM25EngineAndOptions(storage.NewMemoryEngine(), 3, "v2", &ServiceOptions{VectorMemoryMaxBytes: 8})

	err := service.IndexEdge(&storage.Edge{
		ID:         "relationship",
		Type:       "RELATED_TO",
		Properties: map[string]any{"embedding": []float32{1, 2, 3}},
	})
	require.ErrorIs(t, err, ErrIndexMemoryBudgetExceeded)
	require.Empty(t, service.edgePropVector)
}

func TestSearchResultCacheResizeAndTTL(t *testing.T) {
	cache := newSearchResultCache(3, time.Hour)
	cache.Put("a", &SearchResponse{Query: "a"})
	cache.Put("b", &SearchResponse{Query: "b"})
	cache.Put("c", &SearchResponse{Query: "c"})
	require.NotNil(t, cache.Get("a"))

	cache.Resize(2)
	require.Nil(t, cache.Get("b"))
	require.NotNil(t, cache.Get("a"))
	require.NotNil(t, cache.Get("c"))

	cache.Resize(4)
	require.NotNil(t, cache.Get("a"))
	cache.Resize(0)
	require.Nil(t, cache.Get("a"))
	cache.Put("disabled", &SearchResponse{Query: "disabled"})
	require.Nil(t, cache.Get("disabled"))

	cache.Resize(2)
	cache.Put("old", &SearchResponse{Query: "old"})
	time.Sleep(5 * time.Millisecond)
	cache.SetTTL(time.Millisecond)
	require.Nil(t, cache.Get("old"))
}
