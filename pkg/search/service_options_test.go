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

func TestSearchResultCachePolicyAppliesToLiveService(t *testing.T) {
	var nilService *Service
	nilService.SetSearchResultCachePolicy(10, time.Minute)
	maxEntries, ttl := nilService.SearchResultCachePolicy()
	require.Zero(t, maxEntries)
	require.Zero(t, ttl)

	service := NewServiceWithDimensionsAndBM25Engine(storage.NewMemoryEngine(), 3, "v2")
	service.resultCache.Put("first", &SearchResponse{Query: "first"})
	service.resultCache.Put("second", &SearchResponse{Query: "second"})

	service.SetSearchResultCachePolicy(1, 30*time.Second)
	maxEntries, ttl = service.SearchResultCachePolicy()
	require.Equal(t, 1, maxEntries)
	require.Equal(t, 30*time.Second, ttl)
	service.resultCache.mu.Lock()
	require.Len(t, service.resultCache.entries, 1)
	service.resultCache.mu.Unlock()

	service.SetSearchResultCachePolicy(0, time.Minute)
	maxEntries, ttl = service.SearchResultCachePolicy()
	require.Zero(t, maxEntries)
	require.Equal(t, time.Minute, ttl)
	service.resultCache.mu.Lock()
	require.Empty(t, service.resultCache.entries)
	service.resultCache.mu.Unlock()
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

func TestIndexCapacityAccountingIsReleasedOnRemoval(t *testing.T) {
	service := NewServiceWithDimensionsAndBM25EngineAndOptions(storage.NewMemoryEngine(), 3, "v2", &ServiceOptions{
		BM25StorageMode:        "memory",
		VectorStorageMode:      "memory",
		VectorMemoryMaxBytes:   1 << 20,
		MetadataMemoryMaxBytes: 1 << 20,
	})
	service.SetIndexFlags(false, true)

	node := &storage.Node{
		ID:              "node",
		NamedEmbeddings: map[string][]float32{"default": {1, 2, 3}},
	}
	require.NoError(t, service.IndexNode(node))
	require.Equal(t, int64(24), service.vectorResidentBytes)
	require.Equal(t, int64(54), service.vectorMetadataBytes)
	require.Contains(t, service.indexCapacityByNode, "node")

	require.NoError(t, service.RemoveNode(node.ID))
	require.Zero(t, service.vectorResidentBytes)
	require.Zero(t, service.vectorMetadataBytes)
	require.NotContains(t, service.indexCapacityByNode, "node")
	require.NoError(t, service.RemoveNode(node.ID), "repeated removal must remain idempotent")

	edge := &storage.Edge{
		ID:         "edge",
		Type:       "RELATED",
		Properties: map[string]any{"embedding": []float32{1, 2, 3}},
	}
	require.NoError(t, service.IndexEdge(edge))
	require.Equal(t, int64(12), service.vectorResidentBytes)
	require.Equal(t, int64(52), service.vectorMetadataBytes)
	require.Contains(t, service.indexCapacityByEdge, "edge")
	require.True(t, service.HasRelationshipVectorEntries("RELATED", "embedding"))

	require.NoError(t, service.RemoveEdge(edge.ID))
	require.Zero(t, service.vectorResidentBytes)
	require.Zero(t, service.vectorMetadataBytes)
	require.NotContains(t, service.indexCapacityByEdge, "edge")
	require.False(t, service.HasRelationshipVectorEntries("RELATED", "embedding"))
	require.NoError(t, service.RemoveEdge(edge.ID), "repeated removal must remain idempotent")
	require.NoError(t, (*Service)(nil).RemoveEdge(edge.ID))
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

func TestCollapseCandidatesByNodeIDKeepsHighestScore(t *testing.T) {
	require.Nil(t, collapseCandidatesByNodeID(nil))

	collapsed := collapseCandidatesByNodeID([]SearchCandidate{
		{ID: "node-a-chunk-0", Score: 0.4},
		{ID: "node-b-prop-embedding", Score: 0.8},
		{ID: "node-a-named-summary", Score: 0.9},
		{ID: "node-c", Score: 0.6},
		{ID: "node-b-chunk-2", Score: 0.7},
	})

	require.Equal(t, []SearchCandidate{
		{ID: "node-a", Score: 0.9},
		{ID: "node-b", Score: 0.8},
		{ID: "node-c", Score: 0.6},
	}, collapsed)
}
