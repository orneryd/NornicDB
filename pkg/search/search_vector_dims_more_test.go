package search

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestService_MaybeAutoSetVectorDimensions_MoreBranches(t *testing.T) {
	// Nil and invalid-dimension guards are no-ops.
	var nilSvc *Service
	nilSvc.maybeAutoSetVectorDimensions(4)

	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 3)
	oldDims := svc.vectorIndex.GetDimensions()
	svc.maybeAutoSetVectorDimensions(0)
	require.Equal(t, oldDims, svc.vectorIndex.GetDimensions())

	// Prepare dependent indexes that should be invalidated on dimension change.
	embCfg := gpu.DefaultEmbeddingIndexConfig(3)
	embCfg.GPUEnabled = false
	embCfg.AutoSync = false
	svc.gpuEmbeddingIndex = gpu.NewEmbeddingIndex(nil, embCfg)
	clusterIdx := gpu.NewClusterIndex(nil, embCfg, gpu.DefaultKMeansConfig())
	svc.clusterIndex = clusterIdx
	svc.clusterHNSW = map[int]*HNSWIndex{0: NewHNSWIndex(3, DefaultHNSWConfig())}
	svc.ivfpqIndex = &IVFPQIndex{profile: IVFPQProfile{Dimensions: 3}}
	svc.vectorPipeline = NewVectorSearchPipeline(NewBruteForceCandidateGen(svc.vectorIndex), NewCPUExactScorer(svc.vectorIndex))

	svc.maybeAutoSetVectorDimensions(5)
	require.Equal(t, 5, svc.vectorIndex.GetDimensions())
	require.Nil(t, svc.vectorPipeline)
	require.Nil(t, svc.gpuEmbeddingIndex)
	require.NotNil(t, svc.clusterIndex)
	require.Nil(t, svc.clusterHNSW)
	require.Nil(t, svc.ivfpqIndex)

	// Once vectors exist, dimensions must not auto-change.
	require.NoError(t, svc.vectorIndex.Add("keep", []float32{1, 0, 0, 0, 0}))
	svc.ivfpqIndex = &IVFPQIndex{profile: IVFPQProfile{Dimensions: 5}}
	svc.maybeAutoSetVectorDimensions(7)
	require.Equal(t, 5, svc.vectorIndex.GetDimensions())
	require.NotNil(t, svc.ivfpqIndex)
}

func TestService_GetVectorForCypher_MoreBranches(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 3)

	// vectorIndex path returns a copy, not alias to raw backing storage.
	require.NoError(t, svc.vectorIndex.Add("idx", []float32{1, 0, 0}))
	v, ok := svc.getVectorForCypher("idx")
	require.True(t, ok)
	require.InDeltaSlice(t, []float32{1, 0, 0}, v, 1e-6) // SIMD normalization may differ by 1 ulp (#595)
	v[0] = 9
	v2, ok := svc.getVectorForCypher("idx")
	require.True(t, ok)
	require.InDelta(t, 1.0, v2[0], 1e-6)

	// vector file store takes precedence when configured.
	vfs, err := NewVectorFileStore(t.TempDir()+"/vectors", 3)
	require.NoError(t, err)
	defer func() { _ = vfs.Close() }()
	require.NoError(t, vfs.Add("vfs", []float32{0, 1, 0}))
	svc.vectorFileStore = vfs
	v3, ok := svc.getVectorForCypher("vfs")
	require.True(t, ok)
	require.Len(t, v3, 3)
	require.InDelta(t, 0.0, v3[0], 1e-5)
	require.InDelta(t, 1.0, v3[1], 1e-5)
	require.InDelta(t, 0.0, v3[2], 1e-5)

	// Missing id returns false.
	_, ok = svc.getVectorForCypher("missing")
	require.False(t, ok)
}

func TestService_VectorQueryNodes_FileStorePropertyVectors(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")

	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 4)
	vfs, err := NewVectorFileStore(t.TempDir()+"/vectors", 4)
	require.NoError(t, err)
	defer func() { _ = vfs.Close() }()
	svc.vectorFileStore = vfs

	require.NoError(t, svc.IndexNode(&storage.Node{
		ID:     "best",
		Labels: []string{"Entity"},
		Properties: map[string]any{
			"name":           "FalkorDB",
			"name_embedding": []float64{1, 0, 0, 0},
		},
	}))
	require.NoError(t, svc.IndexNode(&storage.Node{
		ID:     "other",
		Labels: []string{"Entity"},
		Properties: map[string]any{
			"name":           "NornicDB",
			"name_embedding": []float64{0, 1, 0, 0},
		},
	}))
	require.Equal(t, 2, svc.CountPropertyVectorEntries("name_embedding"))
	require.Equal(t, 2, svc.EmbeddingCount())
	require.Equal(t, 0, svc.vectorIndex.Count(), "regression requires vectors to live only in the file store")

	hits, err := svc.VectorQueryNodes(context.Background(), []float32{1, 0, 0, 0}, VectorQuerySpec{
		Label:    "Entity",
		Property: "name_embedding",
		Limit:    1,
	})
	require.NoError(t, err)
	require.Len(t, hits, 1)
	require.Equal(t, "best", hits[0].ID)
	require.InDelta(t, 1.0, hits[0].Score, 1e-5)
}

func TestService_VectorQueryNodes_FileStorePropertyVectorsSupplementsStaleHNSW(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 4)
	vfs, err := NewVectorFileStore(t.TempDir()+"/vectors", 4)
	require.NoError(t, err)
	defer func() { _ = vfs.Close() }()
	svc.vectorFileStore = vfs

	require.NoError(t, svc.IndexNode(&storage.Node{
		ID:     "joe-existing",
		Labels: []string{"Entity"},
		Properties: map[string]any{
			"name":           "Joe",
			"name_embedding": []float64{1, 0, 0, 0},
		},
	}))
	require.NoError(t, svc.IndexNode(&storage.Node{
		ID:     "claude-existing",
		Labels: []string{"Entity"},
		Properties: map[string]any{
			"name":           "Claude",
			"name_embedding": []float64{0, 1, 0, 0},
		},
	}))
	require.Equal(t, 2, svc.CountPropertyVectorEntries("name_embedding"))
	require.Equal(t, 2, svc.EmbeddingCount())

	// Simulate a candidate-generation visibility/recall miss: the file-backed
	// vector store can score Joe exactly, but the active HNSW pipeline only
	// returns Claude. This mirrors high-frequency entities being present in the
	// file store while candidate generation misses them during ingest.
	staleHNSW := NewHNSWIndex(4, DefaultHNSWConfig())
	require.NoError(t, staleHNSW.Add("claude-existing-prop-name_embedding", []float32{0, 1, 0, 0}))
	svc.vectorPipeline = NewVectorSearchPipeline(NewHNSWCandidateGen(staleHNSW), NewCPUExactScorer(vfs))
	svc.hnswIndex = staleHNSW

	hits, err := svc.VectorQueryNodes(context.Background(), []float32{1, 0, 0, 0}, VectorQuerySpec{
		Label:    "Entity",
		Property: "name_embedding",
		Limit:    1,
	})
	require.NoError(t, err)
	require.Len(t, hits, 1)
	require.Equal(t, "joe-existing", hits[0].ID)
	require.InDelta(t, 1.0, hits[0].Score, 1e-5)
}

func TestService_MinSimilarityAndCountDimensionFallbacks(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 3)

	// resolveMinSimilarity precedence: explicit option > service default > fallback.
	explicit := 0.77
	require.Equal(t, explicit, *svc.resolveMinSimilarity(&SearchOptions{MinSimilarity: &explicit}))

	svc.SetDefaultMinSimilarity(0.33)
	require.Equal(t, 0.33, *svc.resolveMinSimilarity(&SearchOptions{}))

	svc.SetDefaultMinSimilarity(-1)
	require.Equal(t, 0.5, *svc.resolveMinSimilarity(nil))

	// File-store-backed count/dimension branches.
	vfs, err := NewVectorFileStore(t.TempDir()+"/vectors", 4)
	require.NoError(t, err)
	defer func() { _ = vfs.Close() }()
	require.NoError(t, vfs.Add("nornic:v1", []float32{1, 0, 0, 0}))
	svc.vectorFileStore = vfs

	require.Equal(t, 1, svc.EmbeddingCount())
	require.Equal(t, 4, svc.VectorIndexDimensions())

	// Empty file store should fall back to in-memory index for count/dimensions.
	svcFallback := NewServiceWithDimensions(storage.NewMemoryEngine(), 3)
	emptyVFS, err := NewVectorFileStore(t.TempDir()+"/vectors-empty", 7)
	require.NoError(t, err)
	defer func() { _ = emptyVFS.Close() }()
	svcFallback.vectorFileStore = emptyVFS
	require.Equal(t, 0, svcFallback.EmbeddingCount())
	require.Equal(t, 3, svcFallback.VectorIndexDimensions())

	// No vector backends => zero count/dimensions fallback branches.
	svcNoIndex := NewService(storage.NewMemoryEngine())
	svcNoIndex.vectorIndex = nil
	require.Equal(t, 0, svcNoIndex.EmbeddingCount())
	require.Equal(t, 0, svcNoIndex.VectorIndexDimensions())
}

func TestService_ClearVectorIndex_ClearsAllBackendsAndMetadata(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 3)

	vfs, err := NewVectorFileStore(t.TempDir()+"/vectors-clear", 3)
	require.NoError(t, err)
	require.NoError(t, vfs.Add("nornic:n1", []float32{1, 0, 0}))
	svc.vectorFileStore = vfs

	embCfg := gpu.DefaultEmbeddingIndexConfig(3)
	embCfg.GPUEnabled = false
	embCfg.AutoSync = false
	svc.gpuEmbeddingIndex = gpu.NewEmbeddingIndex(nil, embCfg)
	svc.clusterIndex = gpu.NewClusterIndex(nil, embCfg, gpu.DefaultKMeansConfig())
	svc.clusterHNSW = map[int]*HNSWIndex{0: NewHNSWIndex(3, DefaultHNSWConfig())}
	svc.ivfpqIndex = &IVFPQIndex{profile: IVFPQProfile{Dimensions: 3}}
	svc.hnswIndex = NewHNSWIndex(3, DefaultHNSWConfig())
	svc.vectorPipeline = NewVectorSearchPipeline(NewBruteForceCandidateGen(svc.vectorIndex), NewCPUExactScorer(svc.vectorIndex))

	svc.nodeLabels["nornic:n1"] = []string{"Doc"}
	svc.nodeNamedVector["nornic:n1"] = map[string]string{"default": "nornic:n1-named-default"}
	svc.nodePropVector["nornic:n1"] = map[string]string{"vec": "nornic:n1-prop-vec"}
	svc.nodeChunkVectors["nornic:n1"] = []string{"nornic:n1-chunk-0"}

	svc.ClearVectorIndex()

	require.Nil(t, svc.vectorPipeline)
	require.Nil(t, svc.vectorFileStore)
	require.Nil(t, svc.clusterHNSW)
	require.Nil(t, svc.ivfpqIndex)
	require.Nil(t, svc.hnswIndex)
	require.Empty(t, svc.nodeLabels)
	require.Empty(t, svc.nodeNamedVector)
	require.Empty(t, svc.nodePropVector)
	require.Empty(t, svc.nodeChunkVectors)
}
