package search

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func writeHNSWSnapshot(t *testing.T, path string, snap *hnswIndexSnapshot) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	f, err := os.Create(path)
	require.NoError(t, err)
	require.NoError(t, msgpack.NewEncoder(f).Encode(snap))
	require.NoError(t, f.Close())
}

func TestHNSWIndex_AddPoolsAndSearchLargeDimBranches(t *testing.T) {
	cfg := DefaultHNSWConfig()
	idx := NewHNSWIndex(300, cfg)

	// Exercise pool New closures initialized in NewHNSWIndex.
	require.Len(t, idx.queryBufPool.Get().([]float32), 300)
	require.NotNil(t, idx.visitedPool.Get())
	require.NotNil(t, idx.heapPool.Get())
	require.NotNil(t, idx.idsPool.Get())
	require.NotNil(t, idx.itemsPool.Get())

	// Empty ID is ignored.
	require.NoError(t, idx.Add("", make([]float32, 300)))
	idx.mu.RLock()
	require.Len(t, idx.idToInternal, 0)
	idx.mu.RUnlock()

	// m<=0 guard in Add returns nil and skips insertion.
	idx.mu.Lock()
	idx.config.M = 0
	idx.mu.Unlock()
	require.NoError(t, idx.Add("m0", make([]float32, 300)))
	idx.mu.RLock()
	require.Len(t, idx.idToInternal, 0)
	idx.mu.RUnlock()

	idx2 := NewHNSWIndex(300, DefaultHNSWConfig())
	v1 := make([]float32, 300)
	v2 := make([]float32, 300)
	v1[0] = 1
	v2[1] = 1
	require.NoError(t, idx2.Add("a", v1))
	require.NoError(t, idx2.Add("b", v2))

	// ef<=0 path and >256 dimensions path in searchWithEf.
	res, err := idx2.SearchWithEf(context.Background(), v1, 2, -1, 0)
	require.NoError(t, err)
	require.NotEmpty(t, res)

	// out-of-range internalToID guard in result projection.
	idx2.mu.Lock()
	idx2.internalToID = []string{}
	idx2.mu.Unlock()
	res, err = idx2.SearchWithEf(context.Background(), v1, 5, -1, 8)
	require.NoError(t, err)
	require.Empty(t, res)
}

func TestHNSWIndex_SaveLoadAndIVFMoreErrorBranches(t *testing.T) {
	t.Run("save_mkdir_error", func(t *testing.T) {
		tmp := t.TempDir()
		blocked := filepath.Join(tmp, "blocked")
		require.NoError(t, os.WriteFile(blocked, []byte("x"), 0o644))

		idx := NewHNSWIndex(2, DefaultHNSWConfig())
		require.NoError(t, idx.Add("a", []float32{1, 0}))
		err := idx.Save(filepath.Join(blocked, "hnsw"))
		require.Error(t, err)
	})

	t.Run("load_snapshot_guard_rejections", func(t *testing.T) {
		tmp := t.TempDir()
		base := filepath.Join(tmp, "idx")

		cases := []hnswIndexSnapshot{
			{Version: "1.0.0", Dimensions: 0, InternalToID: []string{"a"}},
			{Version: "1.0.0", Dimensions: 2, InternalToID: nil},
			{Version: "0.0.1", Dimensions: 2, InternalToID: []string{"a"}},
			{Version: hnswIndexFormatVersionGraphOnly, Dimensions: 2, InternalToID: []string{"a"}},
		}
		for i, snap := range cases {
			p := filepath.Join(base, "case", string(rune('a'+i)))
			writeHNSWSnapshot(t, p, &snap)
			h, err := LoadHNSWIndex(p, nil)
			require.NoError(t, err)
			require.Nil(t, h)
		}
	})

	t.Run("save_ivf_nil_index_and_load_cluster_empty_args", func(t *testing.T) {
		hnswPath := filepath.Join(t.TempDir(), "hnsw")
		require.NoError(t, SaveIVFHNSW(hnswPath, map[int]*HNSWIndex{0: nil}))
		h, err := LoadIVFHNSWCluster("", 0, func(string) ([]float32, bool) { return nil, false })
		require.NoError(t, err)
		require.Nil(t, h)
	})
}

func TestDeriveIVFCentroidsFromClusters_MoreBranchCoverage(t *testing.T) {
	t.Run("nonnumeric_files_only", func(t *testing.T) {
		tmp := t.TempDir()
		hnswPath := filepath.Join(tmp, "hnsw")
		ivfDir := filepath.Join(tmp, "hnsw_ivf")
		require.NoError(t, os.MkdirAll(ivfDir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(ivfDir, "centroids.gob"), []byte("x"), 0o644))
		centroids, idToCluster, err := DeriveIVFCentroidsFromClusters(hnswPath, func(string) ([]float32, bool) { return []float32{1, 0}, true })
		require.NoError(t, err)
		require.Nil(t, centroids)
		require.Nil(t, idToCluster)
	})

	t.Run("cluster_members_but_lookup_missing_vectors", func(t *testing.T) {
		tmp := t.TempDir()
		hnswPath := filepath.Join(tmp, "hnsw")
		ivfDir := filepath.Join(tmp, "hnsw_ivf")
		require.NoError(t, os.MkdirAll(ivfDir, 0o755))
		writeHNSWSnapshot(t, filepath.Join(ivfDir, "g1-0"), &hnswIndexSnapshot{Version: hnswIndexFormatVersionGraphOnly, Dimensions: 2, InternalToID: []string{"n1", "n2"}})
		require.NoError(t, os.WriteFile(filepath.Join(ivfDir, ivfHNSWManifestName), []byte(`{"seq":1,"dimensions":2,"files":["g1-0",""]}`), 0o644))
		centroids, idToCluster, err := DeriveIVFCentroidsFromClusters(hnswPath, func(string) ([]float32, bool) { return nil, false })
		require.NoError(t, err)
		require.Nil(t, centroids)
		require.Nil(t, idToCluster)
	})
}
