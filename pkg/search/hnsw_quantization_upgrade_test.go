package search

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// A graph saved before compressed traversal existed (float coordinates, no
// quantization keys in the file) is reused by a fresh service whose default is
// scalar int8. The restarted service loads the saved graph and state instead
// of rebuilding them, encodes the graph, keeps the exact results, and saves the
// scalar model for the next restart.
func TestHNSWScalarDefaultUpgradesSavedFloatGraph(t *testing.T) {
	t.Setenv("NORNICDB_GPU_BACKEND", "cpu")
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	t.Setenv("NORNICDB_VECTOR_HNSW_QUANTIZATION", "none")
	const dimensions, count = 16, 400
	ctx := context.Background()
	rng := rand.New(rand.NewSource(143))
	randomVector := func() []float32 {
		vec := make([]float32, dimensions)
		for d := range vec {
			vec[d] = float32(rng.NormFloat64())
		}
		return vec
	}
	engine := newNamespacedEngine(t)
	stored := make([][]float32, count)
	for i := range stored {
		stored[i] = randomVector()
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("doc-%03d", i)), Labels: []string{"Doc"},
			Properties: map[string]any{"text": fmt.Sprintf("document %d", i)}, NamedEmbeddings: map[string][]float32{"default": stored[i]}})
		require.NoError(t, err)
	}
	queries := make([][]float32, 8)
	for i := range queries {
		queries[i] = append([]float32(nil), stored[i*37]...)
		for d := range queries[i] {
			queries[i][d] += float32(rng.NormFloat64()) / 10
		}
	}

	dir := t.TempDir()
	hnswPath := filepath.Join(dir, "hnsw")
	open := func(engine storage.Engine) *Service {
		svc := NewServiceWithDimensions(engine, dimensions)
		svc.SetFulltextIndexPath(filepath.Join(dir, "bm25"))
		svc.SetVectorIndexPath(filepath.Join(dir, "vectors"))
		svc.SetHNSWIndexPath(hnswPath)
		svc.SetPersistenceEnabled(true)
		return svc
	}
	options := &SearchOptions{HNSWEf: 256, QuantizationOversampling: 256}
	results := func(svc *Service, exact bool) [][]VectorQueryHit {
		out := make([][]VectorQueryHit, len(queries))
		for i, query := range queries {
			hits, err := svc.VectorQueryNodesWithOptions(ctx, query, VectorQuerySpec{Limit: 10, Exact: exact}, options)
			require.NoError(t, err)
			require.NotEmpty(t, hits)
			out[i] = hits
		}
		return out
	}
	requireSameHits := func(want, got [][]VectorQueryHit) {
		t.Helper()
		require.Len(t, got, len(want))
		for i := range want {
			require.Len(t, got[i], len(want[i]), "query %d", i)
			for j := range want[i] {
				require.Equal(t, want[i][j].ID, got[i][j].ID, "query %d rank %d", i, j)
				require.InDelta(t, want[i][j].Score, got[i][j].Score, 1e-6, "query %d rank %d", i, j)
			}
		}
	}
	graphMode := func(svc *Service) (HNSWQuantization, int) {
		svc.hnswMu.RLock()
		idx := svc.hnswIndex
		svc.hnswMu.RUnlock()
		require.NotNil(t, idx)
		idx.mu.RLock()
		mode := idx.config.Quantization.Mode
		idx.mu.RUnlock()
		return mode, idx.QuantizationBytes()
	}

	floatService := open(engine)
	require.NoError(t, floatService.BuildIndexes(ctx))
	exact := results(floatService, true)
	requireSameHits(exact, results(floatService, false))
	mode, encoded := graphMode(floatService)
	require.Equal(t, HNSWQuantizationNone, mode)
	require.Zero(t, encoded)
	floatService.PersistIndexesToDisk()
	require.NoError(t, floatService.Close())
	removeQuantizationKeys(t, hnswPath)

	// Fresh services over saved state: storage iteration would mean a rebuild.
	saved := &iteratorEngine{Engine: engine, iterateErr: errors.New("saved indexes must not rebuild from storage")}
	hnswBeforeInstallHook = func(context.Context) { t.Error("the saved graph must be loaded, not rebuilt") }
	t.Cleanup(func() { hnswBeforeInstallHook = nil })
	t.Setenv("NORNICDB_VECTOR_HNSW_QUANTIZATION", "")
	for _, restart := range []string{"upgrade from float graph", "restart with scalar model"} {
		svc := open(saved)
		require.NoError(t, svc.BuildIndexes(ctx), restart)
		requireSameHits(exact, results(svc, false))
		mode, encoded := graphMode(svc)
		require.Equal(t, HNSWQuantizationScalar, mode, restart)
		require.Positive(t, encoded, restart)
		svc.PersistIndexesToDisk()
		require.NoError(t, svc.Close())
		snapshot := readGraphSnapshot(t, hnswPath)
		require.NotNil(t, snapshot.Quantization, restart)
		require.Equal(t, HNSWQuantizationScalar, snapshot.Quantization.Config.Mode, restart)
	}
}

// removeQuantizationKeys rewrites a saved graph in the format written before
// compressed traversal: the same msgpack map without the quantization keys.
func removeQuantizationKeys(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var snapshot map[string]any
	require.NoError(t, msgpack.Unmarshal(data, &snapshot))
	require.Contains(t, snapshot, "Quantization")
	require.Nil(t, snapshot["Quantization"], "a float graph has no quantization model")
	delete(snapshot, "Quantization")
	config, ok := snapshot["Config"].(map[string]any)
	require.True(t, ok)
	delete(config, "Quantization")
	data, err = msgpack.Marshal(snapshot)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o644))
	require.Nil(t, readGraphSnapshot(t, path).Quantization)
}

func readGraphSnapshot(t *testing.T, path string) hnswIndexSnapshot {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var snapshot hnswIndexSnapshot
	require.NoError(t, msgpack.Unmarshal(data, &snapshot))
	return snapshot
}
