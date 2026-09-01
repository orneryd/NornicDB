package search

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestResolveCompressedVectorStrategy_FallsBackWhenNotReady(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_ANN_QUALITY", "compressed")
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 16)

	strategy, err := svc.resolveVectorStrategy(context.Background(), 8000, 16, nil)
	require.NoError(t, err)
	require.Contains(t, strategy.name, "compressed-disabled")
}

func TestResolveCompressedVectorStrategy_UsesIVFPQWhenReady(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_ANN_QUALITY", "compressed")
	t.Setenv("NORNICDB_VECTOR_IVF_LISTS", "16")
	t.Setenv("NORNICDB_VECTOR_PQ_SEGMENTS", "4")
	t.Setenv("NORNICDB_VECTOR_PQ_BITS", "4")
	t.Setenv("NORNICDB_VECTOR_IVFPQ_NPROBE", "4")

	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 16)
	dir := t.TempDir()
	vfs, err := NewVectorFileStore(fmt.Sprintf("%s/vectors", dir), 16)
	require.NoError(t, err)
	defer vfs.Close()

	for i := 0; i < 1500; i++ {
		vec := make([]float32, 16)
		vec[i%16] = 1
		require.NoError(t, vfs.Add(fmt.Sprintf("doc-%d", i), vec))
	}
	svc.mu.Lock()
	svc.vectorFileStore = vfs
	svc.vectorIndexPath = fmt.Sprintf("%s/vectors", dir)
	svc.hnswIndexPath = fmt.Sprintf("%s/hnsw", dir)
	svc.mu.Unlock()

	strategy, err := svc.resolveVectorStrategy(context.Background(), 1500, 16, vfs)
	require.NoError(t, err)
	require.IsType(t, &IVFPQCandidateGen{}, strategy.candidateGen)
}

func TestResolveCompressedVectorStrategy_FallbackOnLoadError(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_ANN_QUALITY", "compressed")

	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 16)
	dir := t.TempDir()
	vfs, err := NewVectorFileStore(fmt.Sprintf("%s/vectors", dir), 16)
	require.NoError(t, err)
	defer vfs.Close()
	require.NoError(t, vfs.Add("doc-1", []float32{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}))

	// Corrupt persisted IVFPQ meta to force load error and exercise compressed-fallback path.
	badBundleDir := filepath.Join(dir, "ivfpq")
	require.NoError(t, os.MkdirAll(badBundleDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(badBundleDir, "meta"), []byte("not-msgpack"), 0o644))

	svc.mu.Lock()
	svc.vectorFileStore = vfs
	svc.vectorIndexPath = fmt.Sprintf("%s/vectors", dir)
	svc.hnswIndexPath = fmt.Sprintf("%s/hnsw", dir)
	svc.mu.Unlock()

	strategy, err := svc.resolveCompressedVectorStrategy(context.Background(), 1500, 16, vfs)
	require.NoError(t, err)
	require.Contains(t, strategy.name, "compressed-fallback")
}
