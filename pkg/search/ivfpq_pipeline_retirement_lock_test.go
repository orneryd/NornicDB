package search

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestCompressedPipelineCreationRacesSourceRetirement: source retirement holds
// indexMu while it takes pipelineMu, so compressed pipeline creation must not
// read the node metadata (the IVF-PQ build's BM25 seeds) under pipelineMu.
// Creation is parked inside pipelineMu on ivfpqMu while a retirement takes
// indexMu and waits for pipelineMu; both must then finish.
func TestCompressedPipelineCreationRacesSourceRetirement(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_ANN_QUALITY", "compressed")
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 16)
	dir := t.TempDir()
	vfs, err := NewVectorFileStore(filepath.Join(dir, "vectors"), 16)
	require.NoError(t, err)
	t.Cleanup(func() { _ = vfs.Close() })
	for i := 0; i < 1500; i++ {
		vec := make([]float32, 16)
		vec[i%16] = 1
		require.NoError(t, vfs.Add(fmt.Sprintf("doc-%d", i), vec))
	}
	svc.mu.Lock()
	svc.vectorFileStore = vfs
	svc.vectorIndexPath = filepath.Join(dir, "vectors")
	svc.hnswIndexPath = filepath.Join(dir, "hnsw")
	svc.mu.Unlock()

	held := func(tryLock func() bool, unlock func()) bool {
		if tryLock() {
			unlock()
			return false
		}
		return true
	}
	svc.ivfpqMu.Lock()
	created := make(chan error, 1)
	go func() {
		_, err := svc.getOrCreateVectorPipeline(context.Background())
		created <- err
	}()
	require.Eventually(t, func() bool { return held(svc.pipelineMu.TryLock, svc.pipelineMu.Unlock) },
		10*time.Second, time.Millisecond, "creation never reached pipelineMu")
	retired := make(chan struct{})
	go func() {
		defer close(retired)
		svc.indexMu.Lock()
		svc.retireVectorSourceLocked(false)
		svc.indexMu.Unlock()
	}()
	require.Eventually(t, func() bool { return held(svc.indexMu.TryRLock, svc.indexMu.RUnlock) },
		10*time.Second, time.Millisecond, "retirement never took indexMu")
	svc.ivfpqMu.Unlock()

	deadline := time.After(30 * time.Second)
	select {
	case err := <-created:
		require.NoError(t, err)
	case <-deadline:
		t.Fatal("compressed pipeline creation and source retirement deadlocked")
	}
	select {
	case <-retired:
	case <-deadline:
		t.Fatal("source retirement never finished")
	}
}
