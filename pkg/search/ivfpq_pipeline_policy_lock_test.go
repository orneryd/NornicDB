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

// TestCompressedPipelineCreationRacesPolicyChange: a compression policy change
// holds indexMu while it takes pipelineMu, so compressed pipeline creation
// must not read the node metadata (the IVF-PQ build's BM25 seeds) under
// pipelineMu. Creation is parked inside pipelineMu on ivfpqMu while a policy
// change takes indexMu and waits for pipelineMu; both must then finish.
func TestCompressedPipelineCreationRacesPolicyChange(t *testing.T) {
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
	configured := make(chan error, 1)
	go func() {
		configured <- svc.ConfigureHNSWQuantization(context.Background(), HNSWQuantizationConfig{Mode: HNSWQuantizationBinary})
	}()
	require.Eventually(t, func() bool { return held(svc.indexMu.TryRLock, svc.indexMu.RUnlock) },
		10*time.Second, time.Millisecond, "the policy change never took indexMu")
	svc.ivfpqMu.Unlock()

	deadline := time.After(30 * time.Second)
	select {
	case err := <-created:
		require.NoError(t, err)
	case <-deadline:
		t.Fatal("compressed pipeline creation and the policy change deadlocked")
	}
	select {
	case err := <-configured:
		require.NoError(t, err)
	case <-deadline:
		t.Fatal("the policy change never finished")
	}
}
