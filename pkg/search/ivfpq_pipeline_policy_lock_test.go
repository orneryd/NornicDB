package search

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestCompressedPipelineCreationRacesPolicyChange: a compression policy change
// takes pipelineMu before indexMu. Creation is parked inside pipelineMu on
// ivfpqMu while a policy change queues on pipelineMu; it must leave admission
// free while waiting. Both operations must then finish.
func TestCompressedPipelineCreationRacesPolicyChange(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_ANN_QUALITY", "compressed")
	svc := NewServiceWithDimensions(newTestMemoryEngine(t), 16)
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

	svc.ivfpqMu.Lock()
	created := make(chan error, 1)
	go func() {
		_, err := svc.getOrCreateVectorPipeline(context.Background())
		created <- err
	}()
	serviceLockWait(t, "pipeline construction waiting for the compressed model", func() bool {
		return serviceLockStack("getOrBuildIVFPQIndex", "getOrCreateVectorPipeline")
	})
	configured := make(chan error, 1)
	go func() {
		configured <- svc.ConfigureHNSWQuantization(context.Background(), HNSWQuantizationConfig{Mode: HNSWQuantizationBinary})
	}()
	serviceLockWait(t, "policy change waiting for pipelineMu", func() bool {
		return serviceLockStack("setHNSWQuantizationOverride", "sync.(*RWMutex).Lock")
	})
	admissionFree := svc.indexMu.TryLock()
	if admissionFree {
		svc.indexMu.Unlock()
	}
	svc.ivfpqMu.Unlock()
	require.True(t, admissionFree, "policy must not own indexMu while waiting on pipelineMu")

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
