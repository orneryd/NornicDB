package search

import (
	"context"
	"fmt"

	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestResolveCompressedVectorStrategy covers every compressed-vector outcome:
// no file store (disabled), a ready store that builds IVFPQ candidates, and a
// corrupt persisted bundle that falls back.
func TestResolveCompressedVectorStrategy(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_ANN_QUALITY", "compressed")
	t.Setenv("NORNICDB_VECTOR_IVF_LISTS", "16")
	t.Setenv("NORNICDB_VECTOR_PQ_SEGMENTS", "4")
	t.Setenv("NORNICDB_VECTOR_PQ_BITS", "4")
	t.Setenv("NORNICDB_VECTOR_IVFPQ_NPROBE", "4")

	for _, tc := range []struct {
		name string
		run  func(t *testing.T, svc *Service)
	}{
		{
			name: "falls back when not ready",
			run: func(t *testing.T, svc *Service) {
				strategy, err := svc.resolveVectorStrategy(context.Background(), svc.pipelineBuildSeeds(), 8000, 16, nil)
				require.NoError(t, err)
				require.Contains(t, strategy.name, "compressed-disabled")
			},
		},
		{
			name: "uses IVFPQ when ready",
			run: func(t *testing.T, svc *Service) {
				dir := t.TempDir()
				vfs, err := NewVectorFileStore(filepath.Join(dir, "vectors"), 16)
				require.NoError(t, err)
				defer vfs.Close()

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

				strategy, err := svc.resolveVectorStrategy(context.Background(), svc.pipelineBuildSeeds(), 1500, 16, vfs)
				require.NoError(t, err)
				require.IsType(t, &IVFPQCandidateGen{}, strategy.candidateGen)
			},
		},
		{
			name: "falls back on load error",
			run: func(t *testing.T, svc *Service) {
				dir := t.TempDir()
				vfs, err := NewVectorFileStore(filepath.Join(dir, "vectors"), 16)
				require.NoError(t, err)
				defer vfs.Close()
				require.NoError(t, vfs.Add("doc-1", []float32{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}))

				// Corrupt persisted IVFPQ meta to force load error and exercise compressed-fallback path.
				badBundleDir := filepath.Join(dir, "ivfpq")
				require.NoError(t, os.MkdirAll(badBundleDir, 0o755))
				require.NoError(t, os.WriteFile(filepath.Join(badBundleDir, "meta"), []byte("not-msgpack"), 0o644))

				svc.mu.Lock()
				svc.vectorFileStore = vfs
				svc.vectorIndexPath = filepath.Join(dir, "vectors")
				svc.hnswIndexPath = filepath.Join(dir, "hnsw")
				svc.mu.Unlock()

				strategy, err := svc.resolveCompressedVectorStrategy(context.Background(), svc.pipelineBuildSeeds(), 1500, 16, vfs)
				require.NoError(t, err)
				require.Contains(t, strategy.name, "compressed-fallback")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.run(t, NewService(newTestMemoryEngine(t), 16, nil))
		})
	}
}
