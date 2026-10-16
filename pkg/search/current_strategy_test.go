package search

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The status names every strategy a pipeline can run, IVF-PQ and k-means
// routing included, rather than "unknown".
func TestCurrentStrategyNamesEveryStrategy(t *testing.T) {
	svc := NewService(storage.NewMemoryEngine())
	t.Cleanup(func() { _ = svc.Close() })
	require.Equal(t, "unknown", svc.CurrentStrategy())

	for gen, want := range map[CandidateGenerator]string{
		&HNSWCandidateGen{}:       "HNSW",
		&BruteForceCandidateGen{}: "CPU brute-force",
		&IVFPQCandidateGen{}:      "IVFPQ compressed",
		&KMeansCandidateGen{}:     "CPU k-means (cluster routing)",
		&GPUKMeansCandidateGen{}:  "GPU k-means (cluster routing)",
		&IVFHNSWCandidateGen{}:    "IVF-HNSW (per-cluster HNSW)",
	} {
		svc.pipelineMu.Lock()
		svc.vectorPipeline = NewVectorSearchPipeline(gen, nil)
		svc.pipelineMu.Unlock()
		require.Equal(t, want, svc.CurrentStrategy())
	}
}

// Runtime transitions move only between brute force and HNSW: an IVF-PQ or
// k-means pipeline is a configured strategy they never replace.
func TestRuntimeTransitionsOnlyManageBruteAndHNSW(t *testing.T) {
	for mode, managed := range map[strategyMode]bool{
		strategyModeBruteCPU: true, strategyModeBruteGPU: true, strategyModeHNSW: true,
		strategyModeIVFPQ: false, strategyModeKMeansCPU: false, strategyModeKMeansGPU: false,
		strategyModeIVFHNSW: false, strategyModeUnknown: false,
	} {
		require.Equal(t, managed, mode.runtimeManaged(), mode.String())
	}
}

// IVF-PQ and k-means routing probe an eighth of their lists, at least 16, by
// default: a fixed 3 searched about 1.4% of 210 clusters (recall@10 0.80).
func TestDefaultListsToProbe(t *testing.T) {
	require.Equal(t, 16, defaultListsToProbe(100))
	require.Equal(t, 27, defaultListsToProbe(210))
	require.Equal(t, 128, defaultListsToProbe(1024))
	require.Equal(t, 5, defaultListsToProbe(5), "never more than there are")
}
