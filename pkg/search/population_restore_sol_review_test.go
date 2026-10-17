package search

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// openPersistedAutoDimensions opens a persisted service over dir whose vector
// dimensions are inferred, not fixed.
func openPersistedAutoDimensions(t *testing.T, engine storage.Engine, dir, backend string) *Service {
	t.Helper()
	svc := NewServiceWithDimensionsAndBM25EngineAndOptions(engine, 3, backend, nil)
	t.Cleanup(func() { _ = svc.Close() })
	svc.SetFulltextIndexPath(filepath.Join(dir, "bm25"))
	svc.SetVectorIndexPath(filepath.Join(dir, "vectors"))
	svc.SetHNSWIndexPath(filepath.Join(dir, "hnsw"))
	svc.SetPersistenceEnabled(true)
	return svc
}

func vectorHitNodeIDs(t *testing.T, svc *Service, query []float32) []string {
	t.Helper()
	hits, err := svc.VectorSearchCandidates(context.Background(), query, &SearchOptions{Limit: 10})
	require.NoError(t, err)
	ids := make([]string, 0, len(hits))
	for _, h := range hits {
		ids = append(ids, normalizeVectorResultIDToNodeID(h.ID))
	}
	return ids
}

// Review sol-r1 finding 1: a provisional write must not infer vector
// dimensions. Before admission the in-memory index is empty even though the
// saved source holds vectors; inferring from the write would mark that source
// cleared and authoritative, and the saved vectors would be lost for good.
func TestProvisionalWriteDoesNotReplaceSavedVectorDimensions(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "10")
	for _, backend := range []string{"v1", "v2"} {
		t.Run(backend, func(t *testing.T) {
			engine := storage.NewMemoryEngine()
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			_, err := engine.CreateNode(admissionNode("nornic:a", "aterm", []float32{1, 0, 0}, "named"))
			require.NoError(t, err)
			dir := t.TempDir()
			seed := openPersistedAutoDimensions(t, engine, dir, backend)
			require.NoError(t, seed.BuildIndexes(context.Background()))
			seed.PersistIndexesToDisk()
			require.NoError(t, seed.Close())

			svc := openPersistedAutoDimensions(t, engine, dir, backend)
			b := admissionNode("nornic:b", "bterm", []float32{0, 1}, "named")
			_, err = engine.CreateNode(b)
			require.NoError(t, err)
			require.NoError(t, svc.IndexNode(b))
			require.Eventually(t, func() bool { return svc.pendingNodeAdmission() == nil }, 10*time.Second, 10*time.Millisecond)

			require.Equal(t, 3, svc.VectorIndexDimensions(), "a provisional write replaced the saved source's dimensions")
			require.Contains(t, vectorHitNodeIDs(t, svc, []float32{1, 0, 0}), "nornic:a",
				"the saved vector disappeared after a differently dimensioned provisional write")
			require.NoError(t, svc.BuildIndexes(context.Background()))
			require.Contains(t, vectorHitNodeIDs(t, svc, []float32{1, 0, 0}), "nornic:a", "an explicit build did not recover it")
		})
	}
}

// Review sol-r1 finding 2: a vector-source reset that fails after clearing
// the source still re-applies the recorded writes before releasing indexMu.
func TestResetBaseVectorSourceFailureKeepsLiveWrites(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "10")
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	svc := NewServiceWithDimensions(engine, 3)
	t.Cleanup(func() { _ = svc.Close() })
	svc.FixVectorDimensions()
	// A non-empty directory where the saved graph lives: removing the saved
	// artifacts fails after the source has been cleared.
	hnswPath := filepath.Join(t.TempDir(), "hnsw")
	require.NoError(t, os.MkdirAll(filepath.Join(hnswPath, "keep"), 0o755))
	svc.SetHNSWIndexPath(hnswPath)
	job := svc.claimNodeAdmission() // a population is pending: writes are recorded
	t.Cleanup(func() { svc.finishNodeAdmission(job, nil, false) })

	b := admissionNode("nornic:b", "bterm", []float32{0, 1, 0}, "named")
	_, err := engine.CreateNode(b)
	require.NoError(t, err)
	require.NoError(t, svc.IndexNode(b))
	require.Contains(t, vectorHitNodeIDs(t, svc, []float32{0, 1, 0}), "nornic:b", "precondition: the write is searchable")

	require.Error(t, svc.resetBaseVectorSource(), "precondition: removing the saved artifacts fails")
	require.Contains(t, vectorHitNodeIDs(t, svc, []float32{0, 1, 0}), "nornic:b",
		"a successful write left vector search after a failed source reset")
}

// Review sol-r1 finding 3: when staging the saved vector file fails, the
// population does not repeat the load under indexMu, where every live write
// would wait for it.
func TestFailedVectorStagingIsNotReloadedUnderWriteLock(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "10")
	for _, backend := range []string{"v1", "v2"} {
		t.Run(backend, func(t *testing.T) {
			engine := storage.NewMemoryEngine()
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			_, err := engine.CreateNode(admissionNode("nornic:a", "aterm", []float32{1, 0, 0}, "named"))
			require.NoError(t, err)
			dir := t.TempDir()
			seed := configureNodeAdmissionService(t, engine, dir, backend, nil)
			require.NoError(t, seed.BuildIndexes(context.Background()))
			seed.PersistIndexesToDisk()
			require.NoError(t, seed.Close())
			// A saved vector file that cannot be loaded.
			require.NoError(t, os.WriteFile(filepath.Join(dir, "vectors.vec"), []byte("not a vector file"), 0o600))

			svc := configureNodeAdmissionService(t, engine, dir, backend, nil)
			var lockedLoads atomic.Int32
			count := func(s *Service) {
				if s == svc {
					lockedLoads.Add(1)
				}
			}
			vectorStoreLockedLoadHook.Store(&count)
			t.Cleanup(func() { vectorStoreLockedLoadHook.Store(nil) })

			require.NoError(t, svc.BuildIndexes(context.Background()))
			require.Zero(t, lockedLoads.Load(), "the failed vector file was loaded again under indexMu")
			require.Contains(t, vectorHitNodeIDs(t, svc, []float32{1, 0, 0}), "nornic:a", "the population rebuilt the vectors from storage")
		})
	}
}
