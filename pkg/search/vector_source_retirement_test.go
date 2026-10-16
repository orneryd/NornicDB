package search

import (
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func sourceRetirementNode(t *testing.T, svc *Service, id string, vector []float32) {
	t.Helper()
	node := &storage.Node{ID: storage.NodeID(id), ChunkEmbeddings: [][]float32{vector}}
	_, err := svc.engine.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, svc.IndexNode(node))
	require.NoError(t, waitNodeAdmission(t, svc))
}

func sourceRetirementIDs(t *testing.T, svc *Service, want ...string) {
	t.Helper()
	results, err := svc.VectorSearchCandidates(context.Background(), []float32{1, 0, 0}, &SearchOptions{Limit: 10})
	require.NoError(t, err)
	ids := make([]string, len(results))
	for i, result := range results {
		ids[i] = result.ID
	}
	require.ElementsMatch(t, want, ids)
}

func TestVectorSourceMigrationRetiresCachedMemoryPipeline(t *testing.T) {
	for _, populated := range []bool{false, true} {
		name := "empty"
		if populated {
			name = "populated"
		}
		t.Run(name, func(t *testing.T) {
			t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "10")
			svc := NewServiceWithDimensions(newNamespacedEngine(t), 3)
			t.Cleanup(func() { _ = svc.Close() })
			svc.FixVectorDimensions()
			if populated {
				sourceRetirementNode(t, svc, "before", []float32{1, 0, 0})
			}
			// Include ordinary first ingestion: no dimension change can clear
			// the empty pipeline, since configured dimensions are fixed.
			_, err := svc.getOrCreateVectorPipeline(context.Background())
			require.NoError(t, err)
			require.NotNil(t, cachedVectorPipeline(svc))
			svc.SetVectorIndexPath(filepath.Join(t.TempDir(), "vectors"))
			svc.SetPersistenceEnabled(true)
			sourceRetirementNode(t, svc, "after", []float32{1, 1, 0})
			require.NotNil(t, svc.vectorFileStore)
			if populated {
				sourceRetirementIDs(t, svc, "before", "after")
			} else {
				sourceRetirementIDs(t, svc, "after")
			}
		})
	}
}

func TestVectorSourcePersistenceDisableRetainsPublicRetrieval(t *testing.T) {
	for _, maximum := range []string{"0", "10"} {
		t.Run(maximum, func(t *testing.T) {
			t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", maximum)
			svc := NewServiceWithDimensions(newNamespacedEngine(t), 3)
			t.Cleanup(func() { _ = svc.Close() })
			svc.FixVectorDimensions()
			svc.SetVectorIndexPath(filepath.Join(t.TempDir(), "vectors"))
			svc.SetPersistenceEnabled(true)
			sourceRetirementNode(t, svc, "before", []float32{3, 4, 0})
			_, err := svc.getOrCreateHNSWIndex(context.Background(), 3)
			require.NoError(t, err)
			sourceRetirementIDs(t, svc, "before")
			old := svc.vectorFileStore
			stored, ok := old.GetVector("before")
			require.True(t, ok)
			svc.SetPersistenceEnabled(false)
			require.Nil(t, svc.vectorFileStore)
			preserved, ok := svc.vectorIndex.GetVector("before")
			require.True(t, ok, "persistence disable discarded an existing vector")
			require.Equal(t, stored, preserved, "source migration renormalized canonical coordinates")
			sourceRetirementNode(t, svc, "after", []float32{1, 0, 0})
			sourceRetirementIDs(t, svc, "before", "after")
			require.NoError(t, svc.RemoveNode("before"))
			svc.SetPersistenceEnabled(true)
			sourceRetirementNode(t, svc, "again", []float32{1, 1, 0})
			sourceRetirementIDs(t, svc, "after", "again")
			require.NotNil(t, svc.vectorFileStore)
			require.False(t, svc.vectorFileStore.Has("before"), "re-enabled persistence resurrected a deleted vector")
		})
	}
}

func TestVectorSourceClearDoesNotReloadRetiredFileVectors(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "10")
	svc := NewServiceWithDimensions(newNamespacedEngine(t), 3)
	t.Cleanup(func() { _ = svc.Close() })
	svc.FixVectorDimensions()
	svc.SetVectorIndexPath(filepath.Join(t.TempDir(), "vectors"))
	svc.SetPersistenceEnabled(true)
	sourceRetirementNode(t, svc, "before", []float32{1, 0, 0})
	require.NoError(t, svc.persistBaseIndexes())
	svc.ClearVectorIndex()
	sourceRetirementNode(t, svc, "after", []float32{1, 1, 0})
	require.False(t, svc.vectorFileStore.Has("before"), "live indexing reloaded cleared on-disk vectors")
	sourceRetirementIDs(t, svc, "after")
}

func TestVectorSourcePersistenceDisableHonorsMemoryBudget(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "10")
	svc := NewServiceWithDimensions(newNamespacedEngine(t), 3)
	t.Cleanup(func() { _ = svc.Close() })
	svc.SetVectorIndexPath(filepath.Join(t.TempDir(), "vectors"))
	svc.SetPersistenceEnabled(true)
	sourceRetirementNode(t, svc, "item", []float32{1, 0, 0})
	old := svc.vectorFileStore
	svc.vectorMemoryMaxBytes = 4
	svc.SetPersistenceEnabled(false)
	require.True(t, svc.persistEnabled.Load(), "a refused transfer must keep the existing persistence policy")
	require.Same(t, old, svc.vectorFileStore)
	sourceRetirementIDs(t, svc, "item")
}

func TestVectorSourceDimensionEligibilityUsesActiveFilePopulation(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "10")
	svc := NewServiceWithDimensions(newNamespacedEngine(t), 3)
	t.Cleanup(func() { _ = svc.Close() })
	svc.SetVectorIndexPath(filepath.Join(t.TempDir(), "vectors"))
	svc.SetPersistenceEnabled(true)
	sourceRetirementNode(t, svc, "before", []float32{1, 0, 0})
	require.Zero(t, svc.vectorIndex.Count())
	svc.maybeAutoSetVectorDimensions(5)
	require.Equal(t, 3, svc.vectorIndex.GetDimensions(), "a populated file source is not an empty first-vector index")
	sourceRetirementIDs(t, svc, "before")
}

func TestVectorSourceMigrationReleasesMemoryLockBeforeServicePublication(t *testing.T) {
	svc := NewServiceWithDimensions(newNamespacedEngine(t), 3)
	t.Cleanup(func() { _ = svc.Close() })
	svc.FixVectorDimensions()
	sourceRetirementNode(t, svc, "before", []float32{1, 0, 0})
	svc.SetVectorIndexPath(filepath.Join(t.TempDir(), "vectors"))
	svc.SetPersistenceEnabled(true)
	node := &storage.Node{ID: "after", ChunkEmbeddings: [][]float32{{0, 1, 0}}}
	_, err := svc.engine.CreateNode(node)
	require.NoError(t, err)
	// This is the source ownership held by the factory before its Count read.
	// Gate real live ingestion at the service publication lock, then verify
	// that a canonical memory Count can still complete inside that ownership.
	svc.mu.RLock()
	var once sync.Once
	release := func() { once.Do(svc.mu.RUnlock) }
	defer release()
	id := make(chan string, 1)
	done := make(chan error, 1)
	go func() { id <- serviceTestGoroutineID(); done <- svc.IndexNode(node) }()
	writerID := <-id
	require.Eventually(t, func() bool {
		return serviceStackContains(writerID, "(*Service).admitVectorFileStore", "sync.(*RWMutex).Lock")
	}, 5*time.Second, time.Millisecond)
	countDone := make(chan struct{})
	go func() { _ = svc.vectorIndex.Count(); close(countDone) }()
	finished := false
	select {
	case <-countDone:
		finished = true
	case <-time.After(50 * time.Millisecond):
	}
	release()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("migration did not finish")
	}
	select {
	case <-countDone:
	case <-time.After(5 * time.Second):
		t.Fatal("memory Count did not finish")
	}
	require.True(t, finished, "migration retained vectorIndex.mu while waiting for service publication")
}

type sourceRetirementBuildLog struct {
	entered, release chan struct{}
	once             sync.Once
}

func (h *sourceRetirementBuildLog) Enabled(context.Context, slog.Level) bool { return true }
func (h *sourceRetirementBuildLog) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *sourceRetirementBuildLog) WithGroup(string) slog.Handler            { return h }
func (h *sourceRetirementBuildLog) Handle(_ context.Context, r slog.Record) error {
	if strings.Contains(r.Message, "BM25 disabled — skipping fulltext build") {
		h.once.Do(func() { close(h.entered); <-h.release })
	}
	return nil
}

func TestVectorSourceRetirementJoinsEnclosingBaseBuild(t *testing.T) {
	for _, operation := range []string{"close", "clear", "disable"} {
		t.Run(operation, func(t *testing.T) {
			svc := NewServiceWithDimensions(newNamespacedEngine(t), 3)
			t.Cleanup(func() { _ = svc.Close() })
			svc.SetIndexFlags(false, true)
			svc.SetVectorIndexPath(filepath.Join(t.TempDir(), "vectors"))
			svc.SetPersistenceEnabled(true)
			_, err := svc.engine.CreateNode(&storage.Node{ID: "item", ChunkEmbeddings: [][]float32{{1, 0, 0}}})
			require.NoError(t, err)
			h := &sourceRetirementBuildLog{entered: make(chan struct{}), release: make(chan struct{})}
			var once sync.Once
			release := func() { once.Do(func() { close(h.release) }) }
			defer release()
			svc.SetLogger(slog.New(h))
			buildDone := make(chan error, 1)
			go func() { buildDone <- svc.BuildIndexes(context.Background()) }()
			select {
			case <-h.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("base build did not enter gate")
			}
			closeDone := make(chan error, 1)
			go func() {
				switch operation {
				case "close":
					closeDone <- svc.Close()
				case "clear":
					svc.ClearVectorIndex()
					closeDone <- nil
				case "disable":
					svc.SetPersistenceEnabled(false)
					closeDone <- nil
				}
			}()
			if operation == "close" {
				require.Eventually(t, func() bool { return svc.lifecycleCtx.Err() != nil }, 5*time.Second, time.Millisecond)
			}
			closedEarly := false
			select {
			case <-closeDone:
				closedEarly = true
			case <-time.After(25 * time.Millisecond):
			}
			release()
			select {
			case err := <-buildDone:
				if operation == "disable" {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, context.Canceled)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("base build did not finish after retirement")
			}
			if !closedEarly {
				select {
				case err := <-closeDone:
					require.NoError(t, err)
				case <-time.After(5 * time.Second):
					t.Fatal("close did not join base build")
				}
			}
			require.False(t, closedEarly, "retirement returned while the enclosing base build could still publish sources")
			require.Nil(t, svc.vectorFileStore)
			if operation == "close" {
				require.ErrorIs(t, svc.BuildIndexes(context.Background()), context.Canceled)
				require.Nil(t, svc.vectorFileStore)
			} else {
				if operation == "clear" {
					require.Zero(t, svc.EmbeddingCount())
				} else {
					require.Equal(t, 1, svc.EmbeddingCount())
				}
				sourceRetirementNode(t, svc, "replacement", []float32{1, 0, 0})
				if operation == "clear" {
					sourceRetirementIDs(t, svc, "replacement")
				} else {
					sourceRetirementIDs(t, svc, "item", "replacement")
				}
			}
		})
	}
}
