package search

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type serviceLockQueryContext struct {
	context.Context
	once sync.Once
	gate func()
}

func (c *serviceLockQueryContext) Done() <-chan struct{} {
	c.once.Do(c.gate)
	return c.Context.Done()
}

func serviceLockResult(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		serviceLockDump(t, "Service operation deadlocked")
	}
}

// The public exact read holds the metadata snapshot while checking its
// context. Queue an ordinary configuration writer at that boundary.
func TestServiceExactVectorReadVersusAdminWriter(t *testing.T) {
	svc := NewServiceWithDimensions(newTestMemoryEngine(t), 2)
	require.NoError(t, svc.IndexNode(&storage.Node{ID: "live", ChunkEmbeddings: [][]float32{{1, 0}}}))
	entered, resume := make(chan struct{}), make(chan struct{})
	ctx := &serviceLockQueryContext{Context: context.Background(), gate: func() { close(entered); <-resume }}
	queried := make(chan error, 1)
	go func() {
		hits, err := svc.VectorQueryNodes(ctx, []float32{1, 0}, VectorQuerySpec{Exact: true, Limit: 1})
		if err == nil && (len(hits) != 1 || hits[0].ID != "live") {
			err = fmt.Errorf("exact read lost its indexed vector: %v", hits)
		}
		queried <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("exact read did not reach its metadata snapshot")
	}
	written := make(chan error, 1)
	go func() { svc.SetDefaultMinSimilarity(0.25); written <- nil }()
	serviceLockWait(t, "admin writer queued behind exact read", func() bool {
		if svc.mu.TryRLock() {
			svc.mu.RUnlock()
			return false
		}
		return true
	})
	close(resume)
	serviceLockResult(t, queried)
	serviceLockResult(t, written)
	require.NoError(t, svc.Close())
}

func TestServiceCompressedSettingsVersusAdminWriter(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_ANN_QUALITY", "compressed")
	svc := NewServiceWithDimensions(newTestMemoryEngine(t), 2)
	finished := make(chan error, 2)
	start := make(chan struct{})
	go func() {
		<-start
		for i := 0; i < 5000; i++ {
			if settings := svc.composeStrategyBuildSettings(); !strings.Contains(settings, ";dims=2;") {
				finished <- fmt.Errorf("wrong dimensions in settings: %s", settings)
				return
			}
		}
		finished <- nil
	}()
	go func() {
		<-start
		for i := 0; i < 5000; i++ {
			svc.SetDefaultMinSimilarity(float64(i % 2))
		}
		finished <- nil
	}()
	close(start)
	serviceLockResult(t, finished)
	serviceLockResult(t, finished)
	require.NoError(t, svc.Close())
}

// Both strategy publication paths must take pipelineMu before indexMu, so
// the policy setter cannot capture pipelineMu while they own admission.
func TestServiceStrategyPublicationVersusQuantization(t *testing.T) {
	for _, transition := range []bool{false, true} {
		t.Run(fmt.Sprintf("transition=%t", transition), func(t *testing.T) {
			svc := NewServiceWithDimensions(newTestMemoryEngine(t), 2)
			require.NoError(t, svc.IndexNode(&storage.Node{ID: "live", ChunkEmbeddings: [][]float32{{1, 0}}}))
			// The target is CPU; no device is needed for a GPU candidate descriptor.
			svc.pipelineMu.Lock()
			svc.cacheDerivedVectorPipelineLocked(NewVectorSearchPipeline(NewGPUBruteForceCandidateGen(nil), &IdentityExactScorer{}))
			svc.pipelineMu.Unlock()
			svc.indexMu.Lock()
			switched := make(chan error, 1)
			published := make(chan bool, 1)
			generation := svc.hnswGeneration.Load()
			go func() {
				if transition {
					svc.runStrategyTransition(strategyModeBruteCPU)
				} else {
					published <- svc.switchBruteStrategy(strategyModeBruteCPU)
				}
				switched <- nil
			}()
			method := "(*Service).switchBruteStrategy"
			if transition {
				method = "(*Service).runStrategyTransition"
			}
			serviceLockWait(t, "strategy waiting for mutation admission", func() bool {
				return serviceLockStack(method, "sync.(*RWMutex).Lock")
			})
			configured := make(chan error, 1)
			go func() { configured <- svc.ConfigureHNSWQuantization(context.Background(), HNSWQuantizationConfig{}) }()
			serviceLockWait(t, "policy waiting for publication", func() bool {
				return serviceLockStack("setHNSWQuantizationOverride", "sync.(*RWMutex).Lock")
			})
			svc.indexMu.Unlock()
			serviceLockResult(t, switched)
			serviceLockResult(t, configured)
			if !transition {
				// Policy preflight retires the snapshot captured by the waiting
				// strategy. It must finish without publishing that stale source.
				require.Greater(t, svc.hnswGeneration.Load(), generation)
				require.False(t, <-published, "a retired strategy snapshot was published")
			}
			require.Equal(t, strategyModeUnknown, svc.currentPipelineStrategy())
			require.NoError(t, svc.Close())
		})
	}
}

// HNSW's file iterator invokes callbacks that look vectors up through Service.
// A pending close or append must not turn that lookup into a recursive lock.
func TestServiceFileIterationCallbacksReleaseLeafLock(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_VFS_COMPACT_MIN_OBSOLETE", "1")
	t.Setenv("NORNICDB_VECTOR_VFS_COMPACT_MIN_SIZE_MB", "0")
	for _, action := range []string{"append", "close", "compact"} {
		t.Run(action, func(t *testing.T) {
			svc := NewServiceWithDimensions(newTestMemoryEngine(t), 2)
			svc.SetVectorIndexPath(filepath.Join(t.TempDir(), "vectors"))
			svc.SetPersistenceEnabled(true)
			for _, id := range []string{"a", "b"} {
				require.NoError(t, svc.IndexNode(&storage.Node{ID: storage.NodeID(id), ChunkEmbeddings: [][]float32{{1, 0}}}))
			}
			svc.mu.RLock()
			vfs := svc.vectorFileStore
			svc.mu.RUnlock()
			entered, resume := make(chan struct{}), make(chan struct{})
			iterated := make(chan error, 1)
			go func() {
				count := 0
				err := vfs.IterateChunked(1, func(ids []string, _ [][]float32) error {
					if count == 0 {
						close(entered)
						<-resume
						_, _ = svc.getVectorLookup()(ids[0])
					}
					count += len(ids)
					return nil
				})
				if err == nil && count != 2 {
					err = fmt.Errorf("iteration lost its original snapshot: %d vectors", count)
				}
				iterated <- err
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("iteration did not reach its callback")
			}
			written := make(chan error, 1)
			go func() {
				if action == "close" {
					written <- svc.Close()
				} else if action == "compact" {
					vfs.Remove("a")
					compacted, err := vfs.CompactIfNeeded()
					if err == nil && !compacted {
						err = fmt.Errorf("compaction did not replace the vector file")
					}
					written <- err
				} else {
					written <- vfs.Add("c", []float32{0, 1})
				}
			}()
			serviceLockWait(t, "writer queued or completed", func() bool {
				if len(written) > 0 {
					return true
				}
				if vfs.mu.TryRLock() {
					vfs.mu.RUnlock()
					return false
				}
				return true
			})
			close(resume)
			serviceLockResult(t, iterated)
			serviceLockResult(t, written)
			require.NoError(t, svc.Close())
		})
	}
}
