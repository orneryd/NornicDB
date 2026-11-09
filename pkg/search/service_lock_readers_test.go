package search

import (
	"context"
	"fmt"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"

	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func serviceLockResult(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		serviceLockDump(t, "Service operation deadlocked")
	}
}

// serviceLockExactHits is the public exact vector read this file's admin-writer
// case stages against. It also asserts the result the read must preserve: the
// one live indexed vector, by ID.
func serviceLockExactHits(svc *Service) error {
	hits, err := svc.VectorQueryNodes(context.Background(), []float32{1, 0}, VectorQuerySpec{Exact: true, Limit: 1})
	if err != nil {
		return err
	}
	if len(hits) != 1 || hits[0].ID != "live" {
		return fmt.Errorf("exact read lost its indexed vector: %v", hits)
	}
	return nil
}

// serviceLockLiveNode indexes one live node with exactly the given chunk
// vectors, so a case can stage the number of vector reads the ordinary exact
// scoring loop performs.
func serviceLockLiveNode(t *testing.T, chunks ...[]float32) *Service {
	t.Helper()
	svc := NewService(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), 2, nil)
	require.NoError(t, svc.IndexNode(&storage.Node{ID: "live", ChunkEmbeddings: chunks}))
	return svc
}

// The public exact read and an ordinary configuration write both use service
// metadata under svc.mu: SetDefaultMinSimilarity holds it for the whole write,
// and vectorQueryNodes reads the indexed dimensions/count and the metadata
// snapshot under it. That mutual exclusion is staged with the production leaf
// locks rather than a caller context: beginRead derives the read's context, so
// a caller's Done is observed once at read admission, before any service lock,
// and cannot mark the metadata boundary.
//
// Four boundaries are covered, each on its own live fixture:
//   - metadata access: an admin write holds svc.mu, so the read's own metadata
//     access cannot complete until the write releases it.
//   - snapshot admission: indexMu read-admits the exact snapshot, so the read
//     parks on its first snapshot line while an ordinary publication completes
//     and its snapshot then still resolves the live vector.
//   - vector leaf: the captured vector index holds the read inside its snapshot,
//     so an admin write queues on the metadata write lock while the read still
//     owns the metadata snapshot.
//   - concurrent publication: no staging; both public calls complete and the
//     write is published.
func TestServiceExactVectorReadVersusAdminWriter(t *testing.T) {
	t.Run("metadata access waits for the admin write lock", func(t *testing.T) {
		svc := serviceLockLiveNode(t, []float32{1, 0})
		svc.mu.Lock()
		var once sync.Once
		release := func() { once.Do(svc.mu.Unlock) }
		t.Cleanup(release)

		queried := make(chan error, 1)
		go func() { queried <- serviceLockExactHits(svc) }()
		serviceLockWait(t, "exact read waiting for metadata access", func() bool {
			reading := func(method string) bool {
				return serviceLockStack("(*Service).VectorQueryNodes(", method, "sync.(*RWMutex).RLock")
			}
			// The read's first metadata access may now be the vector-storage health
			// probe before the count/dimension reads. The staged property is the
			// same: the read cannot complete while the admin write holds svc.mu.
			return reading("(*Service).VectorIndexDimensions") || reading("(*Service).EmbeddingCount") ||
				reading("(*Service).vectorStorageError")
		})
		select {
		case err := <-queried:
			serviceLockDump(t, fmt.Sprintf("exact read completed while the admin metadata lock was held: %v", err))
		default:
		}
		release()
		serviceLockResult(t, queried)
		require.NoError(t, svc.Close())
	})

	t.Run("snapshot admission excludes an admin publication", func(t *testing.T) {
		svc := serviceLockLiveNode(t, []float32{1, 0})
		svc.indexMu.Lock()
		var once sync.Once
		release := func() { once.Do(svc.indexMu.Unlock) }
		t.Cleanup(release)

		queried := make(chan error, 1)
		go func() { queried <- serviceLockExactHits(svc) }()
		serviceLockWait(t, "exact read waiting for its metadata snapshot", func() bool {
			return serviceLockStack("(*Service).vectorQueryNodesExact.func1", "sync.(*RWMutex).RLock")
		})

		// The ordinary configuration writer publishes while the read is parked at
		// the snapshot's admission lock, so the snapshot it is about to take must
		// observe the publication.
		svc.SetDefaultMinSimilarity(0.25)
		require.Equal(t, 0.25, svc.GetDefaultMinSimilarity())
		select {
		case err := <-queried:
			serviceLockDump(t, fmt.Sprintf("exact read took its snapshot without its admission lock: %v", err))
		default:
		}
		release()
		serviceLockResult(t, queried)
		require.Equal(t, 0.25, svc.GetDefaultMinSimilarity())
		require.NoError(t, svc.Close())
	})

	// The original exact read scored its snapshot under svc.mu.RLock and reached
	// each vector through the public locking getter, so an admin write queued
	// behind that read made the read's own second metadata RLock wait for the
	// writer: a deadlock. Neither a caller context nor a single metadata hold can
	// stage that overlap, because beginRead consumes the read's context before any
	// service lock and the read's early count/dimension reads take svc.mu for
	// themselves. The staging therefore uses two production boundaries in order:
	// indexMu holds the read at snapshot admission until it has passed those early
	// reads, then the captured vector index holds it inside the scoring loop.
	t.Run("vector leaf read spans a queued admin write", func(t *testing.T) {
		// Two chunks make the ordinary public read perform a second vector read
		// after the admin write is queued. That is the reachable condition which
		// turns a recursive metadata read into a deadlock instead of a harmless
		// re-entrant read, so a regression to public locking also fails this case.
		svc := serviceLockLiveNode(t, []float32{1, 0}, []float32{0, 1})

		// Own releases and the actual worker joins are pre-registered before any
		// assertion: both releases are idempotent, and no step reacquires svc.mu
		// behind the queued writer.
		var heldMu sync.Mutex
		releaseAdmission, releaseLeaf := func() {}, func() {}
		release := func() {
			heldMu.Lock()
			defer heldMu.Unlock()
			releaseAdmission()
			releaseLeaf()
		}
		queried, written := make(chan error, 1), make(chan error, 1)
		queryDone, writeDone := make(chan struct{}), make(chan struct{})
		queryStarted, writeStarted := false, false
		closeService := sync.OnceValue(svc.Close)
		t.Cleanup(func() {
			release()
			joined := true
			if queryStarted {
				select {
				case <-queryDone:
				case <-time.After(5 * time.Second):
					t.Error("public exact read did not actually return")
					joined = false
				}
			}
			if writeStarted {
				select {
				case <-writeDone:
				case <-time.After(5 * time.Second):
					t.Error("admin write did not actually return")
					joined = false
				}
			}
			if !joined {
				// Close waits on the same public path, so a worker still inside the
				// service only leaves the tracked engine to the fixture owner.
				return
			}
			if err := closeService(); err != nil {
				t.Errorf("close: %v", err)
			}
		})

		// First stage: hold the read's index admission. The public read passes its
		// early count, dimension and backing reads and then parks on func1's
		// indexMu.RLock, still before any snapshot or metadata read.
		svc.indexMu.Lock()
		heldMu.Lock()
		releaseAdmission = sync.OnceFunc(svc.indexMu.Unlock)
		heldMu.Unlock()

		identity := make(chan string, 1)
		queryStarted = true
		go func() {
			defer close(queryDone)
			identity <- serviceTestGoroutineID()
			queried <- serviceLockExactHits(svc)
		}()
		queryID := <-identity
		serviceLockWait(t, "exact read waiting for its snapshot admission", func() bool {
			return serviceStackContains(queryID, "(*Service).vectorQueryNodesExact.func1", "sync.(*RWMutex).RLock")
		})
		select {
		case err := <-queried:
			serviceLockDump(t, fmt.Sprintf("exact read completed behind its index admission: %v", err))
		default:
		}

		// Second stage: capture the live vector index the read scores from and hold
		// its leaf lock, so the read stops inside its snapshot instead of between
		// snapshots.
		svc.mu.RLock()
		vi := svc.vectorIndex
		svc.mu.RUnlock()
		require.NotNil(t, vi, "the live fixture has no in-memory vector index to stage")
		vi.mu.Lock()
		heldMu.Lock()
		releaseLeaf = sync.OnceFunc(vi.mu.Unlock)
		heldMu.Unlock()

		// Releasing admission lets the read take its snapshot and metadata read
		// locks and park on the staged vector leaf.
		releaseAdmission()
		serviceLockWait(t, "exact read holding its metadata snapshot at the vector leaf", func() bool {
			if !serviceStackContains(queryID, "(*Service).vectorQueryNodesExact.func1", "sync.(*RWMutex).RLock") {
				return false
			}
			// The read owns its snapshot: indexMu is held for reading and metadata
			// is read-locked, so neither write lock is available.
			if svc.indexMu.TryLock() {
				svc.indexMu.Unlock()
				return false
			}
			if svc.mu.TryLock() {
				svc.mu.Unlock()
				return false
			}
			return true
		})

		// The ordinary admin writer then queues on the metadata write lock while
		// the read still owns the metadata snapshot.
		writerIdentity := make(chan string, 1)
		writeStarted = true
		go func() {
			defer close(writeDone)
			writerIdentity <- serviceTestGoroutineID()
			svc.SetDefaultMinSimilarity(0.25)
			written <- nil
		}()
		writerID := <-writerIdentity
		serviceLockWait(t, "admin write waiting for the metadata write lock", func() bool {
			return serviceStackContains(writerID, "(*Service).SetDefaultMinSimilarity", "sync.(*RWMutex).Lock")
		})
		select {
		case <-writeDone:
			serviceLockDump(t, "admin write completed while the exact read held the metadata read lock")
		default:
		}

		// Releasing the staged leaf lock lets the read finish its snapshot and
		// release the metadata read lock, so the queued admin write proceeds.
		releaseLeaf()
		serviceLockResult(t, queried)
		serviceLockResult(t, written)
		require.Equal(t, 0.25, svc.GetDefaultMinSimilarity())
		select {
		case <-queryDone:
		case <-time.After(5 * time.Second):
			serviceLockDump(t, "exact read goroutine did not return")
		}
		select {
		case <-writeDone:
		case <-time.After(5 * time.Second):
			serviceLockDump(t, "admin write goroutine did not return")
		}
		require.NoError(t, closeService())
	})

	t.Run("concurrent publication", func(t *testing.T) {
		svc := serviceLockLiveNode(t, []float32{1, 0})
		start := make(chan struct{})
		queried := make(chan error, 1)
		written := make(chan error, 1)
		go func() {
			<-start
			queried <- serviceLockExactHits(svc)
		}()
		go func() {
			<-start
			svc.SetDefaultMinSimilarity(0.25)
			written <- nil
		}()
		close(start)
		serviceLockResult(t, queried)
		serviceLockResult(t, written)
		require.Equal(t, 0.25, svc.GetDefaultMinSimilarity())
		require.NoError(t, svc.Close())
	})
}

func TestServiceCompressedSettingsVersusAdminWriter(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_ANN_QUALITY", "compressed")
	svc := NewService(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), 2, nil)
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
			svc := NewService(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), 2, nil)
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
			svc := NewService(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), 2, nil)
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
