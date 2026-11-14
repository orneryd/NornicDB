package search

import (
	"context"
	"errors"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"

	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func serviceLockWait(t *testing.T, why string, ready func() bool) {
	t.Helper()
	until := time.Now().Add(5 * time.Second)
	for time.Now().Before(until) {
		if ready() {
			return
		}
		runtime.Gosched()
		time.Sleep(time.Millisecond)
	}
	serviceLockDump(t, "could not stage "+why)
}
func serviceLockStack(parts ...string) bool {
	for _, g := range strings.Split(serviceLockStacks(), "\n\n") {
		found := true
		for _, p := range parts {
			if !strings.Contains(g, p) {
				found = false
				break
			}
		}
		if found {
			return true
		}
	}
	return false
}
func serviceLockStacks() string {
	for size := 1 << 20; ; size *= 2 {
		b := make([]byte, size)
		n := runtime.Stack(b, true)
		if n < len(b) {
			return string(b[:n])
		}
	}
}
func serviceLockDump(t *testing.T, why string) {
	t.Helper()
	t.Fatalf("%s\n%s", why, serviceLockStacks())
}

// All operations are production operations; locks only stage their ordinary
// overlap. A policy update holds indexMu, and ClearVectorIndex holds pipelineMu.
func TestServiceClearVersusQuantization(t *testing.T) {
	svc := NewService(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), 2, nil)
	// Let policy preflight read the current configuration and join any build
	// before staging the admission/publication overlap under the service lock.
	svc.pipelineMu.Lock()
	pipelineHeld, serviceHeld := true, false
	defer func() {
		if serviceHeld {
			svc.mu.Unlock()
		}
		if pipelineHeld {
			svc.pipelineMu.Unlock()
		}
		_ = svc.Close()
	}()
	configured := make(chan error, 1)
	go func() { configured <- svc.ConfigureHNSWQuantization(context.Background(), HNSWQuantizationConfig{}) }()
	serviceLockWait(t, "configuration waiting for publication", func() bool {
		return serviceLockStack("setHNSWQuantizationOverride", "sync.(*RWMutex).Lock")
	})
	svc.mu.Lock()
	serviceHeld = true
	svc.pipelineMu.Unlock()
	pipelineHeld = false
	serviceLockWait(t, "configuration owning indexMu", func() bool {
		if svc.indexMu.TryLock() {
			svc.indexMu.Unlock()
			return false
		}
		return true
	})
	if svc.pipelineMu.TryLock() {
		svc.pipelineMu.Unlock()
		t.Fatal("configuration owns indexMu without first owning pipelineMu")
	}
	cleared := make(chan error, 1)
	go func() { cleared <- svc.ClearVectorIndex() }()
	serviceLockWait(t, "clear waiting at its lock boundary", func() bool { return serviceLockStack("(*Service).clearIndexes", "sync.(*RWMutex).Lock") })
	svc.mu.Unlock()
	serviceHeld = false
	select {
	case err := <-configured:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		serviceLockDump(t, "configuration and ClearVectorIndex deadlock")
	}
	select {
	case err := <-cleared:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		serviceLockDump(t, "clear did not finish")
	}
}

// Pipeline resolution first prepares the index, then takes pipelineMu and mu.
// Gate it at those real boundaries, then queue a normal persistence retirement
// while its Count holds mu. Its second RLock must not deadlock on that writer.
func TestServicePipelineDimensionsVersusClose(t *testing.T) {
	svc := NewService(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), 2, nil)
	svc.indexMu.Lock()
	queried := make(chan error, 1)
	go func() { _, err := svc.getOrCreateVectorPipeline(context.Background()); queried <- err }()
	serviceLockWait(t, "pipeline preparing GPU", func() bool { return serviceLockStack("prepareGPUIndexForCurrentStrategy", "getOrCreateVectorPipeline") })
	svc.pipelineMu.Lock()
	svc.indexMu.Unlock()
	serviceLockWait(t, "pipeline waiting at construction", func() bool {
		return serviceLockStack("getOrCreateVectorPipeline", "sync.(*RWMutex).Lock") && !serviceLockStack("prepareGPUIndexForCurrentStrategy", "getOrCreateVectorPipeline")
	})
	svc.vectorIndex.mu.Lock()
	svc.pipelineMu.Unlock()
	serviceLockWait(t, "pipeline count holding service read lock", func() bool { return serviceLockStack("getOrCreateVectorPipeline", "embeddingCountLocked") })
	retired := make(chan struct{})
	go func() { _ = svc.Close(); close(retired) }()
	serviceLockWait(t, "retirement waiting for service lock", func() bool {
		if svc.mu.TryRLock() {
			svc.mu.RUnlock()
			return false
		}
		return true
	})
	svc.vectorIndex.mu.Unlock()
	select {
	case err := <-queried:
		// Close now cancels service-owned graph construction. The query must
		// still leave its dimension read without recursively locking mu.
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		serviceLockDump(t, "pipeline recursively RLocks mu behind pending retirement writer")
	}
	select {
	case <-retired:
	case <-time.After(time.Second):
		serviceLockDump(t, "retirement did not finish")
	}
	if !errors.Is(svc.lifecycleCtx.Err(), context.Canceled) || svc.currentPipelineStrategy() != strategyModeUnknown {
		t.Fatal("shutdown left a live service or published pipeline")
	}
	_ = svc.Close()
}

type serviceLockBuildEngine struct {
	storage.Engine
	count func(context.Context) (int64, error)
}

func (e *serviceLockBuildEngine) NodeCount(ctx context.Context) (int64, error) { return e.count(ctx) }

// A base build keeps its source until retirement joins the build owner. The engine
// callback stages shutdown or persistence disable during the supported node-count
// read, before rejected population retirement; no service pointer is changed. Old retirement reached the
// file lock directly; Close now joins the lifetime before delegated disposal.
func TestServiceRebuildVersusRetirement(t *testing.T) {
	for _, closeService := range []bool{false, true} {
		t.Run(fmt.Sprintf("close=%t", closeService), func(t *testing.T) {
			dir := t.TempDir()
			engine := &serviceLockBuildEngine{Engine: fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))}
			svc := NewService(engine, 2, nil)
			ctx, cancel := context.WithCancel(t.Context())
			staged := make(chan *VectorFileStore, 1)
			built, retired := make(chan error, 1), make(chan error, 1)
			buildIdentity := make(chan uint64, 1)
			buildDone, retireDone := make(chan struct{}), make(chan struct{})
			var heldMu sync.Mutex
			unlock := func() {}
			cleaning, retireStarted, buildStarted := false, false, false
			releaseHeld := func() { heldMu.Lock(); release := unlock; heldMu.Unlock(); release() }
			// Own release and actual goroutine joins before any callback assertion or
			// launch. In particular testing.Goexit in a build callback is not a panic.
			t.Cleanup(func() {
				cancel()
				heldMu.Lock()
				cleaning = true
				heldMu.Unlock()
				releaseHeld()
				if buildStarted {
					select {
					case <-buildDone:
					case <-time.After(5 * time.Second):
						t.Error("build did not actually return during cleanup")
					}
				}
				heldMu.Lock()
				started := retireStarted
				heldMu.Unlock()
				if started {
					select {
					case <-retireDone:
					case <-time.After(5 * time.Second):
						t.Error("retirement did not actually return during cleanup")
					}
				}
				svc.BeginShutdown()
				final, cancelFinal := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancelFinal()
				if err := svc.lifetimeWork.WaitStopped(final); err != nil {
					t.Errorf("Close cleanup: %v", err)
				}
			})
			svc.SetVectorIndexPath(filepath.Join(dir, "vectors"))
			svc.SetPersistenceEnabled(true)
			if err := svc.IndexNode(&storage.Node{ID: "live", ChunkEmbeddings: [][]float32{{1, 0}}}); err != nil {
				t.Fatal(err)
			}
			engine.count = func(ctx context.Context) (int64, error) {
				svc.mu.RLock()
				vfs := svc.vectorFileStore
				svc.mu.RUnlock()
				if vfs == nil {
					t.Fatal("missing loaded vector store")
				}
				heldMu.Lock()
				if cleaning {
					heldMu.Unlock()
					return engine.Engine.NodeCount(ctx)
				}
				vfs.mu.Lock()
				unlock = sync.OnceFunc(vfs.mu.Unlock)
				retireStarted = true
				heldMu.Unlock()
				transferred := false
				defer func() {
					if !transferred {
						releaseHeld()
					}
				}()
				go func() {
					defer close(retireDone)
					if closeService {
						retired <- svc.Close()
					} else {
						svc.SetPersistenceEnabled(false)
						retired <- nil
					}
				}()
				serviceLockWait(t, "retirement retaining the build's source", func() bool {
					if closeService {
						// Close joins this owner's accepted build; disposal cannot reach the
						// file lock until that build returns its ticket. Do not require Close
						// and the delegated disposer to appear in the same goroutine stack.
						select {
						case <-svc.lifetimeWork.Quiescent():
							return false
						default:
						}
						select {
						case <-retireDone:
							return false
						default:
						}
						return svc.Closed() && svc.lifetimeWork.Closing() && svc.lifetimeWork.Active() > 0
					}
					return serviceLockStack("(*Service).SetPersistenceEnabled", "sync.(*RWMutex)")
				})
				// Check identity before returning from the callback: the build
				// subsequently holds svc.mu while retiring this rejected store.
				svc.mu.RLock()
				current := svc.vectorFileStore
				svc.mu.RUnlock()
				if current != vfs {
					t.Fatal("retirement replaced the held build source")
				}
				staged <- vfs
				transferred = true
				return engine.Engine.NodeCount(ctx)
			}
			buildStarted = true
			go func() {
				defer close(buildDone)
				buildIdentity <- fixtureowner.CurrentGoroutineID()
				defer func() {
					if p := recover(); p != nil {
						built <- fmt.Errorf("BuildIndexes panicked: %v", p)
					}
				}()
				built <- svc.BuildIndexes(ctx)
			}()
			var buildID uint64
			select {
			case buildID = <-buildIdentity:
			case <-time.After(5 * time.Second):
				t.Fatal("build did not start")
			}
			var vfs *VectorFileStore
			select {
			case vfs = <-staged:
			case <-buildDone:
				// A completed/canceled build may already have handed off the
				// buffered stage. Preserve that stage and its queued outcome.
				select {
				case vfs = <-staged:
				default:
					t.Fatal("build callback exited before transferring its lock")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("build never staged")
			}
			serviceLockWait(t, "rejected store retirement or build cancellation", func() bool {
				return serviceLockStack(fmt.Sprintf("goroutine %d [", buildID), "(*Service).BuildIndexes", "(*VectorFileStore).Close") || len(built) > 0
			})
			// The fixture still owns this VFS lock, so disposal cannot have
			// completed. Do not recursively acquire svc.mu behind store retirement.
			if vfs.closed {
				t.Fatal("retirement closed the held build source")
			}

			select {
			case err := <-retired:
				t.Fatalf("retirement completed before held source release: %v", err)
			default:
			}
			releaseHeld()
			select {
			case err := <-retired:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				serviceLockDump(t, "shutdown did not finish")
			}
			select {
			case <-retireDone:
			case <-time.After(5 * time.Second):
				serviceLockDump(t, "retirement did not actually return")
			}
			select {
			case err := <-built:
				if err != nil && !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				serviceLockDump(t, "build did not finish after shutdown")
			}
			select {
			case <-buildDone:
			case <-time.After(5 * time.Second):
				serviceLockDump(t, "build did not actually return")
			}
			svc.mu.RLock()
			store := svc.vectorFileStore
			svc.mu.RUnlock()
			if store != nil {
				t.Fatal("build republished the file store after shutdown")
			}
			vfs.mu.RLock()
			closed := vfs.closed
			vfs.mu.RUnlock()
			if !closed {
				t.Fatal("retirement did not close the captured file store")
			}
		})
	}
}
