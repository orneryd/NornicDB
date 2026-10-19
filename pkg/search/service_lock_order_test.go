package search

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
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
	t.Fatal("could not stage " + why)
}
func serviceLockStack(parts ...string) bool {
	b := make([]byte, 1<<20)
	n := runtime.Stack(b, true)
	for _, g := range strings.Split(string(b[:n]), "\n\n") {
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
func serviceLockDump(t *testing.T, why string) {
	t.Helper()
	b := make([]byte, 1<<20)
	n := runtime.Stack(b, true)
	t.Fatalf("%s\n%s", why, b[:n])
}

// All operations are production operations; locks only stage their ordinary
// overlap. A policy update holds indexMu, and ClearVectorIndex holds pipelineMu.
func TestServiceClearVersusQuantization(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
	svc.mu.Lock()
	configured := make(chan error, 1)
	go func() { configured <- svc.ConfigureHNSWQuantization(context.Background(), HNSWQuantizationConfig{}) }()
	serviceLockWait(t, "configuration owning indexMu", func() bool {
		if svc.indexMu.TryLock() {
			svc.indexMu.Unlock()
			return false
		}
		return true
	})
	cleared := make(chan error, 1)
	go func() { cleared <- svc.ClearVectorIndex() }()
	serviceLockWait(t, "clear waiting at its lock boundary", func() bool { return serviceLockStack("(*Service).clearIndexes", "sync.(*RWMutex).Lock") })
	svc.mu.Unlock()
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
	_ = svc.Close()
}

// Pipeline resolution first prepares the index, then takes pipelineMu and mu.
// Gate it at those real boundaries, then queue a normal persistence retirement
// while its Count holds mu. Its second RLock must not deadlock on that writer.
func TestServicePipelineDimensionsVersusClose(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
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
		if err != nil {
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
	_ = svc.Close()
}

type serviceLockBuildEngine struct {
	storage.Engine
	lastWrite func() time.Time
}

func (e *serviceLockBuildEngine) LastWriteTime() time.Time { return e.lastWrite() }

// A base build must keep its source until retirement joins buildMu. The engine
// callback stages shutdown or persistence disable during the supported freshness
// check; no service pointer is changed by this test. Old retirement reached the
// file lock directly; corrected retirement waits for the enclosing build.
func TestServiceBuildResumeVersusRetirement(t *testing.T) {
	for _, closeService := range []bool{false, true} {
		t.Run(fmt.Sprintf("close=%t", closeService), func(t *testing.T) {
			engine := &serviceLockBuildEngine{Engine: storage.NewMemoryEngine()}
			svc := NewServiceWithDimensions(engine, 2)
			svc.SetVectorIndexPath(t.TempDir() + "/vectors")
			svc.SetPersistenceEnabled(true)
			if err := svc.IndexNode(&storage.Node{ID: "live", ChunkEmbeddings: [][]float32{{1, 0}}}); err != nil {
				t.Fatal(err)
			}
			staged := make(chan *VectorFileStore, 1)
			retired := make(chan struct{})
			calls := 0
			engine.lastWrite = func() time.Time {
				calls++
				if calls != 2 {
					return time.Time{}
				}
				svc.mu.RLock()
				vfs := svc.vectorFileStore
				svc.mu.RUnlock()
				if vfs == nil {
					t.Fatal("missing loaded vector store")
				}
				vfs.mu.Lock()
				go func() {
					if closeService {
						_ = svc.Close()
					} else {
						svc.SetPersistenceEnabled(false)
					}
					close(retired)
				}()
				method := "(*Service).SetPersistenceEnabled"
				if closeService {
					method = "(*Service).Close"
				}
				serviceLockWait(t, "retirement waiting on build or file store", func() bool { return serviceLockStack(method, "sync.(*RWMutex).Lock") })
				staged <- vfs
				return time.Time{}
			}
			built := make(chan error, 1)
			go func() {
				defer func() {
					if p := recover(); p != nil {
						built <- fmt.Errorf("BuildIndexes panicked: %v", p)
					}
				}()
				built <- svc.BuildIndexes(context.Background())
			}()
			var vfs *VectorFileStore
			select {
			case vfs = <-staged:
			case err := <-built:
				t.Fatalf("build never reached shutdown overlap: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("build never staged")
			}
			serviceLockWait(t, "resume read or build cancellation", func() bool {
				return serviceLockStack("(*Service).BuildIndexes", "(*VectorFileStore).Count") || len(built) > 0
			})
			vfs.mu.Unlock()
			select {
			case <-retired:
			case <-time.After(5 * time.Second):
				serviceLockDump(t, "shutdown did not finish")
			}
			select {
			case err := <-built:
				if err != nil && !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				serviceLockDump(t, "build did not finish after shutdown")
			}
			svc.mu.RLock()
			store := svc.vectorFileStore
			svc.mu.RUnlock()
			if store != nil {
				t.Fatal("build republished the file store after shutdown")
			}
			_ = svc.Close()
		})
	}
}
