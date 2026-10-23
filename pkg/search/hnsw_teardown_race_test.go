package search

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestHNSWPreflightSynchronizesWithFileStoreTeardown(t *testing.T) {
	for _, operation := range []string{"close", "disable-persistence"} {
		t.Run(operation, func(t *testing.T) {
			svc := NewServiceWithDimensions(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), 3)
			t.Cleanup(func() { require.NoError(t, svc.Close()) })
			vfs, err := NewVectorFileStore(filepath.Join(t.TempDir(), "vectors"), 3)
			require.NoError(t, err)
			svc.mu.Lock()
			svc.vectorFileStore = vfs
			svc.mu.Unlock()
			resume := svc.pauseHNSWBuild()
			defer resume()

			// Hold precisely the source-read ownership used by preflight. Wait
			// until teardown either reaches its writer lock or incorrectly returns.
			svc.mu.RLock()
			var unlockOnce sync.Once
			unlock := func() { unlockOnce.Do(svc.mu.RUnlock) }
			defer unlock()
			teardownDone := make(chan struct{})
			go func() {
				if operation == "close" {
					_ = svc.Close()
				} else {
					svc.SetPersistenceEnabled(false)
				}
				close(teardownDone)
			}()
			require.Eventually(t, func() bool {
				select {
				case <-teardownDone:
					return true
				default:
				}
				for _, stack := range allGoroutineStacks() {
					if strings.Contains(stack, "(*Service).closeVectorFileStore") && strings.Contains(stack, "sync.(*RWMutex).Lock") {
						return true
					}
				}
				return false
			}, 5*time.Second, time.Millisecond, "teardown did not reach source ownership")
			select {
			case <-teardownDone:
				t.Fatal("teardown released vector storage while a preflight source reader still owned it")
			default:
			}
			require.Same(t, vfs, svc.vectorFileStore)
			vfs.mu.RLock()
			closed := vfs.closed
			vfs.mu.RUnlock()
			require.False(t, closed)

			// A late scheduling call contends with the waiting teardown writer.
			// Cancellation/pausing must still prevent a replacement build.
			preflightDone := make(chan error, 1)
			go func() {
				_, _, err := svc.startHNSWBuild(3)
				preflightDone <- err
			}()
			unlock()
			select {
			case <-teardownDone:
			case <-time.After(5 * time.Second):
				t.Fatal("teardown did not finish after the source reader released")
			}
			select {
			case err := <-preflightDone:
				if operation == "close" {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					require.NoError(t, err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("late preflight did not finish")
			}
			svc.mu.RLock()
			require.Nil(t, svc.vectorFileStore)
			svc.mu.RUnlock()
			svc.hnswMu.RLock()
			require.Nil(t, svc.hnswBuild)
			svc.hnswMu.RUnlock()
		})
	}
}

func TestVectorPipelineSnapshotDoesNotReenterServiceLock(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	for _, operation := range []string{"close", "disable-persistence"} {
		for _, fileBacked := range []bool{false, true} {
			mode := "memory"
			if fileBacked {
				mode = "file"
			}
			t.Run(operation+"/"+mode, func(t *testing.T) {
				svc := NewServiceWithDimensions(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), 3)
				resume := svc.pauseHNSWBuild()
				defer resume()
				var releaseCount func()
				if fileBacked {
					vfs, err := NewVectorFileStore(filepath.Join(t.TempDir(), "vectors"), 3)
					require.NoError(t, err)
					require.NoError(t, vfs.Add("item", []float32{1, 0, 0}))
					svc.vectorFileStore = vfs
					vfs.mu.Lock()
					releaseCount = vfs.mu.Unlock
				} else {
					require.NoError(t, svc.vectorIndex.Add("item", []float32{1, 0, 0}))
					svc.vectorIndex.mu.Lock()
					releaseCount = svc.vectorIndex.mu.Unlock
				}
				var releaseOnce sync.Once
				release := func() { releaseOnce.Do(releaseCount) }
				defer release()
				requestDone := make(chan error, 1)
				requestID := make(chan string, 1)
				go func() {
					requestID <- serviceTestGoroutineID()
					_, err := svc.VectorSearchCandidates(context.Background(), []float32{1, 0, 0}, nil)
					requestDone <- err
				}()
				// The real public pipeline holds its first service read lock and
				// is gated inside embeddingCountLocked on the backing-store lock.
				readerID := <-requestID
				require.Eventually(t, func() bool {
					return serviceStackContains(readerID, "(*Service).getOrCreateVectorPipeline", "(*Service).embeddingCountLocked", "sync.(*RWMutex).RLock")
				}, 5*time.Second, time.Millisecond, "public pipeline did not enter the locked source snapshot")
				teardownDone := make(chan error, 1)
				teardownID := make(chan string, 1)
				go func() {
					teardownID <- serviceTestGoroutineID()
					if operation == "close" {
						teardownDone <- svc.Close()
					} else {
						svc.SetPersistenceEnabled(false)
						teardownDone <- nil
					}
				}()
				writerID := <-teardownID
				require.Eventually(t, func() bool {
					return serviceStackContains(writerID, "(*Service).closeVectorFileStore", "sync.(*RWMutex).Lock")
				}, 5*time.Second, time.Millisecond, "teardown writer did not queue behind the public snapshot")
				// A recursive service RLock after Count would now wait behind
				// the writer while retaining the very read lock that writer needs.
				release()
				select {
				case <-requestDone:
				case <-time.After(2 * time.Second):
					t.Fatal("public pipeline deadlocked re-entering source ownership behind teardown")
				}
				select {
				case err := <-teardownDone:
					require.NoError(t, err)
				case <-time.After(2 * time.Second):
					t.Fatal("teardown could not finish after the public snapshot")
				}
				// Register only after the bounded deadlock assertion: a failing
				// regression must not hang cleanup on the same blocked mutex.
				t.Cleanup(func() { require.NoError(t, svc.Close()) })
			})
		}
	}
}

func serviceTestGoroutineID() string {
	buf := make([]byte, 128)
	n := runtime.Stack(buf, false)
	return strings.Fields(string(buf[:n]))[1]
}

// allGoroutineStacks grows its buffer until the dump fits: a full package run
// can exceed any fixed size and silently drop the goroutine being looked for.
func allGoroutineStacks() []string {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return strings.Split(string(buf[:n]), "\n\n")
		}
		buf = make([]byte, 2*len(buf))
	}
}

func serviceStackContains(id string, frames ...string) bool {
	for _, stack := range allGoroutineStacks() {
		if !strings.HasPrefix(stack, "goroutine "+id+" ") {
			continue
		}
		matched := true
		for _, frame := range frames {
			matched = matched && strings.Contains(stack, frame)
		}
		if matched {
			return true
		}
	}
	return false
}
