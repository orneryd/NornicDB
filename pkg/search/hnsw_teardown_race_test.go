package search

import (
	"context"
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
			svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 3)
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
				buf := make([]byte, 1<<20)
				n := runtime.Stack(buf, true)
				for _, stack := range strings.Split(string(buf[:n]), "\n\n") {
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
