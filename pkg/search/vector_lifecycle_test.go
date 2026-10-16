package search

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// maintenanceLoops counts svc's running loops; the receiver is the first
// argument in each runHNSWMaintenance frame.
func maintenanceLoops(svc *Service) int {
	frame := fmt.Sprintf("(*Service).runHNSWMaintenance(%p", svc)
	n := 0
	for _, stack := range allGoroutineStacks() {
		if strings.Contains(stack, frame) {
			n++
		}
	}
	return n
}

// F-I86 B5: the maintenance loop is a registered worker. Close joins it, it
// can be stopped and restarted, and starting it concurrently with stop and
// Close shares no unsynchronised state.
func TestHNSWMaintenanceIsOwnedByTeardown(t *testing.T) {
	t.Setenv("NORNICDB_HNSW_MAINT_INTERVAL_MS", "1")
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
	svc.ensureHNSWMaintenance()
	svc.ensureHNSWMaintenance() // one loop at a time
	require.Eventually(t, func() bool { return maintenanceLoops(svc) == 1 }, 5*time.Second, time.Millisecond)
	require.Equal(t, 1, maintenanceLoops(svc), "a second start ran another loop")

	svc.stopHNSWMaintenance()
	require.False(t, svc.hnswMaintRunning.Load())
	require.Zero(t, maintenanceLoops(svc), "stop did not join the loop")
	svc.ensureHNSWMaintenance()
	require.True(t, svc.hnswMaintRunning.Load(), "a stopped loop could not be restarted")

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); svc.ensureHNSWMaintenance() }()
		go func() { defer wg.Done(); svc.stopHNSWMaintenance() }()
	}
	closed := make(chan error, 1)
	go func() { closed <- svc.Close() }()
	wg.Wait()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return")
	}
	svc.ensureHNSWMaintenance() // refused after Close
	require.False(t, svc.hnswMaintRunning.Load())
	require.Zero(t, maintenanceLoops(svc), "a maintenance loop outlived Close")
}

// Paused worker kinds admit nothing; resume admits again; other kinds are
// unaffected.
func TestLifecycleWorkerRegistryPauseAndResume(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })

	ctx, done, ok := svc.runWorker(context.Background(), workerClustering)
	require.True(t, ok)
	paused := make(chan func(), 1)
	go func() { paused <- svc.pauseWorkers(workerClustering) }()
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("pause did not cancel the running worker")
	}
	select {
	case <-paused:
		t.Fatal("pause returned before the worker finished")
	case <-time.After(20 * time.Millisecond):
	}
	_, other, ok := svc.runWorker(context.Background(), workerMaintenance)
	require.True(t, ok, "pausing one kind refused another")
	other()
	done()
	resume := <-paused
	_, _, ok = svc.runWorker(context.Background(), workerClustering)
	require.False(t, ok, "a paused kind admitted a worker")
	resume()
	_, again, ok := svc.runWorker(context.Background(), workerClustering)
	require.True(t, ok)
	again()
}
