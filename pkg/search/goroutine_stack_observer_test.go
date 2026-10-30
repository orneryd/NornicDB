package search

import (
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// The admission observer must find a newly blocked reconcile even when older
// goroutines fill the former fixed-size stack buffer.
func TestClusterGenerationBackfillObserverReadsCompleteStacks(t *testing.T) {
	const population = 8192
	release := make(chan struct{})
	var started, exited sync.WaitGroup
	started.Add(population)
	exited.Add(population)
	for i := 0; i < population; i++ {
		go func() {
			started.Done()
			<-release
			exited.Done()
		}()
	}
	started.Wait()
	defer func() {
		close(release)
		exited.Wait()
	}()

	// Create this service after the older goroutines so its reconcile is beyond
	// the fixed-size prefix. The precondition fails without printing any stacks.
	f := newCGFixture(t, 20, false)
	buf := make([]byte, 1<<20)
	require.Equal(t, len(buf), runtime.Stack(buf, true), "fixture must exceed the former stack-buffer limit")
	done := parkReconcile(t, f.svc, f.svc.EmbeddingCount()+1)
	f.svc.indexMu.Unlock()
	waitReconcile(t, done)
	assertClusterGenerationCoherent(t, f.svc, cgAllChecks)
}
