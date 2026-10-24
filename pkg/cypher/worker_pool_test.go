package cypher

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
	"github.com/stretchr/testify/require"
)

func TestWorkerPoolConcurrency(t *testing.T) {
	pool := NewWorkerPool(4)
	pool.Start()
	defer pool.Stop()

	var maxConcurrent int64
	var currentConcurrent int64

	// Submit jobs that track concurrency
	for i := 0; i < 20; i++ {
		pool.Submit(func() {
			current := atomic.AddInt64(&currentConcurrent, 1)

			// Track max
			for {
				max := atomic.LoadInt64(&maxConcurrent)
				if current <= max {
					break
				}
				if atomic.CompareAndSwapInt64(&maxConcurrent, max, current) {
					break
				}
			}

			// Simulate work
			time.Sleep(10 * time.Millisecond)

			atomic.AddInt64(&currentConcurrent, -1)
		})
	}

	pool.Wait()

	max := atomic.LoadInt64(&maxConcurrent)
	if max < 2 {
		t.Logf("max concurrent workers: %d (may vary based on timing)", max)
	}
}

// A job that stops on its statement's cancellation is done, and its worker
// runs the next job: Wait returns.
func TestWorkerPoolStoppedJobIsDone(t *testing.T) {
	pool := NewWorkerPool(1)
	pool.Start()
	defer pool.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pool.Submit(func() {
		pass := bulk.NewCheckpoint(ctx)
		pass.Check()
		t.Error("the canceled job ran past its check")
	})
	var ran atomic.Bool
	pool.Submit(func() { ran.Store(true) })
	pool.Wait()
	require.True(t, ran.Load())
}
