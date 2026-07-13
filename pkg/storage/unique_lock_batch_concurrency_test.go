package storage

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

const (
	uniqueLockBatchWriters       = 8
	uniqueLockBatchKeysPerWriter = 100
)

// TestUniqueConstraintCommitLocks_DisjointBatchesAreParallel reproduces the
// production-sized false-sharing gap that the single-key granularity test does
// not cover. Every writer owns a disjoint set of exact UNIQUE values, so all
// writers must be able to enter the protected commit window concurrently.
func TestUniqueConstraintCommitLocks_DisjointBatchesAreParallel(t *testing.T) {
	sm := &SchemaManager{}
	batches := disjointUniqueLockBatches(uniqueLockBatchWriters, uniqueLockBatchKeysPerWriter)

	start := make(chan struct{})
	acquired := make(chan struct{}, len(batches))
	releaseAll := make(chan struct{})
	var wg sync.WaitGroup
	for _, batch := range batches {
		batch := batch
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			release := sm.acquireUniqueConstraintCommitLocks(batch)
			acquired <- struct{}{}
			<-releaseAll
			release()
		}()
	}
	close(start)

	deadline := time.NewTimer(250 * time.Millisecond)
	defer deadline.Stop()
	acquiredBeforeRelease := 0
collect:
	for acquiredBeforeRelease < len(batches) {
		select {
		case <-acquired:
			acquiredBeforeRelease++
		case <-deadline.C:
			break collect
		}
	}
	close(releaseAll)
	wg.Wait()

	if acquiredBeforeRelease != len(batches) {
		t.Fatalf(
			"disjoint UNIQUE batches acquired concurrently = %d/%d, want %d/%d; bounded lock stripes are serializing unrelated values",
			acquiredBeforeRelease,
			len(batches),
			len(batches),
			len(batches),
		)
	}
}

func BenchmarkUniqueConstraintCommitLocks_DisjointBatches(b *testing.B) {
	batches := disjointUniqueLockBatches(uniqueLockBatchWriters, uniqueLockBatchKeysPerWriter)
	b.ReportMetric(float64(uniqueLockBatchWriters), "writers")
	b.ReportMetric(float64(uniqueLockBatchKeysPerWriter), "keys/writer")
	b.ResetTimer()

	for range b.N {
		sm := &SchemaManager{}
		start := make(chan struct{})
		var wg sync.WaitGroup
		for _, batch := range batches {
			batch := batch
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				release := sm.acquireUniqueConstraintCommitLocks(batch)
				time.Sleep(time.Millisecond)
				release()
			}()
		}
		close(start)
		wg.Wait()
	}
}

func disjointUniqueLockBatches(writers, keysPerWriter int) [][]uniqueConstraintLockKey {
	batches := make([][]uniqueConstraintLockKey, writers)
	for writer := range writers {
		batch := make([]uniqueConstraintLockKey, keysPerWriter)
		for key := range keysPerWriter {
			batch[key] = uniqueConstraintLockKey{
				label:    "Function",
				property: "uid",
				value:    fmt.Sprintf("content-entity:writer-%02d:key-%03d", writer, key),
			}
		}
		batches[writer] = batch
	}
	return batches
}
