package storage

// Written by the prune-lockup lane (Claude Opus 5.5), 9 November 2026.
//
// A lifecycle prune cycle is bounded and resumable: a budgeted pass stops at
// its budget, also while it waits for storage or another pass, and the next
// one continues after its last committed decision, so a large history is
// pruned across cycles instead of in one walk. An as-of read, which reads
// history through a fresh view, waits only for the prune batch in flight.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMVCCPruneBudgetedPassesResume(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	const nodes, updates = 3000, 3
	seedPruneHistory(t, engine, prefixTestID("prune-budget"), nodes, updates)
	// Each node has `updates` archived versions; one is kept.
	const want = int64(nodes * (updates - 1))
	const budget = 20 * time.Millisecond
	var total int64
	calls := 0
	for ; total < want; calls++ {
		require.Less(t, calls, 10000, "budgeted passes must make progress")
		started := time.Now()
		deleted, err := engine.PruneMVCCVersions(context.Background(), MVCCPruneOptions{MaxVersionsPerKey: 1, MaxRuntime: budget})
		require.NoError(t, err)
		// The budget ends the call; one admitted commit may finish after it.
		require.Less(t, time.Since(started), budget+2*time.Second)
		total += deleted
	}
	require.Equal(t, want, total)
	require.Greater(t, calls, 1, "one budgeted pass stops at its budget and the next resumes")
	deleted, err := engine.PruneMVCCVersions(context.Background(), MVCCPruneOptions{MaxVersionsPerKey: 1})
	require.NoError(t, err)
	require.Zero(t, deleted, "the budgeted passes pruned everything")
}

// From the round-1 review of #1768 (TestReview1768BudgetBoundsInFlightBatch,
// gpt-6.1-sol): a budgeted prune waiting for storage admission inside its
// batch gives up when its budget is spent, so a historical read is not held
// behind it.
func TestReview1768BudgetBoundsInFlightBatch(t *testing.T) {
	b := createMVCCBadgerEngine(t)
	seedPruneHistory(t, b, "test:review-budget", 2, 4)
	release, done := parkPruneInsideDelete(t, b, MVCCPruneOptions{MaxVersionsPerKey: 1, MaxRuntime: 20 * time.Millisecond})
	started := time.Now()
	select {
	case err := <-done:
		require.NoError(t, err)
		release()
	case <-time.After(250 * time.Millisecond):
		// A normal storage admission wait has outlived the entire cycle budget.
		readCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		_, readErr := b.GetNodeVisibleAt(readCtx, "test:review-budget-0", b.currentMVCCReadVersion("test"))
		release()
		require.NoError(t, <-done)
		t.Fatalf("20ms prune budget still held its batch after %v; as-of read error: %v", time.Since(started), readErr)
	}
	// The spent pass committed nothing; the next one resumes and prunes.
	deleted, err := b.PruneMVCCVersions(context.Background(), MVCCPruneOptions{MaxVersionsPerKey: 1})
	require.NoError(t, err)
	require.EqualValues(t, 2*3, deleted)
}

// A budget spent while another pass holds the pass lock ends the waiting call
// with nothing done, instead of waiting for the other pass.
func TestMVCCPruneBudgetCoversWaitForAnotherPass(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	seedPruneHistory(t, engine, prefixTestID("prune-serial"), 2, 3)
	require.NoError(t, engine.mvccPruneMu.LockContext(context.Background()))
	type outcome struct {
		deleted int64
		err     error
	}
	done := make(chan outcome, 1)
	go func() {
		deleted, err := engine.PruneMVCCVersions(context.Background(), MVCCPruneOptions{MaxVersionsPerKey: 1, MaxRuntime: 20 * time.Millisecond})
		done <- outcome{deleted, err}
	}()
	select {
	case got := <-done:
		engine.mvccPruneMu.Unlock()
		require.NoError(t, got.err)
		require.Zero(t, got.deleted)
	case <-time.After(2 * time.Second):
		engine.mvccPruneMu.Unlock()
		<-done
		t.Fatal("a budgeted prune waited for another pass beyond its budget")
	}
}

func TestMVCCPruneUnbudgetedPassWalksEverything(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	const nodes, updates = mvccPruneBatchKeys + 7, 3
	seedPruneHistory(t, engine, prefixTestID("prune-full"), nodes, updates)
	deleted, err := engine.PruneMVCCVersions(context.Background(), MVCCPruneOptions{MaxVersionsPerKey: 1})
	require.NoError(t, err)
	require.Equal(t, int64(nodes*(updates-1)), deleted, "an explicit prune spans several batches")
}

func TestMVCCAsOfReaderWaitsOnlyForBatchInFlight(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	seedPruneHistory(t, engine, prefixTestID("prune-fence"), 2, 3)
	release, pruned := parkPruneInsideDelete(t, engine, MVCCPruneOptions{MaxVersionsPerKey: 1})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := engine.beginMVCCSnapshotRead(ctx, MVCCVersion{CommitTimestamp: time.Now()})
	require.ErrorIs(t, err, context.DeadlineExceeded, "the batch in flight decided without this reader")
	release()
	require.NoError(t, <-pruned)
	deregister, err := engine.beginMVCCSnapshotRead(context.Background(), MVCCVersion{CommitTimestamp: time.Now()})
	require.NoError(t, err)
	deregister()
}
