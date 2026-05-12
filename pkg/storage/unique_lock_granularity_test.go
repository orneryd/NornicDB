package storage

import (
	"sync"
	"testing"
	"time"
)

// TestUniqueConstraintCommitLocks_DisjointValuesAreParallel pins the
// contract that two transactions whose pending nodes touch disjoint values
// for the same (label, property) constraint can commit in parallel when the
// bounded stripe hash keeps their lock domains separate — they do NOT
// serialize on the full constraint.
//
// Earlier per-(label, property) granularity collapsed throughput to
// single-writer levels under bootstrap-index Pass 2 fan-out: 8 projector
// workers + collector + ingester all writing TerraformResource nodes with
// disjoint uids would serialize at the commit boundary, an effective
// WORKERS=1 contract in disguise. This test fails (deadlock or timeout)
// under the old coarse granularity and passes under value-keyed stripe
// granularity.
func TestUniqueConstraintCommitLocks_DisjointValuesAreParallel(t *testing.T) {
	sm := &SchemaManager{}
	heldKey := uniqueConstraintLockKey{label: "TerraformResource", property: "uid", value: "X"}
	disjointKey := uniqueConstraintLockKey{label: "TerraformResource", property: "uid", value: "Y"}
	for uniqueConstraintLockStripeIndex(heldKey) == uniqueConstraintLockStripeIndex(disjointKey) {
		disjointKey.value = disjointKey.value.(string) + "Y"
	}

	// T1 holds the lock for value "X" indefinitely (until we signal).
	t1Acquired := make(chan struct{})
	t1Release := make(chan struct{})
	t1Done := make(chan struct{})
	go func() {
		defer close(t1Done)
		release := sm.acquireUniqueConstraintCommitLocks([]uniqueConstraintLockKey{heldKey})
		close(t1Acquired)
		<-t1Release
		release()
	}()
	<-t1Acquired

	// T2 acquires the lock for value "Y" — disjoint from T1. Under the new
	// per-value granularity this must NOT block on T1.
	t2Acquired := make(chan struct{})
	t2Done := make(chan struct{})
	go func() {
		defer close(t2Done)
		release := sm.acquireUniqueConstraintCommitLocks([]uniqueConstraintLockKey{disjointKey})
		close(t2Acquired)
		release()
	}()

	select {
	case <-t2Acquired:
		// Expected: T2 ran to completion while T1 still holds its lock.
	case <-time.After(2 * time.Second):
		close(t1Release)
		<-t1Done
		t.Fatal("T2 acquiring a disjoint value's lock blocked on T1 — the lock granularity is too coarse")
	}

	close(t1Release)
	<-t1Done
	<-t2Done
}

func TestUniqueConstraintLockKeyOrderDistinguishesValueTypes(t *testing.T) {
	stringKey := uniqueConstraintLockKey{label: "TerraformResource", property: "uid", value: "1"}
	floatKey := uniqueConstraintLockKey{label: "TerraformResource", property: "uid", value: 1.0}

	if uniqueConstraintLockOrderKey(stringKey) == uniqueConstraintLockOrderKey(floatKey) {
		t.Fatal("lock order key collapsed distinct value types with the same display string")
	}
}

// TestUniqueConstraintCommitLocks_SameValueSerializes pins the converse:
// two transactions touching the SAME (label, property, value) must
// serialize at the commit boundary, otherwise the silent-duplicate-write
// race that the lock exists to prevent comes back. The lock guards the
// validateAllConstraints + commit + RegisterUniqueValue window so a peer
// observing the cache after a winner's commit sees the registered value
// and either fails validation (driver retries) or planner-MATCHes the
// winner's node on a fresh transaction.
func TestUniqueConstraintCommitLocks_SameValueSerializes(t *testing.T) {
	sm := &SchemaManager{}

	t1Acquired := make(chan struct{})
	t1Release := make(chan struct{})
	t1Done := make(chan struct{})
	go func() {
		defer close(t1Done)
		release := sm.acquireUniqueConstraintCommitLocks([]uniqueConstraintLockKey{
			{label: "TerraformResource", property: "uid", value: "X"},
		})
		close(t1Acquired)
		<-t1Release
		release()
	}()
	<-t1Acquired

	t2Acquired := make(chan struct{})
	t2Done := make(chan struct{})
	go func() {
		defer close(t2Done)
		release := sm.acquireUniqueConstraintCommitLocks([]uniqueConstraintLockKey{
			{label: "TerraformResource", property: "uid", value: "X"},
		})
		close(t2Acquired)
		release()
	}()

	select {
	case <-t2Acquired:
		close(t1Release)
		<-t1Done
		<-t2Done
		t.Fatal("T2 acquiring the same value's lock did NOT block on T1 — the lock is missing the serialization guarantee for shared values")
	case <-time.After(100 * time.Millisecond):
		// Expected: T2 is blocked waiting for T1 to release.
	}

	close(t1Release)
	<-t1Done
	<-t2Acquired // T2 should now proceed
	<-t2Done
}

// TestUniqueConstraintCommitLocks_DeterministicOrderingNoDeadlock verifies
// that two transactions touching overlapping sets of values acquire locks
// in the same total order (sorted by label, then property, then value),
// preventing AB-BA deadlock.
//
// Without sort-order acquisition: T1 wants {X, Y}, T2 wants {Y, X}. T1
// holds X, asks for Y; T2 holds Y, asks for X. Deadlock. With sorted
// acquisition, both transactions request X first, then Y; one waits, the
// other progresses.
func TestUniqueConstraintCommitLocks_DeterministicOrderingNoDeadlock(t *testing.T) {
	sm := &SchemaManager{}

	const concurrency = 8
	values := []string{"alpha", "bravo", "charlie", "delta", "echo"}

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			// Each goroutine touches all values but in a different input
			// order. The lock acquirer must sort internally so all
			// goroutines acquire in the same total order.
			keys := make([]uniqueConstraintLockKey, 0, len(values))
			for j := range values {
				// Rotate the order per goroutine.
				v := values[(j+idx)%len(values)]
				keys = append(keys, uniqueConstraintLockKey{
					label:    "Label",
					property: "prop",
					value:    v,
				})
			}
			release := sm.acquireUniqueConstraintCommitLocks(keys)
			// Brief work-simulation window so concurrent goroutines actually
			// overlap on lock holding rather than sequentially zip through.
			time.Sleep(time.Millisecond)
			release()
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		// Expected: all goroutines completed within timeout, no deadlock.
	case <-time.After(5 * time.Second):
		t.Fatal("acquire timed out — possible AB-BA deadlock, sorted acquisition order broken")
	}
}
