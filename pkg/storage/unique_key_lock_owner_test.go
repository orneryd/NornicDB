package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// uniqueKeyLockWorkerTimeout bounds every wait for a constraint key lock
// worker, so a worker that never finishes fails its subtest instead of hanging
// the test binary.
const uniqueKeyLockWorkerTimeout = 5 * time.Second

// uniqueKeyLockWorker runs work that takes constraint key locks on its own
// goroutine and reports the error it produces.
//
// A worker never asserts from its goroutine: it returns its error, and done is
// closed only after that goroutine has finished, so a parent that observes done
// has also observed every release the worker performed. The worker closes its
// own completion signal, so the parent never infers completion from a signal
// that precedes the release (see the "re-entrant for its owner" subtest).
type uniqueKeyLockWorker struct {
	done chan struct{}
	err  error
}

// newUniqueKeyLockWorker starts run on its own goroutine.
func newUniqueKeyLockWorker(run func() error) *uniqueKeyLockWorker {
	worker := &uniqueKeyLockWorker{done: make(chan struct{})}
	go func() {
		worker.err = run()
		close(worker.done)
	}()
	return worker
}

// await waits for the worker to finish and reports the error it produced.
func (worker *uniqueKeyLockWorker) await(t *testing.T, what string) {
	t.Helper()
	select {
	case <-worker.done:
		require.NoError(t, worker.err, what)
	case <-time.After(uniqueKeyLockWorkerTimeout):
		t.Fatalf("%s: worker did not complete within %s", what, uniqueKeyLockWorkerTimeout)
	}
}

// join waits for the worker from a cleanup, where the subtest may already have
// failed: it reports instead of aborting so later cleanups still run.
func (worker *uniqueKeyLockWorker) join(t *testing.T) {
	t.Helper()
	select {
	case <-worker.done:
	case <-time.After(uniqueKeyLockWorkerTimeout):
		t.Errorf("constraint key lock worker did not complete within %s", uniqueKeyLockWorkerTimeout)
	}
}

// uniqueKeyLockCleanup owns the constraint key lock holds and workers a subtest
// starts, so a failed assertion releases every hold and joins every worker
// before the next subtest runs. Release functions are idempotent, so keeping a
// release the subtest already called is safe.
type uniqueKeyLockCleanup struct {
	releases []func()
	workers  []*uniqueKeyLockWorker
}

// hold records a release to call at cleanup. A failed acquisition returns no
// release and is skipped.
func (cleanup *uniqueKeyLockCleanup) hold(release func()) {
	if release != nil {
		cleanup.releases = append(cleanup.releases, release)
	}
}

// start runs work on a worker the cleanup joins.
func (cleanup *uniqueKeyLockCleanup) start(run func() error) *uniqueKeyLockWorker {
	worker := newUniqueKeyLockWorker(run)
	cleanup.workers = append(cleanup.workers, worker)
	return worker
}

// attach registers the cleanup with the subtest. Holds are released in reverse
// acquisition order first, so a worker waiting on one of them can finish, and
// the workers are joined afterwards.
func (cleanup *uniqueKeyLockCleanup) attach(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		for i := len(cleanup.releases) - 1; i >= 0; i-- {
			cleanup.releases[i]()
		}
		for _, worker := range cleanup.workers {
			worker.join(t)
		}
	})
}

// uniqueKeyLockRegistrySizes captures the lock and wait registry sizes under
// the registry mutex. Assertions run on the captured counts after the mutex is
// unlocked, so a failed assertion cannot leave the registry mutex locked and
// strand later subtests.
func uniqueKeyLockRegistrySizes(sm *SchemaManager) (locks, waits int) {
	sm.uniqueConstraintCommitLocksMu.Lock()
	defer sm.uniqueConstraintCommitLocksMu.Unlock()
	return len(sm.uniqueConstraintCommitLocks), len(sm.uniqueConstraintLockWaits)
}

// requireUniqueKeyLockWait waits until owner has registered a wait on one of
// sm's constraint key locks. A wait is registered only while another owner
// holds the key and before the owner blocks, so once it is observed the owner
// holds none of the keys it requested; an exclusion check needs no window of
// its own.
func requireUniqueKeyLockWait(t *testing.T, sm *SchemaManager, owner string) {
	t.Helper()
	require.Eventually(t, func() bool {
		sm.uniqueConstraintCommitLocksMu.Lock()
		defer sm.uniqueConstraintCommitLocksMu.Unlock()
		return sm.uniqueConstraintLockWaits[owner] != nil
	}, uniqueKeyLockWorkerTimeout, time.Millisecond, "owner %q never waited for a constraint key lock", owner)
}

// TestUniqueKeyLocksAreOwned covers the owner-aware constraint key locks
// (#961): an owner takes a lock it holds again without waiting, another
// owner waits until every hold is released, a wait ends with its context,
// a wait that would close a cycle fails with ErrDeadlock, and a release
// callback that outlives its transaction cannot release a newer owner's
// locks when the request buffer is reused.
//
// Each subtest signals acquisition and completion separately: the completion
// signal is closed by the worker after its last release, and the parent asserts
// on registry counts it captured under the registry mutex. A failed assertion
// therefore never runs while the registry mutex is held, and the subtest's
// cleanup releases its holds and joins its workers.
func TestUniqueKeyLocksAreOwned(t *testing.T) {
	sm := NewSchemaManager()
	a := uniqueConstraintLockKey{label: "L", property: "k", value: "a"}
	b := uniqueConstraintLockKey{label: "L", property: "k", value: "b"}
	ctx := context.Background()

	t.Run("re-entrant for its owner", func(t *testing.T) {
		cleanup := &uniqueKeyLockCleanup{}
		cleanup.attach(t)

		first, err := sm.acquireUniqueConstraintCommitLocks(ctx, "t1", []uniqueConstraintLockKey{a})
		require.NoError(t, err)
		cleanup.hold(first)
		second, err := sm.acquireUniqueConstraintCommitLocks(ctx, "t1", []uniqueConstraintLockKey{a, b})
		require.NoError(t, err)
		cleanup.hold(second)
		second()
		// t1 still holds a: another owner waits until the first hold goes.
		acquired := make(chan struct{})
		worker := cleanup.start(func() error {
			release, err := sm.acquireUniqueConstraintCommitLocks(ctx, "t2", []uniqueConstraintLockKey{a})
			if err != nil {
				return err
			}
			close(acquired)
			release()
			return nil
		})
		requireUniqueKeyLockWait(t, sm, "t2")
		select {
		case <-acquired:
			t.Fatal("t2 acquired a lock t1 still holds")
		default:
		}
		first()
		worker.await(t, "t2 could not acquire the released lock")
		select {
		case <-acquired:
		default:
			t.Fatal("t2 completed without acquiring the released lock")
		}
		locks, waits := uniqueKeyLockRegistrySizes(sm)
		require.Zero(t, locks, "constraint key locks held after t2 released")
		require.Zero(t, waits, "constraint key lock waits after t2 released")
	})

	t.Run("a wait ends with its context", func(t *testing.T) {
		cleanup := &uniqueKeyLockCleanup{}
		cleanup.attach(t)

		release, err := sm.acquireUniqueConstraintCommitLocks(ctx, "t1", []uniqueConstraintLockKey{a})
		require.NoError(t, err)
		cleanup.hold(release)
		waitCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
		defer cancel()
		_, err = sm.acquireUniqueConstraintCommitLocks(waitCtx, "t2", []uniqueConstraintLockKey{b, a})
		require.ErrorIs(t, err, context.DeadlineExceeded)
		// b, taken before the wait, was released again.
		other, err := sm.acquireUniqueConstraintCommitLocks(ctx, "t3", []uniqueConstraintLockKey{b})
		require.NoError(t, err)
		cleanup.hold(other)
		other()
		release()
		locks, waits := uniqueKeyLockRegistrySizes(sm)
		require.Zero(t, locks, "constraint key locks held after t2 gave up and t3 released")
		require.Zero(t, waits, "constraint key lock waits after t2 gave up")
	})

	t.Run("a wait closing a cycle is a deadlock", func(t *testing.T) {
		cleanup := &uniqueKeyLockCleanup{}
		cleanup.attach(t)

		releaseA, err := sm.acquireUniqueConstraintCommitLocks(ctx, "t1", []uniqueConstraintLockKey{a})
		require.NoError(t, err)
		cleanup.hold(releaseA)
		releaseB, err := sm.acquireUniqueConstraintCommitLocks(ctx, "t2", []uniqueConstraintLockKey{b})
		require.NoError(t, err)
		cleanup.hold(releaseB)
		// t1 waits on b, which t2 holds, so t2's wait on a closes a cycle.
		waiting := cleanup.start(func() error {
			release, err := sm.acquireUniqueConstraintCommitLocks(ctx, "t1", []uniqueConstraintLockKey{b})
			if err != nil {
				return err
			}
			release()
			return nil
		})
		requireUniqueKeyLockWait(t, sm, "t1")
		_, err = sm.acquireUniqueConstraintCommitLocks(ctx, "t2", []uniqueConstraintLockKey{a})
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrDeadlock), err)
		require.Contains(t, err.Error(), "deadlock detected")
		releaseB()
		waiting.await(t, "t1 could not acquire b after t2 released it")
		releaseA()
		locks, waits := uniqueKeyLockRegistrySizes(sm)
		require.Zero(t, locks, "constraint key locks held after both owners released")
		require.Zero(t, waits, "constraint key lock waits after both owners released")
	})

	// A release callback can outlive its transaction. Calling it again after
	// the request buffer is reused must never release a newer owner's locks.
	t.Run("reused request buffer preserves ownership", func(t *testing.T) {
		reuse := NewSchemaManager()
		cleanup := &uniqueKeyLockCleanup{}
		cleanup.attach(t)
		keys := make([]uniqueConstraintLockKey, 100)
		for i := range keys {
			keys[i] = uniqueConstraintLockKey{entityKind: 1, entityID: fmt.Sprintf("db:node-%d", i)}
		}
		first, err := reuse.acquireUniqueConstraintCommitLocks(context.Background(), "first", keys)
		require.NoError(t, err)
		cleanup.hold(first)
		first()
		second, err := reuse.acquireUniqueConstraintCommitLocks(context.Background(), "second", keys)
		require.NoError(t, err)
		cleanup.hold(second)
		first() // Must not release any request subsequently acquired by second.
		staleCtx, staleCancel := context.WithCancel(context.Background())
		staleCancel()
		staleRelease, err := reuse.acquireUniqueConstraintCommitLocks(staleCtx, "third", keys[:1])
		cleanup.hold(staleRelease)
		require.ErrorIs(t, err, context.Canceled, "third owner acquired an already held key")
		second()
		third, err := reuse.acquireUniqueConstraintCommitLocks(context.Background(), "third", keys)
		require.NoError(t, err)
		cleanup.hold(third)
		third()
	})
}

// TestUniqueMergeKey covers which MERGE lookups lock a key: a node's
// single-property UNIQUE constraint or NODE KEY on the property, with a
// value that has a key.
func TestUniqueMergeKey(t *testing.T) {
	var none *SchemaManager
	_, ok := none.scalarMergeLockKey("L", "k", 1)
	require.False(t, ok)
	sm := NewSchemaManager()
	require.NoError(t, sm.AddConstraint(Constraint{Name: "u", Type: ConstraintUnique, Label: "U", Properties: []string{"k"}}))
	require.NoError(t, sm.AddConstraint(Constraint{Name: "nk", Type: ConstraintNodeKey, Label: "N", Properties: []string{"k"}}))
	require.NoError(t, sm.AddConstraint(Constraint{Name: "pair", Type: ConstraintUnique, Label: "P", Properties: []string{"a", "b"}}))
	require.NoError(t, sm.AddConstraint(Constraint{Name: "r", Type: ConstraintUnique, EntityType: ConstraintEntityRelationship, Label: "R", Properties: []string{"k"}}))
	require.NoError(t, sm.AddConstraint(Constraint{Name: "e", Type: ConstraintExists, Label: "E", Properties: []string{"k"}}))

	key, ok := sm.scalarMergeLockKey("U", "k", int64(7))
	require.True(t, ok)
	require.Equal(t, "U", key.label)
	require.Equal(t, "k", key.property)
	_, ok = sm.scalarMergeLockKey("N", "k", "x")
	require.True(t, ok)
	// A list has a key, as for the commit locks (indexValueKey).
	_, ok = sm.scalarMergeLockKey("U", "k", []interface{}{int64(1)})
	require.True(t, ok)
	for _, missing := range []struct {
		label, property string
		value           interface{}
	}{
		{"U", "other", 1}, {"U", "k", nil}, {"P", "a", 1}, {"R", "k", 1}, {"E", "k", 1}, {"X", "k", 1},
	} {
		_, ok := sm.scalarMergeLockKey(missing.label, missing.property, missing.value)
		require.False(t, ok, "%+v", missing)
	}
}

// TestUniqueKeyLockEdgeBranches: a wait graph that cycles among other
// owners is not this owner's deadlock, a second release is a no-op, and a
// value without a key takes no MERGE lock.
func TestUniqueKeyLockEdgeBranches(t *testing.T) {
	sm := NewSchemaManager()
	heldByA := &uniqueConstraintCommitLock{owner: "a", holds: 1}
	heldByB := &uniqueConstraintCommitLock{owner: "b", holds: 1}
	sm.uniqueConstraintLockWaits = map[string]*uniqueConstraintCommitLock{"a": heldByB, "b": heldByA}
	sm.uniqueConstraintCommitLocksMu.Lock()
	closesCycle := sm.uniqueConstraintLockWaitClosesCycleLocked("c", heldByA, false)
	sm.uniqueConstraintCommitLocksMu.Unlock()
	require.False(t, closesCycle)

	other := NewSchemaManager()
	key := uniqueConstraintLockKey{label: "L", property: "k", value: "x"}
	release, err := other.acquireUniqueConstraintCommitLocks(context.Background(), "t1", []uniqueConstraintLockKey{key})
	require.NoError(t, err)
	release()
	release()
	locks, _ := uniqueKeyLockRegistrySizes(other)
	require.Zero(t, locks)

	require.NoError(t, other.AddConstraint(Constraint{Name: "u", Type: ConstraintUnique, Label: "U", Properties: []string{"k"}}))
	_, ok := other.scalarMergeLockKey("U", "k", struct{ list []int }{list: []int{1}})
	require.False(t, ok)
}

// Keep the scalar regression matrix against the generalized key owner.
func (sm *SchemaManager) scalarMergeLockKey(label, property string, value interface{}) (uniqueConstraintLockKey, bool) {
	keys, err := sm.uniqueMergeKeys(context.Background(), []string{label}, map[string]interface{}{property: value})
	if err != nil {
		panic(err)
	}
	if len(keys) == 0 {
		return uniqueConstraintLockKey{}, false
	}
	return keys[0].lock, true
}
