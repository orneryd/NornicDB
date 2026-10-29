package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestUniqueKeyLocksAreOwned covers the owner-aware constraint key locks
// (#961): an owner takes a lock it holds again without waiting, another
// owner waits until every hold is released, a wait ends with its context,
// a wait that would close a cycle fails with ErrDeadlock, and a release
// callback that outlives its transaction cannot release a newer owner's
// locks when the request buffer is reused.
func TestUniqueKeyLocksAreOwned(t *testing.T) {
	sm := NewSchemaManager()
	a := uniqueConstraintLockKey{label: "L", property: "k", value: "a"}
	b := uniqueConstraintLockKey{label: "L", property: "k", value: "b"}
	ctx := context.Background()

	t.Run("re-entrant for its owner", func(t *testing.T) {
		first, err := sm.acquireUniqueConstraintCommitLocks(ctx, "t1", []uniqueConstraintLockKey{a})
		require.NoError(t, err)
		second, err := sm.acquireUniqueConstraintCommitLocks(ctx, "t1", []uniqueConstraintLockKey{a, b})
		require.NoError(t, err)
		second()
		// t1 still holds a: another owner waits until the first hold goes.
		acquired := make(chan struct{})
		go func() {
			release, err := sm.acquireUniqueConstraintCommitLocks(ctx, "t2", []uniqueConstraintLockKey{a})
			require.NoError(t, err)
			close(acquired)
			release()
		}()
		select {
		case <-acquired:
			t.Fatal("t2 acquired a lock t1 still holds")
		case <-time.After(50 * time.Millisecond):
		}
		first()
		select {
		case <-acquired:
		case <-time.After(5 * time.Second):
			t.Fatal("t2 never acquired the released lock")
		}
		sm.uniqueConstraintCommitLocksMu.Lock()
		require.Empty(t, sm.uniqueConstraintCommitLocks)
		require.Empty(t, sm.uniqueConstraintLockWaits)
		sm.uniqueConstraintCommitLocksMu.Unlock()
	})

	t.Run("a wait ends with its context", func(t *testing.T) {
		release, err := sm.acquireUniqueConstraintCommitLocks(ctx, "t1", []uniqueConstraintLockKey{a})
		require.NoError(t, err)
		waitCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
		defer cancel()
		_, err = sm.acquireUniqueConstraintCommitLocks(waitCtx, "t2", []uniqueConstraintLockKey{b, a})
		require.ErrorIs(t, err, context.DeadlineExceeded)
		// b, taken before the wait, was released again.
		other, err := sm.acquireUniqueConstraintCommitLocks(ctx, "t3", []uniqueConstraintLockKey{b})
		require.NoError(t, err)
		other()
		release()
		sm.uniqueConstraintCommitLocksMu.Lock()
		require.Empty(t, sm.uniqueConstraintCommitLocks)
		require.Empty(t, sm.uniqueConstraintLockWaits)
		sm.uniqueConstraintCommitLocksMu.Unlock()
	})

	t.Run("a wait closing a cycle is a deadlock", func(t *testing.T) {
		releaseA, err := sm.acquireUniqueConstraintCommitLocks(ctx, "t1", []uniqueConstraintLockKey{a})
		require.NoError(t, err)
		releaseB, err := sm.acquireUniqueConstraintCommitLocks(ctx, "t2", []uniqueConstraintLockKey{b})
		require.NoError(t, err)
		waiting := make(chan error, 1)
		go func() {
			release, err := sm.acquireUniqueConstraintCommitLocks(ctx, "t1", []uniqueConstraintLockKey{b})
			if err == nil {
				release()
			}
			waiting <- err
		}()
		require.Eventually(t, func() bool {
			sm.uniqueConstraintCommitLocksMu.Lock()
			defer sm.uniqueConstraintCommitLocksMu.Unlock()
			return sm.uniqueConstraintLockWaits["t1"] != nil
		}, 5*time.Second, time.Millisecond)
		_, err = sm.acquireUniqueConstraintCommitLocks(ctx, "t2", []uniqueConstraintLockKey{a})
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrDeadlock), err)
		require.Contains(t, err.Error(), "deadlock detected")
		releaseB()
		require.NoError(t, <-waiting)
		releaseA()
		sm.uniqueConstraintCommitLocksMu.Lock()
		require.Empty(t, sm.uniqueConstraintCommitLocks)
		require.Empty(t, sm.uniqueConstraintLockWaits)
		sm.uniqueConstraintCommitLocksMu.Unlock()
	})

	// A release callback can outlive its transaction. Calling it again after
	// the request buffer is reused must never release a newer owner's locks.
	t.Run("reused request buffer preserves ownership", func(t *testing.T) {
		reuse := NewSchemaManager()
		keys := make([]uniqueConstraintLockKey, 100)
		for i := range keys {
			keys[i] = uniqueConstraintLockKey{entityKind: 1, entityID: fmt.Sprintf("db:node-%d", i)}
		}
		first, err := reuse.acquireUniqueConstraintCommitLocks(context.Background(), "first", keys)
		if err != nil {
			t.Fatal(err)
		}
		first()
		second, err := reuse.acquireUniqueConstraintCommitLocks(context.Background(), "second", keys)
		if err != nil {
			t.Fatal(err)
		}
		first() // Must not release any request subsequently acquired by second.
		staleCtx, staleCancel := context.WithCancel(context.Background())
		staleCancel()
		if release, err := reuse.acquireUniqueConstraintCommitLocks(staleCtx, "third", keys[:1]); !errors.Is(err, context.Canceled) {
			if release != nil {
				release()
			}
			second()
			t.Fatalf("third owner acquired an already held key: %v", err)
		}
		second()
		third, err := reuse.acquireUniqueConstraintCommitLocks(context.Background(), "third", keys)
		if err != nil {
			t.Fatal(err)
		}
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
	require.False(t, sm.uniqueConstraintLockWaitClosesCycleLocked("c", heldByA, false))
	sm.uniqueConstraintCommitLocksMu.Unlock()

	other := NewSchemaManager()
	key := uniqueConstraintLockKey{label: "L", property: "k", value: "x"}
	release, err := other.acquireUniqueConstraintCommitLocks(context.Background(), "t1", []uniqueConstraintLockKey{key})
	require.NoError(t, err)
	release()
	release()
	other.uniqueConstraintCommitLocksMu.Lock()
	require.Empty(t, other.uniqueConstraintCommitLocks)
	other.uniqueConstraintCommitLocksMu.Unlock()

	require.NoError(t, other.AddConstraint(Constraint{Name: "u", Type: ConstraintUnique, Label: "U", Properties: []string{"k"}}))
	_, ok := other.scalarMergeLockKey("U", "k", struct{ list []int }{list: []int{1}})
	require.False(t, ok)
}

// Keep the scalar regression matrix against the generalized key owner.
func (sm *SchemaManager) scalarMergeLockKey(label, property string, value interface{}) (uniqueConstraintLockKey, bool) {
	keys := sm.uniqueMergeKeys([]string{label}, map[string]interface{}{property: value})
	if len(keys) == 0 {
		return uniqueConstraintLockKey{}, false
	}
	return keys[0].lock, true
}
