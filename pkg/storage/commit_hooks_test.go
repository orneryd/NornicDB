package storage

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// testCommitHook records what a database's commit hook is called with.
type testCommitHook struct {
	phases CommitHookPhases
	before func(ctx context.Context, tx *BadgerTransaction, changes *ChangeSet) error

	mu        sync.Mutex
	beforeSet []*ChangeSet
	afterSet  []*ChangeSet
	rolledSet []*ChangeSet
}

func (h *testCommitHook) Phases() CommitHookPhases { return h.phases }

func (h *testCommitHook) BeforeCommit(ctx context.Context, tx *BadgerTransaction, changes *ChangeSet) error {
	h.mu.Lock()
	h.beforeSet = append(h.beforeSet, changes)
	h.mu.Unlock()
	if h.before != nil {
		return h.before(ctx, tx, changes)
	}
	return nil
}

func (h *testCommitHook) AfterCommit(changes *ChangeSet) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.afterSet = append(h.afterSet, changes)
}

func (h *testCommitHook) AfterRollback(changes *ChangeSet) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.rolledSet = append(h.rolledSet, changes)
}

func (h *testCommitHook) calls() (before, after, rolled int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.beforeSet), len(h.afterSet), len(h.rolledSet)
}

// stampCreated is a BeforeCommit that sets key on every node the
// transaction created, as an APOC before trigger's SET does.
func stampCreated(key string) func(context.Context, *BadgerTransaction, *ChangeSet) error {
	return func(_ context.Context, tx *BadgerTransaction, changes *ChangeSet) error {
		for _, change := range changes.Nodes {
			if change.Before != nil || change.After == nil {
				continue
			}
			node := copyNode(change.After)
			node.Properties[key] = "stamped"
			if err := tx.UpdateNode(node); err != nil {
				return err
			}
		}
		return nil
	}
}

func newHookedEngine(t *testing.T, hook CommitHook) *BadgerEngine {
	t.Helper()
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	engine.SetCommitHook("db", hook)
	return engine
}

// withinDeadline fails the test if run doesn't return in time: a commit
// hook that waits for a lock its own commit holds never returns.
func withinDeadline(t *testing.T, what string, run func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		run()
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatalf("%s did not return: deadlock between the commit hook and its commit", what)
	}
}

// BeforeCommit's writes are part of the same commit, and change capture
// records them in the same transaction's events: one create event whose
// state already has the stamp.
func TestCommitHookBeforeWritesJoinTheCommitAndItsChangeEvents(t *testing.T) {
	hook := &testCommitHook{phases: CommitHookBefore | CommitHookAfter, before: stampCreated("stamp")}
	engine := newHookedEngine(t, hook)
	engine.SetChangeCapture("db", ChangeCaptureFull)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "db:a", Labels: []string{"Doc"}, Properties: map[string]any{"name": "A"}})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	node, err := engine.GetNode("db:a")
	require.NoError(t, err)
	require.Equal(t, "stamped", node.Properties["stamp"])

	before, after, rolled := hook.calls()
	require.Equal(t, [3]int{1, 1, 0}, [3]int{before, after, rolled})
	// The hook was given what the transaction wrote before it ran; the
	// after phase sees its own write too, with the commit's sequence.
	require.NotContains(t, hook.beforeSet[0].Nodes[0].After.Properties, "stamp")
	require.Equal(t, "stamped", hook.afterSet[0].Nodes[0].After.Properties["stamp"])
	require.NotZero(t, hook.afterSet[0].CommitSequence)
	require.False(t, hook.afterSet[0].CommitTime.IsZero())

	events, err := engine.ChangeEvents("db", 0, 0, 0)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, byte('c'), events[0].Operation)
	require.Equal(t, "stamped", events[0].After.Properties["stamp"])
	require.Equal(t, hook.afterSet[0].CommitSequence, events[0].CommitSeq)
}

// The hook runs without any lock its commit holds: it reads and writes
// through the transaction, and another transaction of the same capturing
// database begins, writes and commits while the hook runs (the engine's
// write barrier, the unique-constraint commit locks and the change log are
// all free). Commit, CommitContext and Rollback from inside the hook fail
// with the re-entry error instead of re-entering the commit, and the outer
// commit then completes.
func TestCommitHookCannotDeadlockOrReenterItsCommit(t *testing.T) {
	var engine *BadgerEngine
	var concurrent atomic.Bool
	var reentry [3]error
	hook := &testCommitHook{phases: CommitHookBefore | CommitHookAfter | CommitHookRollback}
	hook.before = func(ctx context.Context, tx *BadgerTransaction, changes *ChangeSet) error {
		if !concurrent.CompareAndSwap(false, true) {
			return nil // the concurrent transaction's own commit
		}
		// Reads and writes through the committing transaction.
		read, err := tx.GetNode("db:a")
		if err != nil {
			return err
		}
		updated := copyNode(read)
		updated.Properties["seen"] = true
		if err := tx.UpdateNode(updated); err != nil {
			return err
		}
		if _, err := tx.CreateNode(&Node{ID: "db:b", Labels: []string{"Doc"}}); err != nil {
			return err
		}
		// No re-entry.
		reentry[0] = tx.Commit()
		reentry[1] = tx.CommitContext(ctx)
		reentry[2] = tx.Rollback()
		// A peer transaction of the same database commits meanwhile.
		peer, err := engine.BeginTransaction()
		if err != nil {
			return err
		}
		if _, err := peer.CreateNode(&Node{ID: "db:peer", Labels: []string{"Doc"}}); err != nil {
			return err
		}
		return peer.Commit()
	}
	engine = newHookedEngine(t, hook)
	engine.SetChangeCapture("db", ChangeCaptureDiff)
	require.NoError(t, engine.GetSchemaForNamespace("db").AddUniqueConstraint("doc_name", "Doc", "name"))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "db:a", Labels: []string{"Doc"}, Properties: map[string]any{"name": "A"}})
	require.NoError(t, err)
	withinDeadline(t, "Commit", func() { err = tx.CommitContext(context.Background()) })
	require.NoError(t, err)

	for i, reentered := range reentry {
		require.ErrorIs(t, reentered, ErrCommitHookReentry, "re-entry %d", i)
	}
	for _, id := range []NodeID{"db:a", "db:b", "db:peer"} {
		_, err := engine.GetNode(id)
		require.NoError(t, err, string(id))
	}
	node, err := engine.GetNode("db:a")
	require.NoError(t, err)
	require.Equal(t, true, node.Properties["seen"])
	require.False(t, tx.IsActive())
	// Two commits, each with one hook pass.
	before, after, _ := hook.calls()
	require.Equal(t, 2, before)
	require.Equal(t, 2, after)
}

// BeforeCommit runs once per commit: what it writes is never handed to a
// hook again, however many writes it makes.
func TestCommitHookBeforeRunsOncePerCommit(t *testing.T) {
	hook := &testCommitHook{phases: CommitHookBefore}
	hook.before = func(_ context.Context, tx *BadgerTransaction, changes *ChangeSet) error {
		for i := 0; i < 3; i++ {
			if err := stampCreated("stamp")(context.Background(), tx, changes); err != nil {
				return err
			}
		}
		return nil
	}
	engine := newHookedEngine(t, hook)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	for _, id := range []NodeID{"db:a", "db:b"} {
		_, err = tx.CreateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{}})
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())
	before, _, _ := hook.calls()
	require.Equal(t, 1, before)
	require.Len(t, hook.beforeSet[0].Nodes, 2)
}

// The hook's error is Commit's: nothing the transaction or the hook wrote
// is stored, and AfterRollback hears what the transaction had changed.
func TestCommitHookErrorRollsTheTransactionBack(t *testing.T) {
	failure := errors.New("trigger failed")
	hook := &testCommitHook{phases: CommitHookBefore | CommitHookAfter | CommitHookRollback}
	hook.before = func(ctx context.Context, tx *BadgerTransaction, changes *ChangeSet) error {
		if err := stampCreated("stamp")(ctx, tx, changes); err != nil {
			return err
		}
		return failure
	}
	engine := newHookedEngine(t, hook)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "db:a", Labels: []string{"Doc"}, Properties: map[string]any{}})
	require.NoError(t, err)
	require.ErrorIs(t, tx.Commit(), failure)
	require.False(t, tx.IsActive())
	_, err = engine.GetNode("db:a")
	require.ErrorIs(t, err, ErrNotFound)

	before, after, rolled := hook.calls()
	require.Equal(t, [3]int{1, 0, 1}, [3]int{before, after, rolled})
	require.Len(t, hook.rolledSet[0].Nodes, 1)
	require.Zero(t, hook.rolledSet[0].CommitSequence)
}

// An explicit Rollback of a transaction that changed something calls
// AfterRollback; one that changed nothing calls nothing, and neither does
// its commit.
func TestCommitHookRollbackAndEmptyTransactions(t *testing.T) {
	hook := &testCommitHook{phases: CommitHookBefore | CommitHookAfter | CommitHookRollback}
	engine := newHookedEngine(t, hook)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "db:a", Labels: []string{"Doc"}, Properties: map[string]any{"v": int64(1)}})
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	before, after, rolled := hook.calls()
	require.Equal(t, [3]int{0, 0, 1}, [3]int{before, after, rolled})
	require.Equal(t, "db:a", string(hook.rolledSet[0].Nodes[0].ID))
	require.Nil(t, hook.rolledSet[0].Nodes[0].Before)

	empty, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, empty.SetNamespace("db"))
	require.NoError(t, empty.Commit())
	before, after, rolled = hook.calls()
	require.Equal(t, [3]int{0, 0, 1}, [3]int{before, after, rolled})
}

// A database with a hook writes in transactions only: a direct engine write
// reaches the hook. Another database's direct writes stay direct and call
// nothing; updates and deletes give the committed state as Before.
func TestCommitHookDirectWritesRunInATransaction(t *testing.T) {
	hook := &testCommitHook{phases: CommitHookBefore, before: stampCreated("stamp")}
	engine := newHookedEngine(t, hook)

	_, err := engine.CreateNode(&Node{ID: "db:a", Labels: []string{"Doc"}, Properties: map[string]any{"v": int64(1)}})
	require.NoError(t, err)
	node, err := engine.GetNode("db:a")
	require.NoError(t, err)
	require.Equal(t, "stamped", node.Properties["stamp"])

	require.NoError(t, engine.UpdateNode(&Node{ID: "db:a", Labels: []string{"Doc", "Seen"}, Properties: map[string]any{"v": int64(2)}}))
	require.NoError(t, engine.DeleteNode("db:a"))
	_, err = engine.CreateNode(&Node{ID: "other:a", Labels: []string{"Doc"}, Properties: map[string]any{}})
	require.NoError(t, err)
	other, err := engine.GetNode("other:a")
	require.NoError(t, err)
	require.NotContains(t, other.Properties, "stamp")

	before, _, _ := hook.calls()
	require.Equal(t, 3, before)
	update := hook.beforeSet[1].Nodes[0]
	require.Equal(t, int64(1), update.Before.Properties["v"])
	require.Equal(t, []string{"Doc", "Seen"}, update.After.Labels)
	deleted := hook.beforeSet[2].Nodes[0]
	require.NotNil(t, deleted.Before)
	require.Nil(t, deleted.After)
}

// No hook, no cost: the engine holds no hook map until one is set, and
// removing the last hook drops it again, so commits read one nil pointer.
func TestCommitHookRegistryIsNilWithoutHooks(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	require.Nil(t, engine.commitHooks.Load())
	require.False(t, engine.needsCommitTransaction("db"))

	hook := &testCommitHook{phases: CommitHookBefore}
	engine.SetCommitHook("db", hook)
	engine.SetCommitHook("db2", hook)
	require.True(t, engine.needsCommitTransaction("db"))
	require.Same(t, hook, engine.CommitHook("db").(*testCommitHook))
	require.Nil(t, engine.CommitHook(""))
	engine.SetCommitHook("db", nil)
	require.Nil(t, engine.CommitHook("db"))
	engine.SetCommitHook("db2", nil)
	require.Nil(t, engine.commitHooks.Load())

	hooks, ok := CommitHookEngineOf(NewNamespacedEngine(engine, "db"))
	require.True(t, ok)
	require.Same(t, engine, hooks.(*BadgerEngine))
}
