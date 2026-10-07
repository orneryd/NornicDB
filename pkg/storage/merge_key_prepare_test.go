package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// newMergeKeyTestEngine returns an engine with a complete UNIQUE constraint
// on (:U {k}) in namespace "test".
func newMergeKeyTestEngine(t *testing.T) (*BadgerEngine, *SchemaManager) {
	t.Helper()
	engine := createTestBadgerEngine(t)
	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddConstraint(Constraint{Name: "u_k", Type: ConstraintUnique, Label: "U", Properties: []string{"k"}}))
	require.NoError(t, RefreshUniqueConstraintValuesForEngine(NewNamespacedEngine(engine, "test"), schema))
	return engine, schema
}

func beginMergeKeyTx(t *testing.T, engine *BadgerEngine) *BadgerTransaction {
	t.Helper()
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	return tx
}

func createKeyNode(t *testing.T, engine *BadgerEngine, id string, k int64) NodeID {
	t.Helper()
	tx := beginMergeKeyTx(t, engine)
	nodeID := NodeID(prefixTestID(id))
	_, err := tx.CreateNode(&Node{ID: nodeID, Labels: []string{"U"}, Properties: map[string]interface{}{"k": k}})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	return nodeID
}

// TestPrepareMergeKeyMovesSnapshotToCommittedNode: a transaction that has
// observed nothing sees a key's node committed after it began once a MERGE
// prepares that key, and can write it; one that has read keeps its snapshot
// (#961).
func TestPrepareMergeKeyMovesSnapshotToCommittedNode(t *testing.T) {
	engine, _ := newMergeKeyTestEngine(t)
	ctx := context.Background()

	fresh := beginMergeKeyTx(t, engine)
	reader := beginMergeKeyTx(t, engine)
	_, _ = reader.GetNodesByLabel("U") // reads its snapshot
	nodeID := createKeyNode(t, engine, "n1", 1)

	require.NoError(t, fresh.PrepareMergeKey(ctx, "U", "k", int64(1)))
	node, err := fresh.GetNode(nodeID)
	require.NoError(t, err)
	require.Equal(t, int64(1), node.Properties["k"])
	node.Properties["seen"] = true
	require.NoError(t, fresh.UpdateNode(node))
	require.NoError(t, fresh.Commit())

	require.NoError(t, reader.PrepareMergeKey(ctx, "U", "k", int64(1)))
	_, err = reader.GetNode(nodeID)
	require.ErrorIs(t, err, ErrNotFound)
	require.NoError(t, reader.Rollback())

	// A key without a constraint, a closed transaction and an
	// already-visible node change nothing.
	visible := beginMergeKeyTx(t, engine)
	require.NoError(t, visible.PrepareMergeKey(ctx, "U", "other", int64(1)))
	require.NoError(t, visible.PrepareMergeKey(ctx, "U", "k", int64(1)))
	_, err = visible.GetNode(nodeID)
	require.NoError(t, err)
	require.NoError(t, visible.Rollback())
	require.NoError(t, visible.PrepareMergeKey(ctx, "U", "k", int64(2)))
}

// TestPrepareMergeKeyWaitsForCreator: a MERGE of a key with no node locks it
// until its transaction ends; a concurrent MERGE of the key waits, then sees
// the committed node. A rolled-back creator lets the waiter take the key.
func TestPrepareMergeKeyWaitsForCreator(t *testing.T) {
	engine, schema := newMergeKeyTestEngine(t)
	ctx := context.Background()

	creator := beginMergeKeyTx(t, engine)
	require.NoError(t, creator.PrepareMergeKey(ctx, "U", "k", int64(5)))
	waiter := beginMergeKeyTx(t, engine)
	done := make(chan error, 1)
	go func() { done <- waiter.PrepareMergeKey(ctx, "U", "k", int64(5)) }()
	select {
	case err := <-done:
		t.Fatalf("waiter didn't wait for the key's creator: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	nodeID := NodeID(prefixTestID("n5"))
	_, err := creator.CreateNode(&Node{ID: nodeID, Labels: []string{"U"}, Properties: map[string]interface{}{"k": int64(5)}})
	require.NoError(t, err)
	require.NoError(t, creator.Commit())
	require.NoError(t, <-done)
	node, err := waiter.GetNode(nodeID)
	require.NoError(t, err)
	require.Equal(t, int64(5), node.Properties["k"])
	require.NoError(t, waiter.Commit())

	rolledBack := beginMergeKeyTx(t, engine)
	require.NoError(t, rolledBack.PrepareMergeKey(ctx, "U", "k", int64(6)))
	next := beginMergeKeyTx(t, engine)
	go func() { done <- next.PrepareMergeKey(ctx, "U", "k", int64(6)) }()
	require.NoError(t, rolledBack.Rollback())
	require.NoError(t, <-done)
	require.NoError(t, next.Rollback())

	// A cancelled wait returns the context's error.
	holder := beginMergeKeyTx(t, engine)
	require.NoError(t, holder.PrepareMergeKey(ctx, "U", "k", int64(7)))
	cancelled, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	other := beginMergeKeyTx(t, engine)
	require.ErrorIs(t, other.PrepareMergeKey(cancelled, "U", "k", int64(7)), context.DeadlineExceeded)
	require.NoError(t, other.Rollback())
	require.NoError(t, holder.Rollback())

	schema.uniqueConstraintCommitLocksMu.Lock()
	require.Empty(t, schema.uniqueConstraintCommitLocks)
	schema.uniqueConstraintCommitLocksMu.Unlock()
}

// TestPrepareMergeKeyDeadlockAtCommit: two transactions holding each other's
// MERGE key and creating the other's key wait for each other at commit; one
// fails with ErrDeadlock, the other commits.
func TestPrepareMergeKeyDeadlockAtCommit(t *testing.T) {
	engine, _ := newMergeKeyTestEngine(t)
	ctx := context.Background()
	first := beginMergeKeyTx(t, engine)
	second := beginMergeKeyTx(t, engine)
	require.NoError(t, first.PrepareMergeKey(ctx, "U", "k", int64(1)))
	require.NoError(t, second.PrepareMergeKey(ctx, "U", "k", int64(2)))
	_, err := first.CreateNode(&Node{ID: NodeID(prefixTestID("a")), Labels: []string{"U"}, Properties: map[string]interface{}{"k": int64(2)}})
	require.NoError(t, err)
	_, err = second.CreateNode(&Node{ID: NodeID(prefixTestID("b")), Labels: []string{"U"}, Properties: map[string]interface{}{"k": int64(1)}})
	require.NoError(t, err)

	results := make(chan error, 2)
	go func() { results <- first.Commit() }()
	go func() { results <- second.Commit() }()
	var deadlocks, committed int
	for i := 0; i < 2; i++ {
		select {
		case err := <-results:
			switch {
			case err == nil:
				committed++
			case errors.Is(err, ErrDeadlock):
				deadlocks++
			default:
				t.Fatalf("unexpected commit error: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("commits never finished")
		}
	}
	require.Equal(t, 1, deadlocks)
	require.Equal(t, 1, committed)
}

// TestPrepareMergeKeyEdgeBranches covers PrepareMergeKey's rarer paths: a
// transaction closed while it waited for the key, a stale cache entry whose
// node has no head, a transaction that observed its snapshot between the
// checks, and a storage error reading the node's head.
func TestPrepareMergeKeyEdgeBranches(t *testing.T) {
	engine, schema := newMergeKeyTestEngine(t)
	ctx := context.Background()

	holder := beginMergeKeyTx(t, engine)
	require.NoError(t, holder.PrepareMergeKey(ctx, "U", "k", int64(9)))
	waiter := beginMergeKeyTx(t, engine)
	done := make(chan error, 1)
	go func() { done <- waiter.PrepareMergeKey(ctx, "U", "k", int64(9)) }()
	require.Eventually(t, func() bool {
		schema.uniqueConstraintCommitLocksMu.Lock()
		defer schema.uniqueConstraintCommitLocksMu.Unlock()
		return schema.uniqueConstraintLockWaits[waiter.ID] != nil
	}, 5*time.Second, time.Millisecond)
	require.NoError(t, waiter.Rollback())
	require.NoError(t, holder.Rollback())
	require.NoError(t, <-done)
	schema.uniqueConstraintCommitLocksMu.Lock()
	require.Empty(t, schema.uniqueConstraintCommitLocks)
	schema.uniqueConstraintCommitLocksMu.Unlock()

	schema.RegisterUniqueValue("U", "k", int64(10), NodeID(prefixTestID("ghost")))
	stale := beginMergeKeyTx(t, engine)
	require.NoError(t, stale.PrepareMergeKey(ctx, "U", "k", int64(10)))
	require.NoError(t, stale.Rollback())

	nodeID := createKeyNode(t, engine, "n11", 11)
	observed := beginMergeKeyTx(t, engine)
	_, _ = observed.GetNodesByLabel("U")
	require.NoError(t, observed.catchUpToNode(nodeID))
	require.NoError(t, observed.Rollback())

	closing := beginMergeKeyTx(t, engine)
	createKeyNode(t, engine, "n12", 12)
	require.NoError(t, engine.Close())
	require.Error(t, closing.PrepareMergeKey(ctx, "U", "k", int64(12)))
}

// TestEngineWriteLockFailureIsReturned: a direct engine write whose key
// locks can't be taken (a deadlock) fails without writing.
func TestEngineWriteLockFailureIsReturned(t *testing.T) {
	engine, _ := newMergeKeyTestEngine(t)
	previous := lockEngineWriteKeys
	lockEngineWriteKeys = func(*SchemaManager, *Node) (func(), error) { return nil, ErrDeadlock }
	t.Cleanup(func() { lockEngineWriteKeys = previous })
	node := &Node{ID: NodeID(prefixTestID("direct")), Labels: []string{"U"}, Properties: map[string]interface{}{"k": int64(20)}}
	_, err := engine.CreateNode(node)
	require.ErrorIs(t, err, ErrDeadlock)
	require.ErrorIs(t, engine.UpdateNode(node), ErrDeadlock)
	lockEngineWriteKeys = previous
	_, err = engine.GetNode(node.ID)
	require.ErrorIs(t, err, ErrNotFound)
}
