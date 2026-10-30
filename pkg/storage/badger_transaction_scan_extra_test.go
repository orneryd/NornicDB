package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBadgerTransaction_ScanForUniqueViolation_HookNamespaceAndExclude(t *testing.T) {
	engine := newTestEngine(t)

	_, err := engine.CreateNode(&Node{ID: "test:u1", Labels: []string{"User"}, Properties: map[string]any{"email": "a@example.com"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "other:u2", Labels: []string{"User"}, Properties: map[string]any{"email": "a@example.com"}})
	require.NoError(t, err)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	defer tx.Rollback()

	hookCalls := 0
	restore := setUniqueConstraintScanHook(func() { hookCalls++ })
	defer restore()

	// Namespace-limited scan should hit test:u1 and return a violation.
	err = tx.scanForUniqueViolation("test", "User", "email", "a@example.com", "")
	require.Error(t, err)
	var cv *ConstraintViolationError
	require.ErrorAs(t, err, &cv)
	require.Equal(t, ConstraintUnique, cv.Type)
	require.Equal(t, 1, hookCalls)

	// Excluding the existing node ID should suppress the violation.
	err = tx.scanForUniqueViolation("test", "User", "email", "a@example.com", "test:u1")
	require.NoError(t, err)
	require.Equal(t, 2, hookCalls)

	// Namespace mismatch should skip other:u2 and remain clean.
	err = tx.scanForUniqueViolation("missing", "User", "email", "a@example.com", "")
	require.NoError(t, err)
	require.Equal(t, 3, hookCalls)
}

func TestBadgerTransaction_ScanForNodeKeyViolation_AndCheckNodeKeyConstraintBranches(t *testing.T) {
	engine := newTestEngine(t)

	_, err := engine.CreateNode(&Node{ID: "test:k1", Labels: []string{"Account"}, Properties: map[string]any{"tenant": "t1", "ext": "e1"}})
	require.NoError(t, err)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	defer tx.Rollback()

	// Direct scan branch: existing storage node violates composite key.
	err = tx.scanForNodeKeyViolation("test", "Account", []string{"tenant", "ext"}, []any{"t1", "e1"}, "")
	require.Error(t, err)
	var cv *ConstraintViolationError
	require.ErrorAs(t, err, &cv)
	require.Equal(t, ConstraintNodeKey, cv.Type)

	// Pending-node branch in checkNodeKeyConstraint should fail before storage scan.
	tx.pendingNodes["test:pending"] = &Node{
		ID:         "test:pending",
		Labels:     []string{"Account"},
		Properties: map[string]any{"tenant": "t2", "ext": "e2"},
	}
	c := Constraint{Type: ConstraintNodeKey, Label: "Account", Properties: []string{"tenant", "ext"}}
	err = tx.checkNodeKeyConstraint(&Node{ID: "test:new", Labels: []string{"Account"}, Properties: map[string]any{"tenant": "t2", "ext": "e2"}}, c)
	require.Error(t, err)
	require.ErrorAs(t, err, &cv)
	require.Equal(t, ConstraintNodeKey, cv.Type)

	// Null property branch.
	err = tx.checkNodeKeyConstraint(&Node{ID: "test:null", Labels: []string{"Account"}, Properties: map[string]any{"tenant": "t2"}}, c)
	require.Error(t, err)
	require.ErrorAs(t, err, &cv)
	require.Equal(t, ConstraintNodeKey, cv.Type)
}

func TestBadgerTransaction_SnapshotIsolationConflict_MaxSequenceFallback(t *testing.T) {
	tx := &BadgerTransaction{
		readTS: MVCCVersion{CommitTimestamp: time.Unix(200, 0).UTC(), CommitSequence: maxMVCCCommitSequence},
	}

	// Equal max sequence falls back to timestamp comparison.
	require.True(t, tx.snapshotIsolationConflict(MVCCVersion{CommitTimestamp: time.Unix(201, 0).UTC(), CommitSequence: maxMVCCCommitSequence}))
	require.False(t, tx.snapshotIsolationConflict(MVCCVersion{CommitTimestamp: time.Unix(199, 0).UTC(), CommitSequence: maxMVCCCommitSequence}))

	// Different sequence uses timestamp ordering first: a head with a
	// later sequence but an earlier timestamp is logically older (the
	// write-behind flusher backdates heads to the buffered ACK time).
	tx.readTS = MVCCVersion{CommitTimestamp: time.Unix(500, 0).UTC(), CommitSequence: 10}
	require.False(t, tx.snapshotIsolationConflict(MVCCVersion{CommitTimestamp: time.Unix(100, 0).UTC(), CommitSequence: 11}), "backdated head is older than the snapshot")
	require.True(t, tx.snapshotIsolationConflict(MVCCVersion{CommitTimestamp: time.Unix(600, 0).UTC(), CommitSequence: 9}))
	// Equal timestamps fall back to sequence ordering.
	require.True(t, tx.snapshotIsolationConflict(MVCCVersion{CommitTimestamp: time.Unix(500, 0).UTC(), CommitSequence: 11}))
	require.False(t, tx.snapshotIsolationConflict(MVCCVersion{CommitTimestamp: time.Unix(500, 0).UTC(), CommitSequence: 10}))
}
