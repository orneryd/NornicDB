package storage

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestUniqueViolationConcurrentFlag covers #657: a UNIQUE violation is
// Concurrent only when the clashing value was committed after the
// transaction began (it isn't in the transaction's snapshot). Only those can
// succeed on retry, so only those are retry-safe MERGE races.
func TestUniqueViolationConcurrentFlag(t *testing.T) {
	engine := createTestBadgerEngine(t)
	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddUniqueConstraint("u_k", "U", "k"))
	// CREATE CONSTRAINT rebuilds the unique-value cache and marks it complete.
	require.NoError(t, RefreshUniqueConstraintValuesForEngine(engine, schema))
	_, err := engine.CreateNode(&Node{ID: NodeID(prefixTestID("stored")), Labels: []string{"U"}, Properties: map[string]interface{}{"k": int64(5)}})
	require.NoError(t, err)

	violation := func(tx *BadgerTransaction, id string, value int64) *ConstraintViolationError {
		t.Helper()
		_, err := tx.CreateNode(&Node{ID: NodeID(prefixTestID(id)), Labels: []string{"U"}, Properties: map[string]interface{}{"k": value}})
		if err == nil {
			err = tx.Commit()
		} else {
			_ = tx.Rollback()
		}
		require.Error(t, err)
		var cv *ConstraintViolationError
		require.True(t, errors.As(err, &cv), err)
		return cv
	}

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.False(t, violation(tx, "dup-stored", 5).Concurrent, "a value stored before the transaction began is not a race")

	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.GetNode(NodeID(prefixTestID("stored"))) // pin the snapshot
	require.NoError(t, err)
	peer, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = peer.CreateNode(&Node{ID: NodeID(prefixTestID("peer")), Labels: []string{"U"}, Properties: map[string]interface{}{"k": int64(9)}})
	require.NoError(t, err)
	require.NoError(t, peer.Commit())
	require.True(t, violation(tx, "dup-peer", 9).Concurrent, "a value a peer committed after the transaction began is a race")
}
