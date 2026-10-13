package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Under write-behind, an acknowledged commit reaches the indexes only when the
// background flush applies it. Validation that reads the NODE KEY index or the
// temporal history index must still see buffered (unflushed) writes, as the
// label scan it replaced did.

func bufferedTry(t *testing.T, engine *BadgerEngine, node *Node) error {
	t.Helper()
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	if _, err := tx.CreateNode(node); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func TestWriteBehind_NodeKeySeesBufferedKeys(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)
	sm := engine.GetSchemaForNamespace("test")
	require.NoError(t, sm.AddConstraint(factKey))
	idx, owned := sm.ConstraintPropertyIndex(factKey.Name)
	require.True(t, owned)
	require.NoError(t, sm.BackfillCompositeIndex(idx.Name, nil))

	require.NoError(t, bufferedTry(t, engine, factNode("a", 1, "name", 1)))
	require.Greater(t, engine.writeBehind.PendingOps(), 0, "the first write must still be buffered")
	require.Error(t, bufferedTry(t, engine, factNode("b", 1, "name", 1)), "a duplicate of a buffered key must be rejected")
	require.NoError(t, bufferedTry(t, engine, factNode("c", 1, "name", 2)))
}

func TestWriteBehind_TemporalSeesBufferedPeriods(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)
	sm := engine.GetSchemaForNamespace("test")
	require.NoError(t, sm.AddConstraint(factWindow))
	require.NoError(t, MarkNewTemporalConstraintsForEngine(engine, sm, []Constraint{factWindow}))

	require.NoError(t, bufferedTry(t, engine, temporalFact("a", int64(1), 0, 10)))
	require.Error(t, bufferedTry(t, engine, temporalFact("b", int64(1), 5, 15)), "an overlap with a buffered period must be rejected")
	require.NoError(t, bufferedTry(t, engine, temporalFact("c", int64(1), 10, 20)))
}
