package storage

import (
	"fmt"
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

// Validating a write under write-behind looks up the buffered nodes sharing
// the key's first value (the buffer's pending value index), not every
// unflushed node of the label: a bulk load stays linear while nothing flushes.
func TestWriteBehind_NodeKeyBulkLoadScalesLinearly(t *testing.T) {
	if testing.Short() {
		t.Skip("timing test")
	}
	run := func(n int) time.Duration {
		var elapsed time.Duration
		t.Run(fmt.Sprintf("nodes-%d", n), func(t *testing.T) {
			engine := newWriteBehindTestEngine(t, time.Hour)
			sm := engine.GetSchemaForNamespace("test")
			require.NoError(t, sm.AddConstraint(factKey))
			idx, _ := sm.ConstraintPropertyIndex(factKey.Name)
			require.NoError(t, sm.BackfillCompositeIndex(idx.Name, nil))
			start := time.Now()
			for i := 0; i < n; i++ {
				require.NoError(t, bufferedTry(t, engine, factNode(fmt.Sprintf("n-%d", i), int64(i), "name", 1)))
			}
			elapsed = time.Since(start)
		})
		return elapsed
	}
	run(200)
	small, large := run(1000), run(8000)
	ratio := float64(large) / float64(small)
	t.Logf("buffered writes: 1000 in %v, 8000 in %v, ratio %.1f (linear ~8, per-write overlay copy ~64)", small, large, ratio)
	require.Less(t, ratio, 20.0)
}
