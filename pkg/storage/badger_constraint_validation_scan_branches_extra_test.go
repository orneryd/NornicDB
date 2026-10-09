package storage

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerConstraintValidation_NodeScanBranchCoverage(t *testing.T) {
	engine := newTestEngine(t)
	now := time.Now().UTC()

	_, err := engine.CreateNode(&Node{ID: "test:u1", Labels: []string{"User"}, Properties: map[string]any{"email": "a@x", "tenant": "t1", "uid": "1", "k": "bucket-1", "from": now, "to": now.Add(2 * time.Hour)}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:u2", Labels: []string{"User"}, Properties: map[string]any{"email": "b@x", "tenant": "t1", "uid": "2", "k": "bucket-2", "from": now.Add(3 * time.Hour), "to": now.Add(4 * time.Hour)}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:u3", Labels: []string{"User"}, Properties: map[string]any{"email": "c@x", "tenant": "t2", "uid": "3", "k": "bucket-bad", "from": "bad", "to": now.Add(5 * time.Hour)}})
	require.NoError(t, err)

	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		err := engine.scanForUniqueViolationInTxn(txn, "test", "User", "email", "a@x", "")
		require.Error(t, err)

		err = engine.scanForUniqueViolationInTxn(txn, "test", "User", "email", "a@x", "test:u1")
		require.NoError(t, err)

		err = engine.scanForUniqueViolationInTxn(txn, "other", "User", "email", "a@x", "")
		require.NoError(t, err)

		err = engine.scanForNodeKeyViolationInTxn(txn, "test", "User", []string{"tenant", "uid"}, []any{"t1", "1"}, "")
		require.Error(t, err)

		err = engine.scanForNodeKeyViolationInTxn(txn, "test", "User", []string{"tenant", "uid"}, []any{"t1", "1"}, "test:u1")
		require.NoError(t, err)

		err = engine.legacyScanForTemporalOverlapInTxn(txn, "test", Constraint{Type: ConstraintTemporal, Label: "User", Properties: []string{"k", "from", "to"}}, "bucket-1", now.Add(30*time.Minute), now.Add(90*time.Minute), true, "")
		require.Error(t, err)

		err = engine.legacyScanForTemporalOverlapInTxn(txn, "test", Constraint{Type: ConstraintTemporal, Label: "User", Properties: []string{"k", "from", "to"}}, "bucket-1", now.Add(30*time.Minute), now.Add(90*time.Minute), true, "test:u1")
		require.NoError(t, err)

		err = engine.legacyScanForTemporalOverlapInTxn(txn, "test", Constraint{Type: ConstraintTemporal, Label: "User", Properties: []string{"k", "from", "to"}}, "bucket-bad", now, now.Add(time.Hour), true, "")
		require.Error(t, err)

		// scanForTemporalOverlapInTxn should fall back to legacy path when no temporal history index entries exist.
		err = engine.scanForTemporalOverlapInTxn(txn, "test", Constraint{Type: ConstraintTemporal, Label: "User", Properties: []string{"k", "from", "to"}}, "bucket-1", now.Add(30*time.Minute), now.Add(90*time.Minute), true, "")
		require.Error(t, err)
		return nil
	}))
}
