package storage

import (
	"errors"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerTransaction_SetNamespace_ClosedBranches(t *testing.T) {
	engine := newTestEngine(t)

	t.Run("returns closedErr when present", func(t *testing.T) {
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		want := errors.New("synthetic-close")
		tx.mu.Lock()
		tx.Status = TxStatusCommitted
		tx.closedErr = want
		tx.mu.Unlock()
		err = tx.SetNamespace("test")
		require.ErrorIs(t, err, want)
	})

	t.Run("returns ErrTransactionClosed when closedErr nil", func(t *testing.T) {
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		tx.mu.Lock()
		tx.Status = TxStatusRolledBack
		tx.closedErr = nil
		tx.mu.Unlock()
		err = tx.SetNamespace("test")
		require.ErrorIs(t, err, ErrTransactionClosed)
	})
}

func TestBadgerTransaction_UpdateAndDelete_ReadCommittedDecodeErrors(t *testing.T) {
	engine := newTestEngine(t)

	_, err := engine.CreateNode(&Node{ID: "test:n1", Labels: []string{"Doc"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:n2", Labels: []string{"Doc"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:n1", EndNode: "test:n2", Type: "REL"}))

	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		if err := txn.Set(nodeKey("test:n1"), []byte("corrupt-node")); err != nil {
			return err
		}
		return txn.Set(edgeKey("test:e1"), []byte("corrupt-edge"))
	}))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	defer tx.Rollback()

	err = tx.UpdateNode(&Node{ID: "test:n1", Labels: []string{"Doc"}})
	require.Error(t, err)
	require.ErrorContains(t, err, "reading node")

	err = tx.DeleteEdge("test:e1")
	require.Error(t, err)
	require.ErrorContains(t, err, "reading edge")
}

func TestBadgerTransaction_ValidateNodeConstraints_DomainBranch(t *testing.T) {
	engine := newTestEngine(t)

	schema := engine.GetSchemaForNamespace("test")
	require.NotNil(t, schema)
	require.NoError(t, schema.AddConstraint(Constraint{
		Name:          "domain_user_status",
		Type:          ConstraintDomain,
		Label:         "User",
		Properties:    []string{"status"},
		AllowedValues: []interface{}{"active", "pending"},
	}))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	defer tx.Rollback()

	err = tx.validateNodeConstraints(&Node{ID: "test:u1", Labels: []string{"User"}, Properties: map[string]any{"status": "disabled"}})
	require.Error(t, err)
	var cv *ConstraintViolationError
	require.True(t, errors.As(err, &cv))
	require.Equal(t, ConstraintDomain, cv.Type)
}

func TestBadgerTransaction_CheckEdgeTemporalConstraint_GetEdgesByTypeErrorIsPropagated(t *testing.T) {
	engine := newTestEngine(t)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	defer tx.Rollback()

	edge := &Edge{ID: "test:e-temp", StartNode: "test:a", EndNode: "test:b", Type: "REL", Properties: map[string]any{
		"k":  "k1",
		"vf": time.Now().UTC(),
		"vt": time.Now().UTC().Add(time.Hour),
	}}
	c := Constraint{Type: ConstraintTemporal, Label: "REL", Properties: []string{"k", "vf", "vt"}}

	require.NoError(t, engine.Close())
	err = tx.checkEdgeTemporalConstraint(edge, c, "test")
	require.ErrorIs(t, err, ErrStorageClosed)
}

func TestBadgerTransaction_ValidatePolicyOnNodeLabelChange_UsesPendingEdges(t *testing.T) {
	engine := newTestEngine(t)

	_, err := engine.CreateNode(&Node{ID: "test:n1", Labels: []string{"A"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:n2", Labels: []string{"B"}})
	require.NoError(t, err)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	defer tx.Rollback()

	// Populate pending edges so validatePolicyOnNodeLabelChange includes them in both
	// outgoing and incoming passes.
	tx.pendingEdges["test:pe-out"] = &Edge{ID: "test:pe-out", StartNode: "test:n1", EndNode: "test:n2", Type: "REL"}
	tx.pendingEdges["test:pe-in"] = &Edge{ID: "test:pe-in", StartNode: "test:n2", EndNode: "test:n1", Type: "REL"}

	err = tx.validatePolicyOnNodeLabelChange(
		&Node{ID: "test:n1", Labels: []string{"A", "C"}},
		&Node{ID: "test:n1", Labels: []string{"A"}},
	)
	require.NoError(t, err)
}
