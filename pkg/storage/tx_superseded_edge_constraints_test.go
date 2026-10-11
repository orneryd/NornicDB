package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The relationship constraints are checked against the transaction's final
// state: a relationship the transaction deleted, or rewrote with another
// value, no longer holds its committed value (#907, I25).
func TestTransactionEdgeConstraintsSkipSupersededRelationships(t *testing.T) {
	day := func(month int) time.Time { return time.Date(2024, time.Month(month), 1, 0, 0, 0, 0, time.UTC) }
	inTx := func(t *testing.T, engine *BadgerEngine, steps func(tx *BadgerTransaction) error) error {
		t.Helper()
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		if err := steps(tx); err != nil {
			_ = tx.Rollback()
			return err
		}
		return tx.Commit()
	}

	t.Run("uniqueness", func(t *testing.T) {
		engine := edgeConstraintFixture(t, Constraint{Name: "u", Type: ConstraintUnique, EntityType: ConstraintEntityRelationship, Label: "OWNS", Properties: []string{"token"}})
		require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "OWNS", Properties: map[string]any{"token": "t"}}))
		require.NoError(t, inTx(t, engine, func(tx *BadgerTransaction) error {
			require.NoError(t, tx.DeleteEdge("test:e1"))
			return tx.CreateEdge(&Edge{ID: "test:e2", StartNode: "test:a", EndNode: "test:b", Type: "OWNS", Properties: map[string]any{"token": "t"}})
		}))
		require.NoError(t, inTx(t, engine, func(tx *BadgerTransaction) error {
			require.NoError(t, tx.UpdateEdge(&Edge{ID: "test:e2", StartNode: "test:a", EndNode: "test:b", Type: "OWNS", Properties: map[string]any{"token": "moved"}}))
			return tx.CreateEdge(&Edge{ID: "test:e3", StartNode: "test:a", EndNode: "test:c", Type: "OWNS", Properties: map[string]any{"token": "t"}})
		}))
		require.Error(t, engine.CreateEdge(&Edge{ID: "test:e4", StartNode: "test:a", EndNode: "test:d", Type: "OWNS", Properties: map[string]any{"token": "t"}}))
	})

	t.Run("temporal no-overlap", func(t *testing.T) {
		engine := edgeConstraintFixture(t, Constraint{Name: "tn", Type: ConstraintTemporal, EntityType: ConstraintEntityRelationship, Label: "RECORDS", Properties: []string{"subject", "valid_from", "valid_to"}})
		interval := func(from, to int) map[string]any {
			return map[string]any{"subject": "s", "valid_from": day(from), "valid_to": day(to)}
		}
		require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "RECORDS", Properties: interval(1, 6)}))
		require.NoError(t, inTx(t, engine, func(tx *BadgerTransaction) error {
			require.NoError(t, tx.DeleteEdge("test:e1"))
			return tx.CreateEdge(&Edge{ID: "test:e2", StartNode: "test:a", EndNode: "test:b", Type: "RECORDS", Properties: interval(2, 7)})
		}))
		require.NoError(t, inTx(t, engine, func(tx *BadgerTransaction) error {
			require.NoError(t, tx.UpdateEdge(&Edge{ID: "test:e2", StartNode: "test:a", EndNode: "test:b", Type: "RECORDS", Properties: interval(10, 12)}))
			return tx.CreateEdge(&Edge{ID: "test:e3", StartNode: "test:a", EndNode: "test:b", Type: "RECORDS", Properties: interval(3, 8)})
		}))
		require.Error(t, engine.CreateEdge(&Edge{ID: "test:e4", StartNode: "test:a", EndNode: "test:b", Type: "RECORDS", Properties: interval(4, 5)}))
	})

	t.Run("cardinality", func(t *testing.T) {
		engine := edgeConstraintFixture(t, Constraint{Name: "c", Type: ConstraintCardinality, EntityType: ConstraintEntityRelationship, Label: "OWNS", Direction: "OUTGOING", MaxCount: 1})
		require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "OWNS", Properties: map[string]any{}}))
		require.NoError(t, inTx(t, engine, func(tx *BadgerTransaction) error {
			require.NoError(t, tx.DeleteEdge("test:e1"))
			return tx.CreateEdge(&Edge{ID: "test:e2", StartNode: "test:a", EndNode: "test:c", Type: "OWNS", Properties: map[string]any{}})
		}))
		// A rewritten relationship still counts, once.
		require.Error(t, inTx(t, engine, func(tx *BadgerTransaction) error {
			require.NoError(t, tx.UpdateEdge(&Edge{ID: "test:e2", StartNode: "test:a", EndNode: "test:c", Type: "OWNS", Properties: map[string]any{"v": int64(1)}}))
			return tx.CreateEdge(&Edge{ID: "test:e3", StartNode: "test:a", EndNode: "test:d", Type: "OWNS", Properties: map[string]any{}})
		}))
	})
}
