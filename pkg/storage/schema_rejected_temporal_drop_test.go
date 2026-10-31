package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// temporalCompletenessMarks reports which runtime completeness marks the
// schema currently holds for c's index key.
func temporalCompletenessMarks(schema *SchemaManager, c Constraint) (complete, incomplete bool) {
	key := temporalIndexCompletenessKey(c)
	_, complete = schema.temporalIndexCompleteKeys[key]
	_, incomplete = schema.temporalIndexIncompleteKeys[key]
	return complete, incomplete
}

// A rejected drop puts back exactly the completeness marks it removed: an
// explicitly incomplete history index stays untrusted and a certified complete
// one stays trusted.
func TestRejectedTemporalConstraintDropRetainsHistoryTrust(t *testing.T) {
	for _, complete := range []bool{false, true} {
		t.Run(map[bool]string{false: "incomplete", true: "complete"}[complete], func(t *testing.T) {
			schema := NewSchemaManager()
			require.NoError(t, schema.AddConstraint(factWindow))
			schema.markTemporalIndexCreated(factWindow, complete)
			engine := &BadgerEngine{}
			engine.temporalIndexReady.Store(true)
			require.Equal(t, complete, engine.temporalIndexAuthoritative(schema, factWindow))

			rejected := errors.New("reject constraint drop")
			schema.SetPersister(func(*SchemaDefinition) error { return rejected })
			require.ErrorIs(t, schema.DropConstraint(factWindow.Name), rejected)
			require.True(t, schema.HasConstraintNamed(factWindow.Name))
			require.Equal(t, complete, engine.temporalIndexAuthoritative(schema, factWindow), "a rejected DDL must not change whether the history index is trusted")
			completeMark, incompleteMark := temporalCompletenessMarks(schema, factWindow)
			require.Equal(t, complete, completeMark)
			require.Equal(t, !complete, incompleteMark, "the rejected drop must restore exactly the marks it removed")
		})
	}
}

// A successful drop still clears both marks: a later constraint over the same
// label and properties starts over.
func TestTemporalConstraintDropClearsCompletenessMarks(t *testing.T) {
	for _, complete := range []bool{false, true} {
		t.Run(map[bool]string{false: "incomplete", true: "complete"}[complete], func(t *testing.T) {
			schema := NewSchemaManager()
			require.NoError(t, schema.AddConstraint(factWindow))
			schema.markTemporalIndexCreated(factWindow, complete)

			require.NoError(t, schema.DropConstraint(factWindow.Name))
			require.False(t, schema.HasConstraintNamed(factWindow.Name))
			completeMark, incompleteMark := temporalCompletenessMarks(schema, factWindow)
			require.False(t, completeMark)
			require.False(t, incompleteMark)
		})
	}
}

// A rejected drop of a contract restores the temporal marks of its compiled
// entry through that entry's own rollback.
func TestRejectedTemporalConstraintContractDropRetainsHistoryTrust(t *testing.T) {
	schema := NewSchemaManager()
	contract := ConstraintContract{
		Name: "fact_contract", TargetEntityType: string(ConstraintEntityNode), TargetLabelOrType: factWindow.Label,
		Entries: []ConstraintContractEntry{{
			Kind:          ConstraintContractKindPrimitiveNode,
			PrimitiveType: string(ConstraintTemporal),
			Properties:    append([]string(nil), factWindow.Properties...),
		}},
	}
	compiled := Constraint{
		Name: ConstraintContractEntryName(contract.Name, 0), Type: ConstraintTemporal,
		EntityType: ConstraintEntityNode, Label: factWindow.Label, Properties: append([]string(nil), factWindow.Properties...),
	}
	require.NoError(t, schema.AddConstraintContractBundle(contract, []Constraint{compiled}, nil, false))
	schema.markTemporalIndexCreated(compiled, false)
	engine := &BadgerEngine{}
	engine.temporalIndexReady.Store(true)
	require.False(t, engine.temporalIndexAuthoritative(schema, compiled))

	rejected := errors.New("reject contract drop")
	schema.SetPersister(func(*SchemaDefinition) error { return rejected })
	require.ErrorIs(t, schema.DropConstraint(contract.Name), rejected)
	require.Len(t, schema.GetAllConstraintContracts(), 1)
	require.True(t, schema.HasConstraintNamed(compiled.Name))
	require.False(t, engine.temporalIndexAuthoritative(schema, compiled), "a rejected contract drop must not promote an incomplete history index into authority")
	completeMark, incompleteMark := temporalCompletenessMarks(schema, compiled)
	require.False(t, completeMark)
	require.True(t, incompleteMark)

	schema.SetPersister(nil)
	require.NoError(t, schema.DropConstraint(contract.Name))
	require.Empty(t, schema.GetAllConstraintContracts())
	require.False(t, schema.HasConstraintNamed(compiled.Name))
	completeMark, incompleteMark = temporalCompletenessMarks(schema, compiled)
	require.False(t, completeMark)
	require.False(t, incompleteMark)
}

// The public consequence: a constraint created over a node written before it
// is explicitly incomplete, and a rejected drop must not promote that index
// into authority. If it did, the overlap check would read only the key's
// (empty) history and accept a node that overlaps the pre-existing window.
func TestRejectedTemporalConstraintDropKeepsIncompleteHistoryUntrusted(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()
	sm := engine.GetSchemaForNamespace("test")
	// The index was rebuilt while the store was empty, so the engine trusts it.
	require.NoError(t, engine.RebuildTemporalIndexes(context.Background()))
	// This node was written before the constraint existed: its key has no
	// history entry until a rebuild.
	commitNodes(t, engine, temporalFact("pre", int64(1), 0, 10))
	require.NoError(t, sm.AddConstraint(factWindow))
	sm.markTemporalIndexCreated(factWindow, false) // created over existing nodes, not backfilled
	require.False(t, engine.temporalIndexAuthoritative(sm, factWindow))

	rejected := errors.New("reject constraint drop")
	sm.SetPersister(func(*SchemaDefinition) error { return rejected })
	require.ErrorIs(t, sm.DropConstraint(factWindow.Name), rejected)
	require.True(t, sm.HasConstraintNamed(factWindow.Name))
	sm.SetPersister(nil)

	scans := countTemporalScans(t)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, createErr := tx.CreateNode(temporalFact("overlap", int64(1), 5, 15))
	require.NoError(t, tx.Rollback())
	require.Error(t, createErr, "the dropped-then-restored constraint still rejects an overlap with the pre-existing window")
	require.NotZero(t, scans.Load(), "an untrusted history index still reads the label")
	require.False(t, engine.temporalIndexAuthoritative(sm, factWindow), "the rejected drop must leave the incomplete history index untrusted")
}
