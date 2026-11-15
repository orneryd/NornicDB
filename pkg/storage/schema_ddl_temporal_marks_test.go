package storage

// A temporal constraint's runtime completeness marks belong to the schema, not
// to the statement that happened to read it. A schema statement's isolated view
// is built from the durable definition, which carries no runtime marks, so the
// snapshot the statement publishes holds only the marks the statement itself
// established (the constraints it created). Publishing that snapshot must not
// replace the marks of the constraints the statement did not touch: after
// CREATE CONSTRAINT ... over existing nodes (which staged the entries and
// recorded its index complete), an unrelated CREATE INDEX must leave the
// earlier constraint's mark in place.
//
// Losing it is a product regression: the published history index loses its
// authority, so while the engine's rebuild readiness is false (a failed or
// canceled rebuild) every write to the label validates NO OVERLAP by scanning
// the whole label instead of reading the key's own history (#1126).
//
// These controls pin the transfer at its own seam: an unrelated statement
// preserves the mark it did not touch and the label scan stays unused, a drop
// removes exactly its own constraint's mark, and a statement's own staged mark
// owns its constraint identity.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// roleWindow is a second temporal constraint, on another label, so one
// statement can drop it while another constraint's marks stay untouched.
var roleWindow = Constraint{
	Name: "role_window", Type: ConstraintTemporal, EntityType: ConstraintEntityNode,
	Label: "Role", Properties: []string{"role", "valid_from", "valid_to"},
}

// roleNode is a stored "Role" node whose validity window covers the test base
// day, so roleWindow's staged backfill writes its history entry.
func roleNode(id, role string) *Node {
	return &Node{ID: NodeID(prefixTestID(id)), Labels: []string{"Role"}, Properties: map[string]interface{}{
		"role": role, "valid_from": temporalTestBase, "valid_to": temporalTestBase.Add(24 * time.Hour),
	}}
}

// runSchemaStatement commits one schema statement's changes as the Cypher
// executor's ordinary DDL does: the statement's isolated view, its change
// applied there, then the staged runtime values.
func runSchemaStatement(t *testing.T, engine *BadgerEngine, change func(view *schemaStatementView)) {
	t.Helper()
	tx, view := beginTemporalDDLStatement(t, engine)
	change(view)
	require.NoError(t, tx.StageSchemaChanges())
	require.NoError(t, tx.Commit())
}

// createTemporalStatement creates one temporal constraint in its own statement,
// with the backfill its creation stages in that statement
// (MarkNewTemporalConstraintsForEngine).
func createTemporalStatement(t *testing.T, engine *BadgerEngine, c Constraint) {
	t.Helper()
	runSchemaStatement(t, engine, func(view *schemaStatementView) {
		require.NoError(t, view.GetSchema().AddConstraint(c))
		require.NoError(t, MarkNewTemporalConstraintsForEngine(view, view.GetSchema(), []Constraint{c}))
	})
}

// TestTemporalConstraintMarkSurvivesUnrelatedSchemaStatement: the mark a
// completing statement published stays with its constraint when a later,
// unrelated schema statement publishes its own snapshot, and the published
// index keeps answering from the key's history while the engine reports no
// completed rebuild.
func TestTemporalConstraintMarkSurvivesUnrelatedSchemaStatement(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()
	sm := engine.GetSchemaForNamespace("test")
	commitNodes(t, engine, currentFact("pre", int64(1), -2*time.Hour, 2*time.Hour))

	createTemporalStatement(t, engine, factWindow)
	complete, incomplete := temporalCompletenessMarks(sm, factWindow)
	require.True(t, complete, "the creating statement publishes the mark its staged backfill reviewed")
	require.False(t, incomplete)

	// An unrelated schema statement: it changes no constraint of the label.
	runSchemaStatement(t, engine, func(view *schemaStatementView) {
		require.NoError(t, view.GetSchema().AddCompositeIndex("person_name", "Person", []string{"name"}))
	})

	complete, incomplete = temporalCompletenessMarks(sm, factWindow)
	require.True(t, complete, "an unrelated schema statement must not drop an unchanged constraint's completeness mark")
	require.False(t, incomplete)
	require.True(t, sm.HasConstraintNamed(factWindow.Name))

	// The product consequence: with no completed rebuild behind the engine, the
	// overlap is still answered from the constraint's own key history.
	engine.temporalIndexReady.Store(false)
	require.True(t, engine.temporalIndexAuthoritative(sm, factWindow))
	scans := countTemporalScans(t)
	overlap, err := engine.BeginTransaction(t.Context(), OrdinaryBegin)
	require.NoError(t, err)
	_, err = overlap.CreateNode(t.Context(), currentFact("overlap", int64(1), -time.Hour, time.Hour))
	require.Error(t, err, "an interval overlapping the one written before the constraint must be rejected")
	require.NoError(t, overlap.Rollback())
	require.Zero(t, scans.Load(), "the preserved mark answers from the key's history without scanning the label")
}

// TestTemporalConstraintMarkRemovedOnlyWithItsOwnConstraint: a statement that
// drops one constraint removes exactly that constraint's marks, and the marks
// of a constraint it did not touch stay published.
func TestTemporalConstraintMarkRemovedOnlyWithItsOwnConstraint(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()
	sm := engine.GetSchemaForNamespace("test")
	commitNodes(t, engine,
		currentFact("pre", int64(1), -2*time.Hour, 2*time.Hour),
		roleNode("role-1", "captain"))

	createTemporalStatement(t, engine, factWindow)
	createTemporalStatement(t, engine, roleWindow)
	for _, c := range []Constraint{factWindow, roleWindow} {
		complete, incomplete := temporalCompletenessMarks(sm, c)
		require.Truef(t, complete, "%s is complete after its creating statement", c.Name)
		require.Falsef(t, incomplete, "%s carries no incomplete mark", c.Name)
	}

	// One statement drops the second constraint and declares an unrelated index.
	runSchemaStatement(t, engine, func(view *schemaStatementView) {
		require.NoError(t, view.GetSchema().DropConstraint(roleWindow.Name))
		require.NoError(t, view.GetSchema().AddCompositeIndex("person_name", "Person", []string{"name"}))
	})
	require.False(t, sm.HasConstraintNamed(roleWindow.Name))

	complete, incomplete := temporalCompletenessMarks(sm, roleWindow)
	require.False(t, complete, "the dropped constraint keeps no complete mark")
	require.False(t, incomplete, "the dropped constraint keeps no incomplete mark")
	complete, incomplete = temporalCompletenessMarks(sm, factWindow)
	require.True(t, complete, "the statement kept the mark of the constraint it did not touch")
	require.False(t, incomplete)

	engine.temporalIndexReady.Store(false)
	require.True(t, engine.temporalIndexAuthoritative(sm, factWindow))
	require.False(t, engine.temporalIndexAuthoritative(sm, roleWindow))
}

// TestTemporalIndexMarkTransferOwnsEachConstraintIdentity pins the transfer
// rule itself: a statement's own staged mark owns its constraint identity, an
// identity the statement left unchanged keeps the live mark, and a dropped
// identity keeps none.
func TestTemporalIndexMarkTransferOwnsEachConstraintIdentity(t *testing.T) {
	key := temporalIndexCompletenessKey(factWindow)
	live := NewSchemaManager()
	require.NoError(t, live.AddConstraint(factWindow))
	live.markTemporalIndexCreated(factWindow, true)

	// The statement re-creates the same identity: the mark it staged decides,
	// not the one the identity carried before.
	recreated := NewSchemaManager()
	require.NoError(t, recreated.AddConstraint(factWindow))
	recreated.markTemporalIndexCreated(factWindow, false)
	complete, incomplete := live.transferTemporalIndexMarksLocked(recreated)
	require.NotContains(t, complete, key, "the previous mark of the identity does not outlive the staged one")
	require.Contains(t, incomplete, key, "the statement's own mark publishes for its identity")

	// The statement leaves the identity unchanged: the live mark publishes.
	unchanged := NewSchemaManager()
	require.NoError(t, unchanged.AddConstraint(factWindow))
	complete, incomplete = live.transferTemporalIndexMarksLocked(unchanged)
	require.Contains(t, complete, key)
	require.NotContains(t, incomplete, key)

	// The statement drops the identity: none of its marks publish.
	dropped := NewSchemaManager()
	complete, incomplete = live.transferTemporalIndexMarksLocked(dropped)
	require.NotContains(t, complete, key)
	require.NotContains(t, incomplete, key)
}
