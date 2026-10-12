package storage

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var temporalTestBase = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func temporalFact(id string, record interface{}, fromDay, toDay int) *Node {
	return &Node{ID: NodeID(prefixTestID(id)), Labels: []string{"Fact"}, Properties: map[string]interface{}{
		"record_id":  record,
		"valid_from": temporalTestBase.Add(time.Duration(fromDay) * 24 * time.Hour),
		"valid_to":   temporalTestBase.Add(time.Duration(toDay) * 24 * time.Hour),
	}}
}

var factWindow = Constraint{
	Name: "fact_window", Type: ConstraintTemporal, EntityType: ConstraintEntityNode,
	Label: "Fact", Properties: []string{"record_id", "valid_from", "valid_to"},
}

func commitNodes(t *testing.T, engine *BadgerEngine, nodes ...*Node) {
	t.Helper()
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	for _, n := range nodes {
		_, err := tx.CreateNode(n)
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())
}

func countTemporalScans(t *testing.T) *atomic.Int64 {
	t.Helper()
	var scans atomic.Int64
	restore := setTemporalConstraintScanHook(func() { scans.Add(1) })
	t.Cleanup(restore)
	return &scans
}

func TestTemporalIndexAuthoritative_Lifecycle(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()
	sm := engine.GetSchemaForNamespace("test")

	// Created on an empty label: complete from the start.
	require.NoError(t, sm.AddConstraint(factWindow))
	require.False(t, engine.temporalIndexAuthoritative(sm, factWindow), "a registered constraint is not trusted before it is marked")
	require.NoError(t, MarkNewTemporalConstraintsForEngine(engine, sm, []Constraint{factWindow}))
	require.True(t, engine.temporalIndexAuthoritative(sm, factWindow))

	// Created over existing nodes: incomplete until a rebuild.
	other := Constraint{Name: "role_window", Type: ConstraintTemporal, EntityType: ConstraintEntityNode, Label: "Role", Properties: []string{"role", "valid_from", "valid_to"}}
	commitNodes(t, engine, &Node{ID: NodeID(prefixTestID("role-1")), Labels: []string{"Role"}, Properties: map[string]interface{}{
		"role": "captain", "valid_from": temporalTestBase, "valid_to": temporalTestBase.Add(24 * time.Hour),
	}})
	require.NoError(t, sm.AddConstraint(other))
	require.NoError(t, MarkNewTemporalConstraintsForEngine(engine, sm, []Constraint{other}))
	require.False(t, engine.temporalIndexAuthoritative(sm, other), "nodes written before the constraint have no history entries")

	require.NoError(t, engine.RebuildTemporalIndexes(context.Background()))
	require.True(t, engine.temporalIndexAuthoritative(sm, other), "a rebuild indexes every node")
	require.True(t, engine.temporalIndexAuthoritative(sm, factWindow))

	// Dropped and re-created over existing nodes after the rebuild: incomplete again.
	require.NoError(t, sm.DropConstraint(other.Name))
	require.NoError(t, sm.AddConstraint(other))
	require.NoError(t, MarkNewTemporalConstraintsForEngine(engine, sm, []Constraint{other}))
	require.False(t, engine.temporalIndexAuthoritative(sm, other))
}

// With a complete index, transactional NO OVERLAP validation reads the
// history of the node's own key and finds every overlap the label scan
// found, without scanning the label (#1126).
func TestTemporalIndexAuthoritative_TransactionUsesKeyHistory(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()
	sm := engine.GetSchemaForNamespace("test")
	require.NoError(t, sm.AddConstraint(factWindow))
	require.NoError(t, MarkNewTemporalConstraintsForEngine(engine, sm, []Constraint{factWindow}))

	var committed []*Node
	for r := 0; r < 50; r++ {
		committed = append(committed, temporalFact(fmt.Sprintf("f-%d-0", r), int64(r), 0, 10), temporalFact(fmt.Sprintf("f-%d-1", r), int64(r), 10, 20))
	}
	commitNodes(t, engine, committed...)
	scans := countTemporalScans(t)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(temporalFact("overlap", int64(3), 5, 15))
	require.Error(t, err, "overlap with a committed window of the same key")
	_, err = tx.CreateNode(temporalFact("numeric-overlap", float64(4), 5, 7))
	require.Error(t, err, "a numerically equal key of another type is the same key")
	_, err = tx.CreateNode(temporalFact("other-key", int64(999), 5, 15))
	require.NoError(t, err, "another key does not overlap")
	_, err = tx.CreateNode(temporalFact("after", int64(3), 20, 30))
	require.NoError(t, err, "an adjacent window does not overlap")
	require.NoError(t, tx.Commit())

	// A committed node the transaction moves away no longer blocks its old window.
	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.UpdateNode(temporalFact("f-7-0", int64(7), 100, 110)))
	_, err = tx.CreateNode(temporalFact("reuse-7", int64(7), 0, 10))
	require.NoError(t, err, "the window a rewritten committed node gave up is free")
	require.NoError(t, tx.Commit())

	// ... and a deleted one neither.
	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.DeleteNode(NodeID(prefixTestID("f-8-0"))))
	_, err = tx.CreateNode(temporalFact("reuse-8", int64(8), 0, 10))
	require.NoError(t, err, "the window of a deleted committed node is free")
	require.NoError(t, tx.Commit())

	require.Zero(t, scans.Load(), "validation with a complete index must not scan the label")
}

// While the index is incomplete (constraint created over existing nodes),
// validation keeps the label scan and still finds overlaps with nodes the
// index does not list; after a rebuild the index finds them.
func TestTemporalIndexAuthoritative_IncompleteIndexScans(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()
	sm := engine.GetSchemaForNamespace("test")
	commitNodes(t, engine, temporalFact("pre", int64(1), 0, 10))
	require.NoError(t, sm.AddConstraint(factWindow))
	require.NoError(t, MarkNewTemporalConstraintsForEngine(engine, sm, []Constraint{factWindow}))
	scans := countTemporalScans(t)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(temporalFact("overlap", int64(1), 5, 15))
	require.Error(t, err, "overlap with a node written before the constraint")
	require.NoError(t, tx.Rollback())
	require.NotZero(t, scans.Load(), "an incomplete index falls back to the label scan")

	require.NoError(t, engine.RebuildTemporalIndexes(context.Background()))
	before := scans.Load()
	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(temporalFact("overlap2", int64(1), 5, 15))
	require.Error(t, err, "after the rebuild the index lists the pre-existing node")
	require.NoError(t, tx.Rollback())
	require.Equal(t, before, scans.Load())
}

// The engine's direct-write path checks against the key's history; with a
// complete index a key without history no longer scans the label.
func TestTemporalIndexAuthoritative_EngineWriteNewKeyDoesNotScan(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()
	sm := engine.GetSchemaForNamespace("test")
	require.NoError(t, sm.AddConstraint(factWindow))
	require.NoError(t, MarkNewTemporalConstraintsForEngine(engine, sm, []Constraint{factWindow}))
	scans := countTemporalScans(t)

	for r := 0; r < 20; r++ {
		_, err := engine.CreateNode(temporalFact(fmt.Sprintf("e-%d", r), int64(r), 0, 10))
		require.NoError(t, err)
	}
	_, err := engine.CreateNode(temporalFact("e-overlap", float64(5), 5, 15))
	require.Error(t, err, "overlap through the history index, across numeric types")
	require.Zero(t, scans.Load())
}
