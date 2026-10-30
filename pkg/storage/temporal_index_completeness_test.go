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

	// Created over existing nodes: their entries are backfilled, so the index is complete.
	other := Constraint{Name: "role_window", Type: ConstraintTemporal, EntityType: ConstraintEntityNode, Label: "Role", Properties: []string{"role", "valid_from", "valid_to"}}
	commitNodes(t, engine, &Node{ID: NodeID(prefixTestID("role-1")), Labels: []string{"Role"}, Properties: map[string]interface{}{
		"role": "captain", "valid_from": temporalTestBase, "valid_to": temporalTestBase.Add(24 * time.Hour),
	}})
	require.NoError(t, sm.AddConstraint(other))
	require.NoError(t, MarkNewTemporalConstraintsForEngine(engine, sm, []Constraint{other}))
	require.True(t, engine.temporalIndexAuthoritative(sm, other), "nodes written before the constraint are backfilled")
	role1, err := engine.GetNode(NodeID(prefixTestID("role-1")))
	require.NoError(t, err)
	history, ok, err := engine.temporalKeyHistoryNodesLatest(context.Background(), "test", other, role1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, history, 1, "the backfill writes the pre-existing node's history entry")

	require.NoError(t, engine.RebuildTemporalIndexes(context.Background()))
	require.True(t, engine.temporalIndexAuthoritative(sm, other), "a rebuild indexes every node")
	require.True(t, engine.temporalIndexAuthoritative(sm, factWindow))

	// A constraint registered but not created through DDL marks (a lazily loaded
	// schema) is trusted only after a rebuild or a clean start.
	engine.temporalIndexReady.Store(false)
	require.NoError(t, sm.DropConstraint(other.Name))
	require.NoError(t, sm.AddConstraint(other))
	require.False(t, engine.temporalIndexAuthoritative(sm, other))
	require.NoError(t, engine.MarkCleanShutdown(context.Background()))
	clean, err := engine.ConsumeCleanShutdownMarker(context.Background())
	require.NoError(t, err)
	require.True(t, clean)
	require.True(t, engine.temporalIndexAuthoritative(sm, other), "a clean start certifies the derived indexes, the temporal history index included")
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
	require.NoError(t, tx.UpdateGraphNode(NodeUpdateRequest{After: temporalFact("f-7-0", int64(7), 100, 110), Policy: ClientNodeEdits}))
	_, err = tx.CreateNode(temporalFact("reuse-7", int64(7), 0, 10))
	require.NoError(t, err, "the window a rewritten committed node gave up is free")
	require.NoError(t, tx.Commit())

	// ... and a deleted one neither.
	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.DeleteGraphNode(NodeID(prefixTestID("f-8-0")), CascadeEdges))
	_, err = tx.CreateNode(temporalFact("reuse-8", int64(8), 0, 10))
	require.NoError(t, err, "the window of a deleted committed node is free")
	require.NoError(t, tx.Commit())

	require.Zero(t, scans.Load(), "validation with a complete index must not scan the label")
}

// A constraint created over existing nodes backfills their history entries:
// validation goes through the index at once and still finds overlaps with the
// nodes written before the constraint.
func TestTemporalIndexAuthoritative_ConstraintOverExistingNodesIsBackfilled(t *testing.T) {
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
	require.Zero(t, scans.Load(), "the backfilled index answers without scanning")
}

// While the index is not trusted (no rebuild, clean start or DDL marks yet),
// validation keeps the label scan and still finds overlaps.
func TestTemporalIndexAuthoritative_IncompleteIndexScans(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()
	sm := engine.GetSchemaForNamespace("test")
	commitNodes(t, engine, temporalFact("pre", int64(1), 0, 10))
	require.NoError(t, sm.AddConstraint(factWindow)) // registered without DDL marks: not trusted
	scans := countTemporalScans(t)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(temporalFact("overlap", int64(1), 5, 15))
	require.Error(t, err, "overlap with a node written before the constraint")
	require.NoError(t, tx.Rollback())
	require.NotZero(t, scans.Load(), "an untrusted index falls back to the label scan")

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

// The commit-time refresh of the temporal "current version" pointers must be
// linear in the transaction: it looped over every pending node per touched
// key and sorted the commit's pending writes for every key, so a commit of n
// temporal nodes was O(n^2 log n).
func TestTemporalCurrentPointerRefresh_ScalesLinearly(t *testing.T) {
	if testing.Short() {
		t.Skip("timing test")
	}
	run := func(n int) time.Duration {
		engine, cleanup := setupTestBadgerEngine(t)
		defer cleanup()
		sm := engine.GetSchemaForNamespace("test")
		require.NoError(t, sm.AddConstraint(factWindow))
		require.NoError(t, MarkNewTemporalConstraintsForEngine(engine, sm, []Constraint{factWindow}))
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		for i := 0; i < n; i++ {
			_, err := tx.CreateNode(temporalFact(fmt.Sprintf("s-%d", i), int64(i), -10, 10000))
			require.NoError(t, err)
		}
		start := time.Now()
		require.NoError(t, tx.Commit())
		return time.Since(start)
	}
	run(300)
	var samples [3][2]time.Duration
	for i := range samples {
		samples[i] = [2]time.Duration{run(1000), run(8000)}
		t.Logf("sample %d: commit of 1000 temporal nodes: %v, 8000: %v, ratio %.1f (linear ~8)", i+1, samples[i][0], samples[i][1], float64(samples[i][1])/float64(samples[i][0]))
	}
	require.Less(t, medianPairedScalingRatio(samples), 20.0)
}

// The current pointer follows the latest version valid now, whether that
// version was written by this commit or an earlier one.
func TestTemporalCurrentPointerRefresh_PointsAtCurrentVersion(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()
	sm := engine.GetSchemaForNamespace("test")
	require.NoError(t, sm.AddConstraint(factWindow))
	require.NoError(t, MarkNewTemporalConstraintsForEngine(engine, sm, []Constraint{factWindow}))
	now := time.Now().UTC()
	day := 24 * time.Hour
	at := func(id string, record int64, from, to time.Time, open bool) *Node {
		props := map[string]interface{}{"record_id": record, "valid_from": from}
		if !open {
			props["valid_to"] = to
		}
		return &Node{ID: NodeID(prefixTestID(id)), Labels: []string{"Fact"}, Properties: props}
	}
	isCurrent := func(id string) bool {
		n, err := engine.GetNode(NodeID(prefixTestID(id)))
		require.NoError(t, err)
		ok, err := engine.IsCurrentTemporalNode(n, now)
		require.NoError(t, err)
		return ok
	}
	// Record 1: a closed past version and the open current one, in one commit.
	commitNodes(t, engine, at("r1-v1", 1, now.Add(-10*day), now.Add(-5*day), false), at("r1-v2", 1, now.Add(-5*day), time.Time{}, true))
	require.True(t, isCurrent("r1-v2"))
	require.False(t, isCurrent("r1-v1"))

	// A later commit closes v2 and adds v3: the pointer moves to v3.
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.UpdateGraphNode(NodeUpdateRequest{After: at("r1-v2", 1, now.Add(-5*day), now.Add(-day), false), Policy: ClientNodeEdits}))
	_, err = tx.CreateNode(at("r1-v3", 1, now.Add(-day), time.Time{}, true))
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	require.True(t, isCurrent("r1-v3"))
	require.False(t, isCurrent("r1-v2"))
}
