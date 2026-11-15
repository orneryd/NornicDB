package storage

// The temporal rebuild family has three writers of the same derived state: the
// start-up rebuild (RebuildTemporalIndexes), the label backfill
// (backfillTemporalIndexForLabel) and the schema statement that creates the
// constraint (stageTemporalBackfillForLabel). Only the write owner differs: the
// maintenance pass writes the engine's transaction, the statement stages its
// buffered writes. The derivation (which history key a node gets, and which
// node is its key's current version) is one owner, so all three writers must
// derive byte-identical entries from the same stored population, and the reads
// served from their indexes must agree.
//
// These low-level fixtures deliberately include overlapping versions to exercise
// current-pointer winner selection. They bypass constraint validation, not the
// write owners. Public DDL rejection/adjacency is covered by the existing protocol
// tests; these fixtures do not establish that overlapping data is valid.
//
// These controls pin that seam at its fixtures: a scalar key, a second scalar
// key and their composite over one label (arbitrary constraint arity on one
// node), a closed version, an open version that supersedes it, and a closed
// version of another key. They pass before and after the write owners are
// separated, so they establish the derivation the separation must preserve
// rather than a behavior change of their own.

import (
	"context"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// temporalRebuildConstraintHistoryEntries is the number of history entries the
// fixture population derives for one temporal constraint: one per stored node.
const temporalRebuildConstraintHistoryEntries = 3

// temporalRebuildFixtures returns the stored population and the constraints
// every writer in this file derives from. All three values are built from one
// captured now, so the derived keys are identical for every writer.
func temporalRebuildFixtures() (time.Time, []*Node, []Constraint) {
	now := time.Now().UTC()
	node := func(id string, record int64, group string, from time.Time, to interface{}) *Node {
		return &Node{ID: NodeID(prefixTestID(id)), Labels: []string{"Fact"}, Properties: map[string]interface{}{
			"record_id":  record,
			"group":      group,
			"valid_from": from,
			"valid_to":   to,
		}}
	}
	nodes := []*Node{
		// The key's earlier version: superseded by window-current, still its
		// own history entry and the answer of an as-of read that predates the
		// later version.
		node("window-pre", 1, "a", now.Add(-2*time.Hour), now.Add(2*time.Hour)),
		// The key's open version: the current version of every one of the
		// node's constraint keys.
		node("window-current", 1, "a", now.Add(-30*time.Minute), nil),
		// A closed version: history only, never a current version.
		node("window-closed", 2, "b", now.Add(-3*time.Hour), now.Add(-time.Hour)),
	}
	constraints := []Constraint{
		{Name: "fixture_record", Type: ConstraintTemporal, EntityType: ConstraintEntityNode, Label: "Fact", Properties: []string{"record_id", "valid_from", "valid_to"}},
		{Name: "fixture_group", Type: ConstraintTemporal, EntityType: ConstraintEntityNode, Label: "Fact", Properties: []string{"group", "valid_from", "valid_to"}},
		{Name: "fixture_record_group", Type: ConstraintTemporal, EntityType: ConstraintEntityNode, Label: "Fact", Properties: []string{"record_id", "group", "valid_from", "valid_to"}},
	}
	return now, nodes, constraints
}

// temporalRebuildTestEngine opens an engine holding the fixture population,
// before any temporal constraint exists: each writer below starts from the same
// stored state and derives its own index from it.
func temporalRebuildTestEngine(t *testing.T, nodes []*Node) *BadgerEngine {
	t.Helper()
	engine, err := runtimeFixtureOwner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	commitNodes(t, engine, nodes...)
	return engine
}

// registerTemporalConstraints registers the constraints on the namespace's
// schema without deriving any entry: the state a rebuild pass starts from.
func registerTemporalConstraints(t *testing.T, engine *BadgerEngine, constraints []Constraint) {
	t.Helper()
	sm := engine.GetSchemaForNamespace("test")
	for _, c := range constraints {
		require.NoError(t, sm.AddConstraint(c))
	}
}

// temporalIndexContents reports the derived temporal index exactly as stored:
// every history and current-version key with its value.
func temporalIndexContents(t *testing.T, engine *BadgerEngine) map[string]string {
	t.Helper()
	out := map[string]string{}
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		for _, prefix := range []byte{prefixTemporalIndex, prefixTemporalHead} {
			it := txn.NewIterator(badgerPrefixIteratorOptions([]byte{prefix}))
			for it.Rewind(); it.ValidForPrefix([]byte{prefix}); it.Next() {
				key := string(it.Item().KeyCopy(nil))
				require.NoError(t, it.Item().Value(func(value []byte) error {
					out[key] = string(value)
					return nil
				}))
			}
			it.Close()
		}
		return nil
	}))
	return out
}

// temporalIndexByLabelBackfill creates the constraints as the engine's own
// maintenance pass does: the constraint is registered, then the label's stored
// nodes are backfilled in the engine's transaction.
func temporalIndexByLabelBackfill(t *testing.T, engine *BadgerEngine, constraints []Constraint) {
	t.Helper()
	registerTemporalConstraints(t, engine, constraints)
	require.NoError(t, MarkNewTemporalConstraintsForEngine(engine, engine.GetSchemaForNamespace("test"), constraints))
}

// temporalIndexByRepeatedLabelBackfill reruns the maintenance label backfill
// over the nodes already indexed, under the maintenance write it belongs to.
func temporalIndexByRepeatedLabelBackfill(t *testing.T, engine *BadgerEngine, label string) {
	t.Helper()
	ctx := context.Background()
	lifecycle, err := engine.beginMaintenanceWrite(ctx, nil)
	require.NoError(t, err)
	defer lifecycle.finish()
	require.NoError(t, engine.backfillTemporalIndexForLabel(ctx, engine, label))
}

// temporalIndexByStatementStage creates the constraints inside a schema
// statement: their backfill stages its entries in the statement's transaction
// and publishes them with it.
func temporalIndexByStatementStage(t *testing.T, engine *BadgerEngine, constraints []Constraint) {
	t.Helper()
	tx, view := beginTemporalDDLStatement(t, engine)
	t.Cleanup(func() { _ = tx.Rollback() })
	for _, c := range constraints {
		require.NoError(t, view.GetSchema().AddConstraint(c))
	}
	require.NoError(t, MarkNewTemporalConstraintsForEngine(view, view.GetSchema(), constraints))

	// The staged writer owns nothing of the engine's before the commit: one
	// history entry per node and constraint, and one current version per
	// constraint, all buffered in the statement.
	require.Equal(t, temporalRebuildConstraintHistoryEntries*len(constraints)+len(constraints), stagedTemporalKeys(t, tx),
		"the staged pass buffers one history and one current-version write per constraint")
	require.Zero(t, countKeysWithPrefix(t, engine, prefixTemporalIndex),
		"a statement's backfill writes no history entry of the engine's own before its commit")
	require.Zero(t, countKeysWithPrefix(t, engine, prefixTemporalHead))

	require.NoError(t, tx.StageSchemaChanges())
	require.NoError(t, tx.Commit())
}

// temporalIndexByRebuild creates the constraints on the registered schema and
// rebuilds the whole temporal index from the stored nodes.
func temporalIndexByRebuild(t *testing.T, engine *BadgerEngine, constraints []Constraint) {
	t.Helper()
	registerTemporalConstraints(t, engine, constraints)
	require.NoError(t, engine.RebuildTemporalIndexes(context.Background()))
}

// storedTemporalFixtureNode reads one fixture node back from the engine.
func storedTemporalFixtureNode(t *testing.T, engine *BadgerEngine, id string) *Node {
	t.Helper()
	node, err := engine.GetNode(context.Background(), NodeID(prefixTestID(id)))
	require.NoError(t, err)
	return node
}

// planEntries drains the entries a rebuild plan derives for node through the
// engine's physical view, writing none of them.
func planEntries(t *testing.T, engine *BadgerEngine, node *Node, asOf time.Time) []temporalRebuildEntry {
	t.Helper()
	var entries []temporalRebuildEntry
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		plan, err := engine.physicalTemporalRebuild(txn).plan(context.Background(), node, asOf)
		if err != nil {
			return err
		}
		for {
			entry, ok, err := plan.nextEntry(context.Background())
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
			entries = append(entries, entry)
		}
	}))
	return entries
}

// TestTemporalRebuildWritersDeriveOneIndex: the maintenance label backfill, a
// schema statement's staged backfill and the whole-store rebuild derive the
// same history and current-version entries, at arbitrary constraint arity, from
// the same stored population.
func TestTemporalRebuildWritersDeriveOneIndex(t *testing.T) {
	_, nodes, constraints := temporalRebuildFixtures()
	want := temporalRebuildConstraintHistoryEntries*len(constraints) + len(constraints)

	physicalEngine := temporalRebuildTestEngine(t, nodes)
	temporalIndexByLabelBackfill(t, physicalEngine, constraints)
	physical := temporalIndexContents(t, physicalEngine)
	require.Len(t, physical, want, "one history entry per node and constraint, one current version per constraint")

	stagedEngine := temporalRebuildTestEngine(t, nodes)
	temporalIndexByStatementStage(t, stagedEngine, constraints)
	staged := temporalIndexContents(t, stagedEngine)

	rebuiltEngine := temporalRebuildTestEngine(t, nodes)
	temporalIndexByRebuild(t, rebuiltEngine, constraints)
	rebuilt := temporalIndexContents(t, rebuiltEngine)

	require.Equal(t, physical, staged,
		"a statement's staged backfill writes exactly the entries the label backfill writes")
	require.Equal(t, physical, rebuilt,
		"the start-up rebuild derives exactly the entries the label backfill writes")

	// The derived state is a function of the stored population, not of a pass:
	// a second label backfill over the same nodes derives the same index.
	temporalIndexByRepeatedLabelBackfill(t, physicalEngine, "Fact")
	require.Equal(t, physical, temporalIndexContents(t, physicalEngine),
		"a repeated label backfill derives the same index")
}

// TestTemporalRebuildWritersServeTheSameReads: the index every writer derives
// answers the same as-of and current-version reads, including the fall back to
// history when the current version does not cover the requested time.
func TestTemporalRebuildWritersServeTheSameReads(t *testing.T) {
	now, nodes, constraints := temporalRebuildFixtures()
	ctx := context.Background()

	writers := []struct {
		name  string
		write func(*testing.T, *BadgerEngine, []Constraint)
	}{
		{"label backfill", temporalIndexByLabelBackfill},
		{"statement stage", temporalIndexByStatementStage},
		{"rebuild", temporalIndexByRebuild},
	}
	for _, writer := range writers {
		t.Run(writer.name, func(t *testing.T) {
			engine := temporalRebuildTestEngine(t, nodes)
			writer.write(t, engine, constraints)

			current, err := engine.GetTemporalNodeAsOfInNamespace(ctx, "test", "Fact", "record_id", int64(1), "valid_from", "valid_to", now)
			require.NoError(t, err)
			require.NotNil(t, current)
			require.Equal(t, NodeID(prefixTestID("window-current")), current.ID,
				"the later covering version is the key's current version")

			// The later version does not cover this time, so the read reaches
			// the earlier version through the key's history.
			earlier, err := engine.GetTemporalNodeAsOfInNamespace(ctx, "test", "Fact", "record_id", int64(1), "valid_from", "valid_to", now.Add(-2*time.Hour))
			require.NoError(t, err)
			require.NotNil(t, earlier)
			require.Equal(t, NodeID(prefixTestID("window-pre")), earlier.ID)

			closed, err := engine.GetTemporalNodeAsOfInNamespace(ctx, "test", "Fact", "record_id", int64(2), "valid_from", "valid_to", now.Add(-2*time.Hour))
			require.NoError(t, err)
			require.NotNil(t, closed)
			require.Equal(t, NodeID(prefixTestID("window-closed")), closed.ID)

			currentNode, err := engine.GetNode(ctx, NodeID(prefixTestID("window-current")))
			require.NoError(t, err)
			isCurrent, err := engine.IsCurrentTemporalNodeInNamespace(ctx, "test", currentNode, now)
			require.NoError(t, err)
			require.True(t, isCurrent, "the open version is its key's current version")
			earlierNode, err := engine.GetNode(ctx, NodeID(prefixTestID("window-pre")))
			require.NoError(t, err)
			isCurrent, err = engine.IsCurrentTemporalNodeInNamespace(ctx, "test", earlierNode, now)
			require.NoError(t, err)
			require.False(t, isCurrent, "the superseded version is not its key's current version")
		})
	}
}

// TestTemporalRebuildPlanYieldsOneConstraintAtATime: the derivation hands a
// writer one constraint's entry at a time, whatever the node's constraint
// count, and its current-version decision is the one the writers record.
func TestTemporalRebuildPlanYieldsOneConstraintAtATime(t *testing.T) {
	now, nodes, constraints := temporalRebuildFixtures()

	// Before any entry is written: one entry per constraint, keyed by the node
	// it indexes, and a pointer for every key a covering version holds.
	fresh := temporalRebuildTestEngine(t, nodes)
	registerTemporalConstraints(t, fresh, constraints)
	for _, node := range nodes {
		entries := planEntries(t, fresh, node, now)
		require.Len(t, entries, len(constraints), "one entry per constraint of the node")
		for _, entry := range entries {
			require.Greater(t, len(entry.history), len(node.ID), "history key includes its prefix and node ID")
			require.Equal(t, string(node.ID), string(entry.history[len(entry.history)-len(node.ID):]),
				"a history entry is keyed by the node it indexes")
			require.Equal(t, byte(0x00), entry.history[len(entry.history)-len(node.ID)-1])
		}
	}
	current := storedTemporalFixtureNode(t, fresh, "window-current")
	for _, entry := range planEntries(t, fresh, current, now) {
		require.NotNil(t, entry.current, "a covering version takes each key it shares")
		require.Equal(t, string(current.ID), string(entry.currentID))
	}
	closed := storedTemporalFixtureNode(t, fresh, "window-closed")
	for _, entry := range planEntries(t, fresh, closed, now) {
		require.Nil(t, entry.current, "a version outside the as-of time takes no pointer")
	}

	// Once the index records the keys' current versions, the derivation writes
	// no pointer again: the superseded version keeps the recorded one, and the
	// version already recorded needs no rewrite.
	indexed := temporalRebuildTestEngine(t, nodes)
	temporalIndexByLabelBackfill(t, indexed, constraints)
	for _, node := range nodes {
		for _, entry := range planEntries(t, indexed, storedTemporalFixtureNode(t, indexed, string(node.ID)), now) {
			require.Nil(t, entry.current,
				"a version the key's recorded current version does not replace takes no pointer")
		}
	}

	// A node the pass cannot index derives nothing: no label constraint, no
	// prefixed ID, no node.
	require.Empty(t, planEntries(t, indexed, &Node{ID: "test:other", Labels: []string{"Other"}}, now))
	require.Empty(t, planEntries(t, indexed, &Node{ID: "unprefixed", Labels: []string{"Fact"}}, now))
	require.Empty(t, planEntries(t, indexed, nil, now))
}

func TestTemporalRebuildPlanStopsAfterDiscoveryCancellation(t *testing.T) {
	asOf, nodes, constraints := temporalRebuildFixtures()
	engine := temporalRebuildTestEngine(t, nodes)
	registerTemporalConstraints(t, engine, constraints)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		plan, err := engine.physicalTemporalRebuild(txn).plan(ctx, nodes[0], asOf)
		require.NoError(t, err)
		cancel()
		entry, ok, err := plan.nextEntry(ctx)
		require.ErrorIs(t, err, context.Canceled)
		require.False(t, ok)
		require.Empty(t, entry.history)
		require.Empty(t, entry.current)
		return nil
	}))
}
