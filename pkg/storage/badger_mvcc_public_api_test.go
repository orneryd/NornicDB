package storage

import (
	"sort"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// mvccPublicAPIFixture creates an engine with two committed nodes and
// one committed edge, all bootstrapped through the standard write
// path so head records and primary keys are populated. Returns:
//
//   - engine
//   - node IDs and the edge ID
//   - the MVCC version stamped on the engine right after the bootstrap
//     commit (used as the floor for subsequent AppendNodeVersion /
//     AppendEdgeVersion calls below)
func mvccPublicAPIFixture(t *testing.T) (*MemoryEngine, NodeID, NodeID, EdgeID) {
	t.Helper()
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)

	const aliceID = NodeID("test:mvcc-alice")
	const bobID = NodeID("test:mvcc-bob")
	const edgeID = EdgeID("test:mvcc-edge")

	for _, n := range []*Node{
		{ID: aliceID, Labels: []string{"Person"}, Properties: map[string]any{"v": int64(1)}},
		{ID: bobID, Labels: []string{"Person"}, Properties: map[string]any{"v": int64(1)}},
	} {
		_, err := tx.CreateNode(n)
		require.NoError(t, err, "CreateNode(%q)", n.ID)
	}
	require.NoError(t, tx.CreateEdge(&Edge{
		ID: edgeID, StartNode: aliceID, EndNode: bobID, Type: "KNOWS",
		Properties: map[string]any{"v": int64(1)},
	}))
	require.NoError(t, tx.Commit())

	return engine, aliceID, bobID, edgeID
}

// AppendNodeTombstone is a low-level helper used by WAL replay /
// reconciliation paths: it writes a tombstone version record and
// flips the head pointer. It does NOT touch the primary nodeKey —
// that's the caller's responsibility (e.g. the commit loop pairs the
// tombstone with a primary-key delete; WAL replay rebuilds heads
// against an already-empty primary keyspace). The asserted contract
// here is exactly that: head bookkeeping is correct, and as-of reads
// at or after the tombstone version see "not found", but the primary
// key is left untouched by this method alone.
func TestMVCC_AppendNodeTombstone_FlipsHeadAndHidesAsOfRead(t *testing.T) {
	engine, aliceID, _, _ := mvccPublicAPIFixture(t)

	tombV := MVCCVersion{
		CommitTimestamp: time.Now().Add(time.Hour).UTC(),
		CommitSequence:  9_999_999,
	}
	require.NoError(t, engine.AppendNodeTombstone(aliceID, tombV))

	head, err := engine.GetNodeCurrentHead(aliceID)
	require.NoError(t, err)
	require.True(t, head.Tombstoned, "head must be tombstoned after AppendNodeTombstone")
	require.Equal(t, 0, head.Version.Compare(tombV))

	// As-of read at the tombstone version: ErrNotFound, per
	// GetNodeVisibleAt's "tombstoned head at or before requested
	// version → ErrNotFound" branch.
	got, err := engine.GetNodeVisibleAt(aliceID, tombV)
	require.ErrorIs(t, err, ErrNotFound)
	require.Nil(t, got)

	// As-of read AFTER the tombstone is also hidden.
	afterTomb := MVCCVersion{
		CommitTimestamp: tombV.CommitTimestamp.Add(time.Minute),
		CommitSequence:  tombV.CommitSequence + 1,
	}
	got, err = engine.GetNodeVisibleAt(aliceID, afterTomb)
	require.ErrorIs(t, err, ErrNotFound)
	require.Nil(t, got)
}

func TestMVCC_AppendEdgeTombstone_FlipsHeadAndHidesAsOfRead(t *testing.T) {
	engine, _, _, edgeID := mvccPublicAPIFixture(t)

	tombV := MVCCVersion{
		CommitTimestamp: time.Now().Add(time.Hour).UTC(),
		CommitSequence:  9_999_999,
	}
	require.NoError(t, engine.AppendEdgeTombstone(edgeID, tombV))

	head, err := engine.GetEdgeCurrentHead(edgeID)
	require.NoError(t, err)
	require.True(t, head.Tombstoned)
	require.Equal(t, 0, head.Version.Compare(tombV))

	got, err := engine.GetEdgeVisibleAt(edgeID, tombV)
	require.ErrorIs(t, err, ErrNotFound)
	require.Nil(t, got)
}

func TestMVCC_UpdateNodeCurrentHead_OverwritesVersion(t *testing.T) {
	engine, aliceID, _, _ := mvccPublicAPIFixture(t)

	original, err := engine.GetNodeCurrentHead(aliceID)
	require.NoError(t, err)
	require.False(t, original.Tombstoned)

	// Manually advance the head to a future version. The body record
	// for the new version doesn't exist; this method ONLY rewrites
	// the head pointer.
	newV := MVCCVersion{
		CommitTimestamp: original.Version.CommitTimestamp.Add(time.Minute),
		CommitSequence:  original.Version.CommitSequence + 100,
	}
	require.NoError(t, engine.UpdateNodeCurrentHead(aliceID, newV, false))

	updated, err := engine.GetNodeCurrentHead(aliceID)
	require.NoError(t, err)
	require.Equal(t, 0, updated.Version.Compare(newV))
	require.False(t, updated.Tombstoned)

	// Flip tombstoned via the same method.
	require.NoError(t, engine.UpdateNodeCurrentHead(aliceID, newV, true))
	updated, err = engine.GetNodeCurrentHead(aliceID)
	require.NoError(t, err)
	require.True(t, updated.Tombstoned)
}

func TestMVCC_UpdateEdgeCurrentHead_OverwritesVersion(t *testing.T) {
	engine, _, _, edgeID := mvccPublicAPIFixture(t)

	original, err := engine.GetEdgeCurrentHead(edgeID)
	require.NoError(t, err)

	newV := MVCCVersion{
		CommitTimestamp: original.Version.CommitTimestamp.Add(time.Minute),
		CommitSequence:  original.Version.CommitSequence + 100,
	}
	require.NoError(t, engine.UpdateEdgeCurrentHead(edgeID, newV, false))

	updated, err := engine.GetEdgeCurrentHead(edgeID)
	require.NoError(t, err)
	require.Equal(t, 0, updated.Version.Compare(newV))
	require.False(t, updated.Tombstoned)

	require.NoError(t, engine.UpdateEdgeCurrentHead(edgeID, newV, true))
	updated, err = engine.GetEdgeCurrentHead(edgeID)
	require.NoError(t, err)
	require.True(t, updated.Tombstoned)
}

func TestMVCC_BatchGetNodesLatestVisible(t *testing.T) {
	engine, aliceID, bobID, _ := mvccPublicAPIFixture(t)

	// Both present.
	got, err := engine.BatchGetNodesLatestVisible([]NodeID{aliceID, bobID})
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.NotNil(t, got[aliceID])
	require.NotNil(t, got[bobID])
	require.Equal(t, aliceID, got[aliceID].ID)
	require.Equal(t, bobID, got[bobID].ID)

	// Mix: present + missing. Missing IDs are silently dropped from
	// the returned map (per the public API contract); the function
	// does not return an error for them.
	got, err = engine.BatchGetNodesLatestVisible([]NodeID{aliceID, "test:nope"})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Contains(t, got, aliceID)
	require.NotContains(t, got, NodeID("test:nope"))

	// Empty input → empty result, no error.
	got, err = engine.BatchGetNodesLatestVisible(nil)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestMVCC_IterateLatestVisibleNodes(t *testing.T) {
	engine, aliceID, bobID, _ := mvccPublicAPIFixture(t)

	var seen []NodeID
	require.NoError(t, engine.IterateLatestVisibleNodes(func(n *Node) error {
		require.NotNil(t, n)
		seen = append(seen, n.ID)
		return nil
	}))
	sort.Slice(seen, func(i, j int) bool { return seen[i] < seen[j] })
	require.Equal(t, []NodeID{aliceID, bobID}, seen)

	// Yield can short-circuit by returning a non-nil error.
	stopErr := errExampleSentinel
	count := 0
	err := engine.IterateLatestVisibleNodes(func(n *Node) error {
		count++
		return stopErr
	})
	require.ErrorIs(t, err, stopErr)
	require.Equal(t, 1, count, "iteration must stop on first non-nil error")
}

func TestMVCC_IterateLatestVisibleEdges(t *testing.T) {
	engine, _, _, edgeID := mvccPublicAPIFixture(t)

	var seen []EdgeID
	require.NoError(t, engine.IterateLatestVisibleEdges(func(e *Edge) error {
		require.NotNil(t, e)
		seen = append(seen, e.ID)
		return nil
	}))
	require.Equal(t, []EdgeID{edgeID}, seen)

	// Short-circuit on non-nil yield error.
	stopErr := errExampleSentinel
	count := 0
	err := engine.IterateLatestVisibleEdges(func(e *Edge) error {
		count++
		return stopErr
	})
	require.ErrorIs(t, err, stopErr)
	require.Equal(t, 1, count)
}

func TestMVCC_LatestNodeMVCCVersionInTxn_ReturnsTopVersion(t *testing.T) {
	engine, aliceID, _, _ := mvccPublicAPIFixture(t)

	// Append two historical versions of alice. latestNodeMVCCVersionInTxn
	// must pick the one with the highest version, not the one written
	// most recently.
	older := MVCCVersion{CommitTimestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), CommitSequence: 1}
	newer := MVCCVersion{CommitTimestamp: time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC), CommitSequence: 2}

	// Write newer FIRST so write-order != version-order.
	require.NoError(t, engine.AppendNodeVersion(&Node{
		ID: aliceID, Labels: []string{"Person"},
		Properties: map[string]any{"phase": "newer"},
	}, newer))
	require.NoError(t, engine.AppendNodeVersion(&Node{
		ID: aliceID, Labels: []string{"Person"},
		Properties: map[string]any{"phase": "older"},
	}, older))

	require.NoError(t, engine.db.View(func(txn *badger.Txn) error {
		rec, ver, err := engine.latestNodeMVCCVersionInTxn(txn, aliceID)
		require.NoError(t, err)
		require.NotNil(t, rec.Node)
		require.False(t, rec.Tombstoned)
		require.Equal(t, "newer", rec.Node.Properties["phase"])
		require.Equal(t, 0, ver.Compare(newer),
			"latestNodeMVCCVersionInTxn must return the highest-versioned record")
		return nil
	}))
}

func TestMVCC_LatestEdgeMVCCVersionInTxn_ReturnsTopVersion(t *testing.T) {
	engine, _, _, edgeID := mvccPublicAPIFixture(t)

	older := MVCCVersion{CommitTimestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), CommitSequence: 1}
	newer := MVCCVersion{CommitTimestamp: time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC), CommitSequence: 2}

	require.NoError(t, engine.AppendEdgeVersion(&Edge{
		ID: edgeID, StartNode: "test:mvcc-alice", EndNode: "test:mvcc-bob",
		Type: "KNOWS", Properties: map[string]any{"phase": "newer"},
	}, newer))
	require.NoError(t, engine.AppendEdgeVersion(&Edge{
		ID: edgeID, StartNode: "test:mvcc-alice", EndNode: "test:mvcc-bob",
		Type: "KNOWS", Properties: map[string]any{"phase": "older"},
	}, older))

	require.NoError(t, engine.db.View(func(txn *badger.Txn) error {
		rec, ver, err := engine.latestEdgeMVCCVersionInTxn(txn, edgeID)
		require.NoError(t, err)
		require.NotNil(t, rec.Edge)
		require.False(t, rec.Tombstoned)
		require.Equal(t, "newer", rec.Edge.Properties["phase"])
		require.Equal(t, 0, ver.Compare(newer))
		return nil
	}))
}

// errExampleSentinel is shared between the iterator short-circuit
// tests so each one can use require.ErrorIs against the same value.
var errExampleSentinel = errSentinel("test mvcc iteration stop sentinel")

type errSentinel string

func (e errSentinel) Error() string { return string(e) }
