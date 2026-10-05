package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Every layer of the server's storage stack answers whether a relationship's
// end node is visible the way GetNode would, including staged async writes,
// and declines to answer while decay filtering can hide a node.
func TestRelationshipEndpointVisibleAcrossStack(t *testing.T) {
	badger, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer badger.Close()
	walBacking, err := NewWAL(t.TempDir(), nil)
	require.NoError(t, err)
	wal := NewWALEngine(badger, walBacking)
	async := NewAsyncEngine(wal, &AsyncEngineConfig{FlushInterval: time.Hour})
	namespaced := NewNamespacedEngine(async, "ns")

	_, err = namespaced.CreateNode(&Node{ID: "flushed", Labels: []string{"Doc"}})
	require.NoError(t, err)
	_, err = namespaced.CreateNode(&Node{ID: "deleted", Labels: []string{"Doc"}})
	require.NoError(t, err)
	require.NoError(t, async.Flush())
	require.NoError(t, namespaced.DeleteNode("deleted"))
	_, err = namespaced.CreateNode(&Node{ID: "staged", Labels: []string{"Doc"}})
	require.NoError(t, err)

	check := func(checker RelationshipEndpointChecker, id NodeID, visible bool) {
		t.Helper()
		got, answered := checker.RelationshipEndpointVisible(id)
		require.True(t, answered, id)
		require.Equal(t, visible, got, id)
	}
	check(namespaced, "flushed", true)
	check(namespaced, "staged", true)
	check(namespaced, "deleted", false)
	check(namespaced, "missing", false)
	check(async, "ns:staged", true)
	check(wal, "ns:flushed", true)
	badger.nodeCacheMu.Lock()
	clear(badger.nodeCache)
	badger.nodeCacheMu.Unlock()
	check(badger, "ns:flushed", true)
	check(badger, "ns:missing", false)

	// A node deleted on the engine is no longer visible, as for GetNode,
	// whether or not it was cached.
	_, err = badger.CreateNode(&Node{ID: "ns:gone", Labels: []string{"Doc"}})
	require.NoError(t, err)
	_, err = badger.GetNode("ns:gone")
	require.NoError(t, err)
	check(badger, "ns:gone", true)
	require.NoError(t, badger.DeleteNode("ns:gone"))
	check(badger, "ns:gone", false)
	_, err = badger.GetNode("ns:gone")
	require.ErrorIs(t, err, ErrNotFound)

	_, answered := badger.RelationshipEndpointVisible("")
	require.False(t, answered)
	badger.SetDecayEnabled(true)
	_, answered = namespaced.RelationshipEndpointVisible("flushed")
	require.False(t, answered)

	// Layers above an engine that can't answer don't answer either.
	plain := NewNamespacedEngine(nonCheckingEngine{Engine: badger}, "ns")
	_, answered = plain.RelationshipEndpointVisible("flushed")
	require.False(t, answered)
	_, answered = NewWALEngine(nonCheckingEngine{Engine: badger}, walBacking).RelationshipEndpointVisible("ns:flushed")
	require.False(t, answered)
	plainAsync := NewAsyncEngine(nonCheckingEngine{Engine: badger}, &AsyncEngineConfig{FlushInterval: time.Hour})
	defer plainAsync.Close()
	_, answered = plainAsync.RelationshipEndpointVisible("ns:flushed")
	require.False(t, answered)

	// A closed engine doesn't answer.
	badger.SetDecayEnabled(false)
	require.NoError(t, badger.Close())
	_, answered = badger.RelationshipEndpointVisible("ns:flushed")
	require.False(t, answered)
}

// nonCheckingEngine hides RelationshipEndpointChecker from the engine it wraps.
type nonCheckingEngine struct{ Engine }

// Inside a transaction the answer matches GetNode at the transaction's
// snapshot: its own writes and deletes first, then the committed node's MVCC
// head as the snapshot sees it, so a peer's later change or delete doesn't
// show. A head newer than the read version, and decay filtering, leave the
// answer to GetNode.
func TestBadgerTransactionRelationshipEndpointVisible(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer engine.Close()
	for _, id := range []NodeID{"ns:kept", "ns:gone", "ns:changed", "ns:peer-deleted", "ns:dropped"} {
		_, err = engine.CreateNode(&Node{ID: id, Labels: []string{"Doc"}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.DeleteNode("ns:dropped"))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, err = tx.GetNode("ns:kept")
	require.NoError(t, err)

	// A peer changes one node and deletes another after the snapshot.
	changed, err := engine.GetNode("ns:changed")
	require.NoError(t, err)
	changed.Properties = map[string]any{"v": int64(2)}
	require.NoError(t, engine.UpdateNode(changed))
	require.NoError(t, engine.DeleteNode("ns:peer-deleted"))

	_, err = tx.CreateNode(&Node{ID: "ns:new", Labels: []string{"Doc"}})
	require.NoError(t, err)
	require.NoError(t, tx.DeleteNode("ns:gone"))

	check := func(id NodeID, wantVisible, wantAnswered bool) {
		t.Helper()
		visible, answered := tx.RelationshipEndpointVisible(id)
		require.Equal(t, wantAnswered, answered, id)
		if !answered {
			return
		}
		require.Equal(t, wantVisible, visible, id)
		_, getErr := tx.GetNode(id)
		require.Equal(t, visible, getErr == nil, id)
	}
	check("ns:kept", true, true)
	check("ns:new", true, true)
	check("ns:gone", false, true)
	check("ns:changed", true, true)
	check("ns:peer-deleted", true, true)
	check("ns:dropped", false, true)
	check("ns:missing", false, false)
	check("", false, false)

	// The snapshot's prefix-scan cache answers for the nodes it holds.
	tx.snapshotPrefixNodeByID = map[NodeID]*Node{"ns:cached": {ID: "ns:cached"}}
	check("ns:cached", true, true)
	tx.snapshotPrefixNodeByID = nil

	engine.SetDecayEnabled(true)
	check("ns:kept", false, false)
	check("ns:new", true, true)
	engine.SetDecayEnabled(false)

	// A read version older than the node's head, or none, reads the node.
	readTS := tx.readTS
	tx.readTS = MVCCVersion{CommitTimestamp: time.Unix(1, 0).UTC()}
	check("ns:kept", false, false)
	tx.readTS = MVCCVersion{}
	check("ns:kept", false, false)
	tx.readTS = readTS

	require.NoError(t, tx.Rollback())
	check("ns:kept", false, false)
}
