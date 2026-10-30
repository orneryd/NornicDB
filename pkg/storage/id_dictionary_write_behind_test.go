package storage

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Write-behind commits write ID entries while they are pending; the flush
// persists their keys and clears the marks, and leaves no transaction state
// behind (each would hold its Badger transaction). The entities resolve
// after a restart.
func TestWriteBehindFlushClearsPendingIDEntries(t *testing.T) {
	dir := t.TempDir()
	open := func() *BadgerEngine {
		engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, WriteBehind: true, WriteBehindInterval: time.Hour}))
		if engine != nil {
			fixtureowner.Own(t, engine)
		}
		require.NoError(t, err)
		return engine
	}
	engine := open()
	pendingEntries := func() int {
		count := 0
		for i := range engine.idDict.nodes {
			sh := &engine.idDict.nodes[i]
			sh.mu.RLock()
			count += len(sh.pending)
			sh.mu.RUnlock()
		}
		for i := range engine.idDict.edges {
			sh := &engine.idDict.edges[i]
			sh.mu.RLock()
			count += len(sh.pending)
			sh.mu.RUnlock()
		}
		return count
	}

	for i := 0; i < 20; i++ {
		bufferedCreate(t, engine, &Node{ID: NodeID(fmt.Sprintf("test:n%d", i)), Labels: []string{"L"}})
	}
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.CreateEdge(&Edge{ID: "test:e", StartNode: "test:n0", EndNode: "test:n1", Type: "R"}))
	require.NoError(t, tx.Commit())
	require.Greater(t, engine.writeBehind.PendingOps(), 0, "the commits stay buffered")
	require.Equal(t, 21, pendingEntries(), "buffered entries are pending until the flush")

	require.NoError(t, engine.FlushWriteBehind(nil))
	require.Zero(t, pendingEntries())
	engine.idDict.txnMu.Lock()
	require.Empty(t, engine.idDict.txnCounters)
	require.Empty(t, engine.idDict.txnCommitting)
	engine.idDict.txnMu.Unlock()

	require.NoError(t, engine.Close())
	engine = open()
	nodes, err := engine.GetNodesByLabel("L")
	require.NoError(t, err)
	require.Len(t, nodes, 20)
	edges, err := engine.GetOutgoingEdges("test:n0")
	require.NoError(t, err)
	require.Len(t, edges, 1)
}

// clearPending clears only the entries whose last writer is still the
// commit's: an entry written since keeps its mark for its own writer.
func TestClearPendingKeepsEntriesWrittenSince(t *testing.T) {
	engine := createTestBadgerEngine(t)
	d := engine.idDict
	newTxn := writeTxnFor(t, engine)
	first := newTxn()
	_, err := resolveOrAllocateNumericIDInTxn(d, first, NodeID("p:a"))
	require.NoError(t, err)
	oldNum, err := resolveOrAllocateNumericIDInTxn(d, first, NodeID("p:b"))
	require.NoError(t, err)
	d.flushTxnCounters(first)
	written := d.takeTxnCommitting(first)
	require.NoError(t, engine.db.commit(context.Background(), first, nil))

	// Reissue the same name as a distinct generation before completing the
	// first commit's publication. A mere adopter of the original generation
	// is already durable when the original writer commits.
	_, removed, ok := removeIDPair(&d.nodes, nodeForwardShard, nodeReverseShard, NodeID("p:b"), 0, idPairByString)
	require.True(t, ok)
	require.Equal(t, oldNum, removed)
	second := newTxn()
	newNum, err := resolveOrAllocateNumericIDInTxn(d, second, NodeID("p:b"))
	require.NoError(t, err)
	require.NotEqual(t, oldNum, newNum)
	d.clearPending(written)
	_, aPending := d.nodePendingWriter("p:a")
	_, bPending := d.nodePendingWriter("p:b")
	require.False(t, aPending)
	require.True(t, bPending, "p:b's new generation belongs to the second transaction")
	d.clearPending(nil)
	d.discardTxnCounters(second)
	second.Discard()
}
