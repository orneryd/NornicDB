// SPDX-License-Identifier: MIT
package storage

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestOversizedTransactionLeavesPersistentGraphIntact(t *testing.T) {
	dir := t.TempDir()
	open := func() *BadgerEngine {
		engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, LowMemory: true})
		require.NoError(t, err)
		return engine
	}
	engine := open()
	keepID := NodeID(prefixTestID("keep"))
	_, err := engine.CreateNode(&Node{ID: keepID, Labels: []string{"Keep"}, Properties: map[string]interface{}{"value": "original"}})
	require.NoError(t, err)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.DeleteNode(keepID))
	_, err = tx.CreateNode(&Node{ID: NodeID(prefixTestID("staged-0")), Labels: []string{"Staged"}})
	require.NoError(t, err)
	largeKeySuffix := strings.Repeat("k", 32<<10)
	var oversizedErr error
	for index := 0; index < 2000; index++ {
		oversizedErr = tx.badgerTx.Set([]byte(fmt.Sprintf("oversized-%04d-%s", index, largeKeySuffix)), []byte{1})
		if oversizedErr != nil {
			break
		}
	}
	if oversizedErr == nil {
		oversizedErr = tx.Commit()
	} else {
		require.NoError(t, tx.Rollback())
	}
	require.True(t, errors.Is(oversizedErr, badger.ErrTxnTooBig), "expected oversized transaction, got %v", oversizedErr)
	require.NoError(t, engine.Close())

	engine = open()
	defer engine.Close()
	kept, err := engine.GetNode(keepID)
	require.NoError(t, err)
	require.Equal(t, "original", kept.Properties["value"])
	_, err = engine.GetNode(NodeID(prefixTestID("staged-0")))
	require.ErrorIs(t, err, ErrNotFound)
	_, err = engine.CreateNode(&Node{ID: NodeID(prefixTestID("after")), Labels: []string{"Keep"}})
	require.NoError(t, err)
}

// stageReservedPublication pauses the existing Commit path immediately before
// Badger publishes its staged bytes. It deliberately uses the production
// allocation, materialization, and buffered-write stages so controls exercise
// the same logical-reservation window as the regression.
func stageReservedPublication(t *testing.T, peer *BadgerTransaction) {
	t.Helper()
	require.NoError(t, peer.validateSnapshotIsolationConflicts())
	version, err := peer.engine.allocateMVCCVersion(peer.badgerTx, peer.namespace, time.Now())
	require.NoError(t, err)
	peer.CommitVersion = version
	require.NoError(t, peer.engine.materializeMVCCCommitInTxn(peer.badgerTx, version, peer.operations))
	require.NoError(t, peer.flushBufferedWrites())
}

func publishReservedPublication(t *testing.T, peer *BadgerTransaction) {
	t.Helper()
	require.NoError(t, peer.badgerTx.Commit())
	peer.mu.Lock()
	peer.closeLocked(TxStatusCommitted, false, nil)
	peer.mu.Unlock()
}

func TestTransactionSnapshotReservedPeerEdgeUpdateConflictsAtCommit(t *testing.T) {
	engine := createTestBadgerEngine(t)
	_, _, edgeID := seedSnapshotAdjacencyGraph(t, engine, 0)

	peer, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = peer.Rollback() })
	peerEdge, err := peer.GetEdge(edgeID)
	require.NoError(t, err)
	peerEdge.Properties = map[string]interface{}{"writer": "peer"}
	require.NoError(t, peer.UpdateEdge(peerEdge))
	stageReservedPublication(t, peer)

	reader, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Rollback() })
	require.NoError(t, reader.SetNamespace(peer.namespace))
	readerEdge, err := reader.GetEdge(edgeID)
	require.NoError(t, err)
	require.Empty(t, readerEdge.Properties, "reader must see the pre-publication edge body")
	readerEdge.Properties = map[string]interface{}{"writer": "reader"}
	require.NoError(t, reader.UpdateEdge(readerEdge))

	publishReservedPublication(t, peer)
	latest, err := engine.GetEdge(edgeID)
	require.NoError(t, err)
	require.Equal(t, "peer", latest.Properties["writer"], "peer update must be independently committed")
	require.ErrorIs(t, reader.Commit(), ErrConflict, "reader cannot overwrite a peer update published after its physical snapshot")

	latest, err = engine.GetEdge(edgeID)
	require.NoError(t, err)
	require.Equal(t, "peer", latest.Properties["writer"], "failed reader must not alter latest truth")
}

func TestTransactionSnapshotReservedPeerNodeDeleteConflictsAtCommit(t *testing.T) {
	engine := createTestBadgerEngine(t)
	source, _, _ := seedSnapshotAdjacencyGraph(t, engine, 0)

	peer, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = peer.Rollback() })
	require.NoError(t, peer.DeleteNode(source))
	stageReservedPublication(t, peer)

	reader, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Rollback() })
	require.NoError(t, reader.SetNamespace(peer.namespace))
	readerNode, err := reader.GetNode(source)
	require.NoError(t, err)
	require.Empty(t, readerNode.Properties, "reader must see the pre-publication node body")
	readerNode.Properties = map[string]interface{}{"writer": "reader"}
	require.NoError(t, reader.UpdateNode(readerNode))

	publishReservedPublication(t, peer)
	// The barrier publishes native bytes before normal post-commit cache
	// maintenance. A fresh transaction independently observes durable truth.
	latestReader, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = latestReader.Rollback() })
	_, latestErr := latestReader.GetNode(source)
	require.ErrorIs(t, latestErr, ErrNotFound, "peer node deletion must be independently committed")
	require.ErrorIs(t, reader.Commit(), ErrConflict, "reader cannot update a node deleted after its physical snapshot")
	afterCommitReader, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = afterCommitReader.Rollback() })
	_, latestErr = afterCommitReader.GetNode(source)
	require.ErrorIs(t, latestErr, ErrNotFound, "failed reader must not resurrect the deleted node")
}

func TestTransactionPinnedSnapshotDiscardedOnRollbackAndCommit(t *testing.T) {
	engine := createTestBadgerEngine(t)

	rolledBack, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NotNil(t, rolledBack.snapshotTx)
	require.NoError(t, rolledBack.Rollback())
	require.Nil(t, rolledBack.snapshotTx)

	committed, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NotNil(t, committed.snapshotTx)
	require.NoError(t, committed.Commit())
	require.Nil(t, committed.snapshotTx)
}

func TestTransactionSnapshotReservedPeerDeleteAllowsUpdateUntilCommit(t *testing.T) {
	engine := createTestBadgerEngine(t)
	_, _, edgeID := seedSnapshotAdjacencyGraph(t, engine, 0)

	peer, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = peer.Rollback() })
	require.NoError(t, peer.DeleteEdge(edgeID))
	stageReservedPublication(t, peer)

	reader, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Rollback() })
	require.NoError(t, reader.SetNamespace(peer.namespace))
	readerEdge, err := reader.GetEdge(edgeID)
	require.NoError(t, err)
	require.Empty(t, readerEdge.Properties)

	publishReservedPublication(t, peer)
	_, err = engine.GetEdge(edgeID)
	require.ErrorIs(t, err, ErrNotFound, "peer deletion must be independently committed")
	readerEdge.Properties = map[string]interface{}{"writer": "reader"}
	require.NoError(t, reader.UpdateEdge(readerEdge), "snapshot write must not leak a hard missing-edge error after peer publication")
	require.ErrorIs(t, reader.Commit(), ErrConflict, "peer deletion must reject the stale write at commit")
	_, err = engine.GetEdge(edgeID)
	require.ErrorIs(t, err, ErrNotFound, "failed reader must not resurrect the deleted edge")
}
