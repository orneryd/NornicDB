// SPDX-License-Identifier: MIT
package storage

import (
	"errors"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestOversizedTransactionLeavesPersistentGraphIntact(t *testing.T) {
	dir := t.TempDir()
	open := func() *BadgerEngine {
		engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, LowMemory: true}))
		if engine != nil {
			fixtureowner.Own(t, engine)
		}
		require.NoError(t, err)
		return engine
	}
	engine := open()
	keepID := NodeID(prefixTestID("keep"))
	_, err := engine.CreateNode(&Node{ID: keepID, Labels: []string{"Keep"}, Properties: map[string]interface{}{"value": "original"}})
	require.NoError(t, err)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.DeleteGraphNode(keepID, CascadeEdges))
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
	require.NoError(t, peer.engine.materializeMVCCCommit(singleBatchWriter(peer.badgerTx), version, peer.operations))
	require.NoError(t, peer.flushBufferedWrites(singleBatchWriter(peer.badgerTx)))
}

func publishReservedPublication(t *testing.T, peer *BadgerTransaction) {
	t.Helper()
	require.NoError(t, peer.engine.db.commit(peer.badgerTx))
	peer.mu.Lock()
	peer.closeLocked(TxStatusCommitted, false, nil)
	peer.mu.Unlock()
}

func TestTransactionSnapshotReservedPeerMutation(t *testing.T) {
	for _, tc := range []struct {
		name                                       string
		node, deletion, lateUpdate                 bool
		bodyMessage, stageMessage, conflictMessage string
		beforeMessage, afterMessage                string
	}{
		{name: "EdgeUpdateConflictsAtCommit", bodyMessage: "reader must see the pre-publication edge body", conflictMessage: "reader cannot overwrite a peer update published after its physical snapshot", beforeMessage: "peer update must be independently committed", afterMessage: "failed reader must not alter latest truth"},
		{name: "NodeDeleteConflictsAtCommit", node: true, deletion: true, bodyMessage: "reader must see the pre-publication node body", conflictMessage: "reader cannot update a node deleted after its physical snapshot", beforeMessage: "peer node deletion must be independently committed", afterMessage: "failed reader must not resurrect the deleted node"},
		{name: "DeleteAllowsUpdateUntilCommit", deletion: true, lateUpdate: true, stageMessage: "snapshot write must not leak a hard missing-edge error after peer publication", conflictMessage: "peer deletion must reject the stale write at commit", beforeMessage: "peer deletion must be independently committed", afterMessage: "failed reader must not resurrect the deleted edge"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine := createTestBadgerEngine(t)
			source, _, edgeID := seedSnapshotAdjacencyGraph(t, engine, 0)
			peer, err := engine.BeginTransaction()
			require.NoError(t, err)
			t.Cleanup(func() { _ = peer.Rollback() })
			switch {
			case tc.node:
				require.NoError(t, peer.DeleteGraphNode(source, CascadeEdges))
			case tc.deletion:
				require.NoError(t, peer.DeleteEdge(edgeID))
			default:
				edge, err := peer.GetEdge(edgeID)
				require.NoError(t, err)
				edge.Properties = map[string]interface{}{"writer": "peer"}
				require.NoError(t, peer.UpdateEdge(edge))
			}
			stageReservedPublication(t, peer)
			reader, err := engine.BeginTransaction()
			require.NoError(t, err)
			t.Cleanup(func() { _ = reader.Rollback() })
			require.NoError(t, reader.SetNamespace(peer.namespace))
			var node *Node
			var edge *Edge
			if tc.node {
				node, err = reader.GetNode(source)
				require.NoError(t, err)
				require.Empty(t, node.Properties, tc.bodyMessage)
			} else {
				edge, err = reader.GetEdge(edgeID)
				require.NoError(t, err)
				require.Empty(t, edge.Properties, tc.bodyMessage)
			}
			stageUpdate := func() {
				if tc.node {
					node.Properties = map[string]interface{}{"writer": "reader"}
					require.NoError(t, reader.UpdateGraphNode(node, ClientNodeEdits), tc.stageMessage)
				} else {
					edge.Properties = map[string]interface{}{"writer": "reader"}
					require.NoError(t, reader.UpdateEdge(edge), tc.stageMessage)
				}
			}
			if !tc.lateUpdate {
				stageUpdate()
			}
			publishReservedPublication(t, peer)
			assertLatest := func(message string) {
				if tc.node {
					// Native bytes are published before normal cache maintenance.
					// Each observation needs its own fresh durable snapshot.
					fresh, err := engine.BeginTransaction()
					require.NoError(t, err)
					t.Cleanup(func() { _ = fresh.Rollback() })
					_, err = fresh.GetNode(source)
					require.ErrorIs(t, err, ErrNotFound, message)
				} else {
					latest, err := engine.GetEdge(edgeID)
					if tc.deletion {
						require.ErrorIs(t, err, ErrNotFound, message)
					} else {
						require.NoError(t, err)
						require.Equal(t, "peer", latest.Properties["writer"], message)
					}
				}
			}
			assertLatest(tc.beforeMessage)
			if tc.lateUpdate {
				stageUpdate()
			}
			require.ErrorIs(t, reader.Commit(), ErrConflict, tc.conflictMessage)
			assertLatest(tc.afterMessage)
		})
	}
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
