package storage

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestPendingCandidatesReadHoldRetries(t *testing.T) {
	e := newTestBadgerEngineForPending(t)
	_, err := e.CreateNode(pendingCandidateNode(0))
	require.NoError(t, err)
	require.True(t, e.db.oracle.holdReads(time.Second))
	defer e.db.oracle.releaseReads()
	require.Nil(t, e.FindNodeNeedingEmbedding(), "restore read hold is a temporary lookup failure")
	e.db.oracle.releaseReads()
	node := e.FindNodeNeedingEmbedding()
	require.NotNil(t, node, "failed initial catalog load must be retried after the read hold")
	require.Equal(t, pendingCandidateNode(0).ID, node.ID)
}

func TestPendingCandidatesUpsertMissing(t *testing.T) {
	e := newTestBadgerEngineForPending(t)
	require.Nil(t, e.FindNodeNeedingEmbedding())
	require.NoError(t, e.UpdateNode(NodeUpdateRequest{After: pendingCandidateNode(0)}))
	node := e.FindNodeNeedingEmbedding()
	require.NotNil(t, node, "upsert's insert branch must publish its durable marker into the loaded cache")
	require.Equal(t, pendingCandidateNode(0).ID, node.ID)
}

func TestPendingCandidatesStaleMarkerDrain(t *testing.T) {
	for _, kind := range []string{"system", "missing", "corrupt", "ineligible"} {
		t.Run(kind, func(t *testing.T) {
			e := newTestBadgerEngineForPending(t)
			require.Nil(t, e.FindNodeNeedingEmbedding())
			staleID := NodeID("test:a-stale")
			if kind == "system" {
				staleID = "system:a-stale"
			}
			// Model a durable marker left by old data or interrupted cleanup.
			// It sorts before the live node, so returning nil would strand work.
			require.NoError(t, e.withUpdate(updateRequest{}, func(txn *badger.Txn) error {
				if kind == "corrupt" {
					if err := txn.Set(nodeKey(staleID), []byte("invalid node body")); err != nil {
						return err
					}
				}
				if kind == "ineligible" {
					node := &Node{ID: staleID, Labels: []string{"_Internal"}, Properties: map[string]any{}}
					value, _, err := e.encodeNode(txn, node)
					if err != nil {
						return err
					}
					if err := txn.Set(nodeKey(staleID), value); err != nil {
						return err
					}
				}
				return e.setPendingEmbeddingInTxn(txn, staleID)
			}))
			_, err := e.CreateNode(pendingCandidateNode(0))
			require.NoError(t, err)
			node := e.FindNodeNeedingEmbedding()
			require.NotNil(t, node, "stale marker cleanup must continue to the live candidate")
			require.Equal(t, pendingCandidateNode(0).ID, node.ID)
			require.NoError(t, e.withView(func(txn *badger.Txn) error {
				_, err := txn.Get(pendingEmbedKey(staleID))
				require.ErrorIs(t, err, badger.ErrKeyNotFound, "cleanup must remove the durable stale marker")
				return nil
			}))
			e.MarkNodeEmbedded(node.ID)
			require.Nil(t, e.FindNodeNeedingEmbedding())
		})
	}
}
