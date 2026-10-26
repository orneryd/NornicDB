package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// Only an ID the caller minted for the create skips the stored presence read.
// A stable ID shaped like a minted one still meets its hidden predecessor in
// the implicit transactions that skip the commit-time head reload, while a
// minted create keeps every check that needs no storage read.
func TestCreatePresenceReadSkipsOnlyMintedIDs(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		name := "sync"
		if buffered {
			name = "write-behind"
		}
		t.Run(name, func(t *testing.T) {
			engine, err := runtimeFixtureOwner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{
				DataDir: t.TempDir(), WriteBehind: buffered, WriteBehindInterval: time.Hour,
			}))
			require.NoError(t, err)
			t.Cleanup(func() { _ = engine.Close() })
			hiddenNode := NodeID("test:" + uuid.NewString())
			hiddenEdge := EdgeID("test:" + uuid.NewString())
			for _, node := range []*Node{
				{ID: "test:a", Labels: []string{"A"}},
				{ID: "test:b", Labels: []string{"B"}},
				{ID: hiddenNode, Labels: []string{"Original"}, Properties: map[string]any{"value": int64(1)}},
			} {
				_, err := engine.CreateNode(node)
				require.NoError(t, err)
			}
			require.NoError(t, engine.CreateEdge(&Edge{ID: hiddenEdge, StartNode: "test:a", EndNode: "test:b", Type: "LINK", Properties: map[string]any{"key": "old"}}))
			if buffered {
				require.NoError(t, engine.FlushWriteBehind())
			}
			require.NoError(t, engine.SetManualSuppression(StoredEntityNodeKind, string(hiddenNode), true))
			require.NoError(t, engine.SetManualSuppression(StoredEntityEdgeKind, string(hiddenEdge), true))

			begin := func() *BadgerTransaction {
				tx, err := engine.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, tx.SetNamespace("test"))
				require.NoError(t, tx.SetImplicit(true))
				require.NoError(t, tx.SetSkipCreateExistenceCheck(true))
				return tx
			}
			hiddenConflict := func(err error) bool {
				var conflict *HiddenEdgeIdentityConflictError
				return errors.Is(err, ErrAlreadyExists) || errors.As(err, &conflict)
			}

			// Stable IDs: the hidden predecessors reject the create.
			tx := begin()
			_, err = tx.CreateNode(&Node{ID: hiddenNode, Labels: []string{"Replacement"}})
			require.ErrorIs(t, err, ErrAlreadyExists)
			require.NoError(t, tx.Rollback())
			for _, bulk := range []bool{false, true} {
				tx = begin()
				edge := &Edge{ID: hiddenEdge, StartNode: "test:a", EndNode: "test:b", Type: "LINK", Properties: map[string]any{"key": "new"}}
				if bulk {
					err = tx.BulkCreateEdges([]*Edge{edge})
				} else {
					err = tx.CreateEdge(edge)
				}
				if err == nil {
					err = tx.Commit()
				} else {
					_ = tx.Rollback()
				}
				require.True(t, hiddenConflict(err), "bulk=%v: %v", bulk, err)
			}
			stored, err := engine.GetStoredNode(hiddenNode)
			require.NoError(t, err)
			require.Equal(t, []string{"Original"}, stored.Labels)
			storedEdge, err := engine.GetStoredEdge(hiddenEdge)
			require.NoError(t, err)
			require.Equal(t, "old", storedEdge.Properties["key"])

			// Minted IDs: the checks that need no storage read remain.
			tx = begin()
			minted := NodeID("test:" + uuid.NewString())
			_, err = tx.CreateMintedNode(&Node{ID: minted, Labels: []string{"Minted"}})
			require.NoError(t, err)
			_, err = tx.CreateMintedNode(&Node{ID: minted, Labels: []string{"Minted"}})
			require.ErrorIs(t, err, ErrAlreadyExists, "a pending create holds the ID")
			require.ErrorIs(t, tx.CreateMintedEdge(&Edge{ID: EdgeID("test:" + uuid.NewString()), StartNode: minted, EndNode: "test:missing", Type: "LINK"}), ErrNotFound, "endpoints are still checked")
			first := EdgeID("test:" + uuid.NewString())
			require.ErrorIs(t, tx.BulkCreateMintedEdges([]*Edge{
				{ID: first, StartNode: minted, EndNode: "test:a", Type: "LINK"},
				{ID: first, StartNode: minted, EndNode: "test:b", Type: "LINK"},
			}), ErrAlreadyExists, "an earlier edge of the batch holds the ID")
			second := EdgeID("test:" + uuid.NewString())
			require.NoError(t, tx.CreateMintedEdge(&Edge{ID: first, StartNode: minted, EndNode: "test:a", Type: "LINK"}))
			require.NoError(t, tx.BulkCreateMintedEdges([]*Edge{{ID: second, StartNode: "test:a", EndNode: minted, Type: "LINK"}}))
			require.NoError(t, tx.Commit())
			for _, id := range []EdgeID{first, second} {
				_, err := engine.GetEdge(id)
				require.NoError(t, err)
			}
			node, err := engine.GetNode(minted)
			require.NoError(t, err)
			require.Equal(t, []string{"Minted"}, node.Labels)
		})
	}
}

// WAL replay re-applies acknowledged creates through the engine's ordinary
// creates, never a minted one: an entry over a hidden predecessor, under a
// UUID-shaped ID like any minted one, is rejected and leaves it in place.
func TestWALReplayCreateMeetsHiddenPredecessor(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		engine, err := runtimeFixtureOwner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{
			DataDir: t.TempDir(), WriteBehind: buffered, WriteBehindInterval: time.Hour,
		}))
		require.NoError(t, err)
		t.Cleanup(func() { _ = engine.Close() })
		raw := uuid.NewString()
		hidden := NodeID("test:" + raw)
		_, err = engine.CreateNode(&Node{ID: hidden, Labels: []string{"Original"}})
		require.NoError(t, err)
		require.NoError(t, engine.FlushWriteBehind())
		require.NoError(t, engine.SetManualSuppression(StoredEntityNodeKind, string(hidden), true))

		data, err := encodeRecoveryPayload(WALNodeData{Node: &Node{ID: NodeID(raw), Labels: []string{"Replayed"}}})
		require.NoError(t, err)
		err = ReplayWALEntry(engine, WALEntry{Operation: OpCreateNode, Data: data, Database: "test"})
		require.ErrorIs(t, err, ErrAlreadyExists, "write-behind=%v", buffered)
		stored, err := engine.GetStoredNode(hidden)
		require.NoError(t, err)
		require.Equal(t, []string{"Original"}, stored.Labels)
	}
}
