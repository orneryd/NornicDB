package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRelationshipCacheRawWritesUseBeforeAndAfterKeys(t *testing.T) {
	for _, replay := range []bool{false, true} {
		t.Run(map[bool]string{false: "transaction", true: "replay"}[replay], func(t *testing.T) {
			var b *BadgerEngine
			if replay {
				b = newWriteBehindTestEngine(t, time.Hour)
				b.GapProbeStopWorker()
			} else {
				b = createTestBadgerEngine(t)
			}
			for _, id := range []NodeID{"test:a", "test:b", "test:c", "test:d"} {
				_, err := b.CreateNode(&Node{ID: id})
				require.NoError(t, err)
			}
			old := &Edge{ID: "test:raw-keyed", StartNode: "test:a", EndNode: "test:b", Type: "OLD", Properties: map[string]any{"v": int64(1)}}
			require.NoError(t, b.CreateEdge(old))
			_, err := b.GetOutgoingEdges(old.StartNode)
			require.NoError(t, err)
			_, err = b.GetIncomingEdges(old.EndNode)
			require.NoError(t, err)
			_, err = b.GetEdgesByType("OLD")
			require.NoError(t, err)
			_, err = b.GetEdgesByType("NEW")
			require.NoError(t, err)
			next := &Edge{ID: old.ID, StartNode: "test:c", EndNode: "test:d", Type: "NEW", Properties: map[string]any{"v": int64(2)}}
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, tx.SetNamespace("test"))
			require.NoError(t, tx.SetImplicit(replay))
			data, err := b.encodeEdgeInTxn(tx.staged(), "test", next)
			require.NoError(t, err)
			tx.bufferSet(edgeKey(old.ID), data)
			tx.bufferDelete(b.outgoingIndexKeyStringLookup(old.StartNode, old.ID))
			tx.bufferDelete(b.incomingIndexKeyStringLookup(old.EndNode, old.ID))
			tx.bufferDelete(b.edgeTypeIndexKeyStringLookup(old.Type, old.ID))
			out, err := b.outgoingIndexKeyString(tx.staged(), next.StartNode, next.ID)
			require.NoError(t, err)
			in, err := b.incomingIndexKeyString(tx.staged(), next.EndNode, next.ID)
			require.NoError(t, err)
			kind, err := b.edgeTypeIndexKeyString(tx.staged(), next.Type, next.ID)
			require.NoError(t, err)
			tx.bufferSet(out, adjacencyValueFromRecord(data))
			tx.bufferSet(in, adjacencyValueFromRecord(data))
			tx.bufferSet(kind, []byte{})
			require.Empty(t, tx.operations, "raw writer has no notification/body operation to repair its caches")
			require.NoError(t, tx.Commit())
			require.NoError(t, b.FlushWriteBehind())
			outgoing, err := b.GetOutgoingEdges(old.StartNode)
			require.NoError(t, err)
			require.Empty(t, outgoing)
			incoming, err := b.GetIncomingEdges(old.EndNode)
			require.NoError(t, err)
			require.Empty(t, incoming)
			typed, err := b.GetEdgesByType("OLD")
			require.NoError(t, err)
			require.Empty(t, typed)
			for _, read := range []func() ([]*Edge, error){func() ([]*Edge, error) { return b.GetOutgoingEdges(next.StartNode) }, func() ([]*Edge, error) { return b.GetIncomingEdges(next.EndNode) }, func() ([]*Edge, error) { return b.GetEdgesByType("NEW") }} {
				rows, err := read()
				require.NoError(t, err)
				require.Len(t, rows, 1)
				require.Equal(t, int64(2), rows[0].Properties["v"])
			}
			deletion, err := b.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, deletion.SetNamespace("test"))
			require.NoError(t, deletion.SetImplicit(replay))
			for _, key := range [][]byte{edgeKey(next.ID), out, in, kind} {
				deletion.bufferDelete(key)
			}
			require.Empty(t, deletion.operations)
			require.NoError(t, deletion.Commit())
			require.NoError(t, b.FlushWriteBehind())
			outgoing, err = b.GetOutgoingEdges(next.StartNode)
			require.NoError(t, err)
			require.Empty(t, outgoing)
			incoming, err = b.GetIncomingEdges(next.EndNode)
			require.NoError(t, err)
			require.Empty(t, incoming)
			typed, err = b.GetEdgesByType("NEW")
			require.NoError(t, err)
			require.Empty(t, typed)
			_, err = b.GetEdge(next.ID)
			require.ErrorIs(t, err, ErrNotFound)
		})
	}
}
