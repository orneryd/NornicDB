package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// An unreadable stored relationship is a failed read, not an absent row.
// Only this test's isolated engine is modified; no production damage mechanism
// is assumed. Warm reads must not make a later scan report partial success.
func TestRelationshipStreamsReportUnreadableStoredRecords(t *testing.T) {
	engine := newTestMemoryEngine(t).BadgerEngine
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := engine.CreateNode(t.Context(), &Node{ID: id})
		require.NoError(t, err)
	}
	for _, id := range []EdgeID{"test:a-good", "test:z-bad"} {
		require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: id, StartNode: "test:a", EndNode: "test:b", Type: "LINK", Properties: map[string]interface{}{"text": "orchard"}}))
	}
	var warm int
	require.NoError(t, engine.StreamEdges(t.Context(), func(*Edge) error { warm++; return nil }))
	require.Equal(t, 2, warm)
	require.NoError(t, engine.withUpdate(t.Context(), updateRequest{}, func(txn *badger.Txn) error {
		return txn.Set(edgeKey("test:z-bad"), []byte{0xff, 0, 0x13, 0x37, 0x42})
	}))
	snapshot, err := OpenReadSnapshot(t.Context(), engine)
	require.NoError(t, err)
	t.Cleanup(snapshot.Close)
	namespace := NewNamespacedEngine(engine, "test")
	readers := []struct {
		name string
		read func(context.Context, func(*Edge) error) error
	}{
		{"native", engine.StreamEdges},
		{"fallback", func(ctx context.Context, visit func(*Edge) error) error {
			return StreamEdgesWithFallback(ctx, engine, StreamChunkSize, visit)
		}},
		{"namespace", func(ctx context.Context, visit func(*Edge) error) error {
			return StreamEdgesWithFallback(ctx, namespace, StreamChunkSize, visit)
		}},
		{"snapshot", snapshot.StreamEdges},
	}
	for _, reader := range readers {
		t.Run(reader.name, func(t *testing.T) {
			var visited []EdgeID
			err := reader.read(t.Context(), func(edge *Edge) error { visited = append(visited, edge.ID); return nil })
			require.ErrorContains(t, err, "unexpected format", "partial population must not be reported as a successful scan")
			require.NotContains(t, visited, EdgeID("test:z-bad"))
		})
	}
	entityReader, ok := snapshot.(EntityReadSnapshot)
	require.True(t, ok)
	edge, err := entityReader.ReadEdge(t.Context(), "test:z-bad")
	require.ErrorContains(t, err, "unexpected format")
	require.Nil(t, edge)
	// Unrelated visitor and cancellation errors retain their own identity.
	require.NoError(t, engine.StreamEdges(t.Context(), func(*Edge) error { return ErrIterationStopped }))
	require.NoError(t, snapshot.StreamEdges(t.Context(), func(*Edge) error { return ErrIterationStopped }))
	failure := errors.New("consumer stopped")
	require.ErrorIs(t, engine.StreamEdges(t.Context(), func(*Edge) error { return failure }), failure)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, engine.StreamEdges(canceled, func(*Edge) error { t.Fatal("canceled read emitted a row"); return nil }), context.Canceled)
}

func TestRelationshipStreamsUseBufferedReplacementBeforeStoredBody(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := engine.CreateNode(t.Context(), &Node{ID: id})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: "test:edge", StartNode: "test:a", EndNode: "test:b", Type: "LINK", Properties: map[string]interface{}{"text": "old"}}))
	require.NoError(t, engine.FlushWriteBehind(t.Context()))
	before, err := engine.GetEdge(t.Context(), "test:edge")
	require.NoError(t, err)
	after := CopyEdge(before)
	after.Properties["text"] = "buffered"
	bufferedWrite(t, engine, func(tx *BadgerTransaction) error {
		return tx.UpdateEdge(t.Context(), EdgeUpdateRequest{Before: before, After: after})
	})
	require.Positive(t, engine.writeBehind.PendingOps(), "the replacement must remain acknowledged but unflushed")
	require.NoError(t, engine.withUpdate(t.Context(), updateRequest{}, func(txn *badger.Txn) error { return txn.Set(edgeKey("test:edge"), []byte{0xff}) }))
	snapshot, err := OpenReadSnapshot(t.Context(), engine)
	require.NoError(t, err)
	t.Cleanup(snapshot.Close)
	for _, read := range []struct {
		name string
		call func(context.Context, func(*Edge) error) error
	}{{"live", engine.StreamEdges}, {"snapshot", snapshot.StreamEdges}} {
		t.Run(read.name, func(t *testing.T) {
			var rows []*Edge
			require.NoError(t, read.call(t.Context(), func(edge *Edge) error { rows = append(rows, edge); return nil }))
			require.Len(t, rows, 1)
			require.Equal(t, "buffered", rows[0].Properties["text"])
		})
	}
}
