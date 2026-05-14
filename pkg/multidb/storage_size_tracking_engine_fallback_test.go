package multidb

import (
	"context"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// nonStreamingEngine wraps storage.Engine but explicitly does NOT
// implement storage.StreamingEngine — used to drive the fallback
// branches in size_tracking_engine.Stream*.
type nonStreamingEngine struct {
	storage.Engine
}

func newNonStreamingEngine(t *testing.T) *nonStreamingEngine {
	t.Helper()
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	return &nonStreamingEngine{Engine: base.BadgerEngine}
}

// Confirm Stream* methods aren't promoted from BadgerEngine through
// our wrapper: BadgerEngine doesn't implement StreamingEngine, so
// nonStreamingEngine doesn't either.
var _ storage.Engine = (*nonStreamingEngine)(nil)

func TestSizeTrackingEngine_StreamNodes_FallbackPath(t *testing.T) {
	inner := newNonStreamingEngine(t)
	for i := 0; i < 3; i++ {
		_, err := inner.Engine.CreateNode(&storage.Node{
			ID:     storage.NodeID("tenant:" + string(rune('a'+i))),
			Labels: []string{"L"},
		})
		require.NoError(t, err)
	}

	wrapped := newSizeTrackingEngine(inner, &DatabaseManager{}, "tenant")
	streamer, ok := wrapped.(storage.StreamingEngine)
	require.True(t, ok, "size-tracking wrapper must satisfy StreamingEngine")

	var seen []storage.NodeID
	require.NoError(t, streamer.StreamNodes(context.Background(), func(node *storage.Node) error {
		seen = append(seen, node.ID)
		return nil
	}))
	require.Len(t, seen, 3)
}

func TestSizeTrackingEngine_StreamEdges_FallbackPath(t *testing.T) {
	inner := newNonStreamingEngine(t)
	for _, id := range []storage.NodeID{"tenant:a", "tenant:b"} {
		_, err := inner.Engine.CreateNode(&storage.Node{ID: id, Labels: []string{"L"}})
		require.NoError(t, err)
	}
	require.NoError(t, inner.Engine.CreateEdge(&storage.Edge{
		ID: "tenant:e1", StartNode: "tenant:a", EndNode: "tenant:b", Type: "REL",
	}))

	wrapped := newSizeTrackingEngine(inner, &DatabaseManager{}, "tenant")
	streamer := wrapped.(storage.StreamingEngine)

	var seen []storage.EdgeID
	require.NoError(t, streamer.StreamEdges(context.Background(), func(e *storage.Edge) error {
		seen = append(seen, e.ID)
		return nil
	}))
	require.Equal(t, []storage.EdgeID{"tenant:e1"}, seen)
}

func TestSizeTrackingEngine_StreamEdges_FallbackContextCancel(t *testing.T) {
	inner := newNonStreamingEngine(t)
	for _, id := range []storage.NodeID{"tenant:a", "tenant:b"} {
		_, err := inner.Engine.CreateNode(&storage.Node{ID: id, Labels: []string{"L"}})
		require.NoError(t, err)
	}
	for _, id := range []storage.EdgeID{"tenant:e1", "tenant:e2"} {
		require.NoError(t, inner.Engine.CreateEdge(&storage.Edge{
			ID: id, StartNode: "tenant:a", EndNode: "tenant:b", Type: "REL",
		}))
	}

	wrapped := newSizeTrackingEngine(inner, &DatabaseManager{}, "tenant")
	streamer := wrapped.(storage.StreamingEngine)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := streamer.StreamEdges(ctx, func(e *storage.Edge) error {
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestSizeTrackingEngine_StreamEdges_FallbackStopOnIterationStopped(t *testing.T) {
	inner := newNonStreamingEngine(t)
	for _, id := range []storage.NodeID{"tenant:a", "tenant:b"} {
		_, err := inner.Engine.CreateNode(&storage.Node{ID: id, Labels: []string{"L"}})
		require.NoError(t, err)
	}
	for _, id := range []storage.EdgeID{"tenant:e1", "tenant:e2", "tenant:e3"} {
		require.NoError(t, inner.Engine.CreateEdge(&storage.Edge{
			ID: id, StartNode: "tenant:a", EndNode: "tenant:b", Type: "REL",
		}))
	}

	wrapped := newSizeTrackingEngine(inner, &DatabaseManager{}, "tenant")
	streamer := wrapped.(storage.StreamingEngine)

	count := 0
	err := streamer.StreamEdges(context.Background(), func(e *storage.Edge) error {
		count++
		return storage.ErrIterationStopped
	})
	require.NoError(t, err, "ErrIterationStopped must be swallowed in fallback")
	require.Equal(t, 1, count)
}

func TestSizeTrackingEngine_StreamNodeChunks_Fallback(t *testing.T) {
	inner := newNonStreamingEngine(t)
	for i := 0; i < 5; i++ {
		_, err := inner.Engine.CreateNode(&storage.Node{
			ID:     storage.NodeID("tenant:" + string(rune('a'+i))),
			Labels: []string{"L"},
		})
		require.NoError(t, err)
	}

	wrapped := newSizeTrackingEngine(inner, &DatabaseManager{}, "tenant")
	streamer := wrapped.(storage.StreamingEngine)

	var chunks [][]string
	require.NoError(t, streamer.StreamNodeChunks(context.Background(), 2, func(nodes []*storage.Node) error {
		ids := make([]string, 0, len(nodes))
		for _, n := range nodes {
			ids = append(ids, string(n.ID))
		}
		sort.Strings(ids)
		chunks = append(chunks, ids)
		return nil
	}))
	// 5 nodes / chunkSize 2 ⇒ 3 chunks (2, 2, 1).
	require.Len(t, chunks, 3)
	require.Len(t, chunks[2], 1)

	// Zero chunkSize is normalized to 1 chunk per node.
	chunks = nil
	require.NoError(t, streamer.StreamNodeChunks(context.Background(), 0, func(nodes []*storage.Node) error {
		chunks = append(chunks, []string{string(nodes[0].ID)})
		return nil
	}))
	require.Len(t, chunks, 5)
}

func TestSizeTrackingEngine_StreamNodesByPrefix_Fallback(t *testing.T) {
	inner := newNonStreamingEngine(t)
	for _, id := range []storage.NodeID{"a:1", "b:1", "a:2"} {
		_, err := inner.Engine.CreateNode(&storage.Node{ID: id, Labels: []string{"L"}})
		require.NoError(t, err)
	}

	wrapped := newSizeTrackingEngine(inner, &DatabaseManager{}, "a")
	prefixer := wrapped.(storage.PrefixStreamingEngine)

	var got []string
	require.NoError(t, prefixer.StreamNodesByPrefix(context.Background(), "a:", func(node *storage.Node) error {
		got = append(got, string(node.ID))
		return nil
	}))
	sort.Strings(got)
	require.Equal(t, []string{"a:1", "a:2"}, got)
}

func TestSizeTrackingEngine_ForEachNodeIDByLabel_FallbackOnNonLookupEngine(t *testing.T) {
	// MemoryEngine implements LabelNodeIDLookupEngine so the
	// "delegate" branch is hit. The fallback branch needs an inner
	// that does NOT implement it. Wrap MemoryEngine as a struct that
	// only embeds storage.Engine (already does, but lookup is type-
	// assertion based — we shadow the interface by name-based type).
	inner := newNonStreamingEngine(t)
	for i := 0; i < 3; i++ {
		_, err := inner.Engine.CreateNode(&storage.Node{
			ID:     storage.NodeID("tenant:" + string(rune('a'+i))),
			Labels: []string{"Person"},
		})
		require.NoError(t, err)
	}

	wrapped := newSizeTrackingEngine(inner, &DatabaseManager{}, "tenant")
	lookup := wrapped.(storage.LabelNodeIDLookupEngine)

	count := 0
	require.NoError(t, lookup.ForEachNodeIDByLabel("Person", func(id storage.NodeID) bool {
		count++
		return true
	}))
	require.Equal(t, 3, count)

	// Visit returning false stops early.
	count = 0
	require.NoError(t, lookup.ForEachNodeIDByLabel("Person", func(id storage.NodeID) bool {
		count++
		return false
	}))
	require.Equal(t, 1, count)
}
