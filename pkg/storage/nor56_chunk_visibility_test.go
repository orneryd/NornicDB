package storage

import (
	"context"
	"errors"
	"sort"
	"testing"

	fixture "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// nor56StreamIDs collects the node IDs a view's node stream and chunk stream
// publish, sorted, so both can be compared with the expected population.
func nor56StreamIDs(t *testing.T, ctx context.Context, view Engine, chunkSize int) (nodes, chunks []NodeID) {
	t.Helper()
	streamer, ok := ResolveStreamingEngine(view)
	require.True(t, ok, "the view must stream")
	require.NoError(t, streamer.StreamNodes(ctx, func(node *Node) error {
		nodes = append(nodes, node.ID)
		return nil
	}))
	require.NoError(t, streamer.StreamNodeChunks(ctx, chunkSize, func(batch []*Node) error {
		for _, node := range batch {
			chunks = append(chunks, node.ID)
		}
		return nil
	}))
	sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
	sort.Slice(chunks, func(i, j int) bool { return chunks[i] < chunks[j] })
	return nodes, chunks
}

// TestNOR56ChunkStreamsApplyNodeVisibility: a chunk stream publishes exactly
// the nodes the node stream publishes. A node hidden on demand stays hidden
// through the direct engine and every supported wrapper, an explicit reveal
// shows it on both streams, and a namespace view never includes another
// namespace.
func TestNOR56ChunkStreamsApplyNodeVisibility(t *testing.T) {
	base := fixture.Own(t, fixture.Track(t, NewMemoryEngine(), ErrStorageClosed))
	ctx := context.Background()
	for _, namespace := range []string{"a", "b"} {
		tenant := NewNamespacedEngine(base, namespace)
		for _, id := range []NodeID{"visible", "hidden"} {
			_, err := tenant.CreateNode(ctx, &Node{ID: id})
			require.NoError(t, err)
		}
	}
	require.NoError(t, base.SetManualSuppression(ctx, StoredEntityNodeKind, "a:hidden", true))
	all := []NodeID{"a:hidden", "a:visible", "b:hidden", "b:visible"}
	wholeStore := []NodeID{"a:visible", "b:hidden", "b:visible"}
	tenantA := []NodeID{"visible"}
	tenantARevealed := []NodeID{"hidden", "visible"}
	views := []struct {
		name     string
		view     Engine
		hidden   []NodeID // the population while a:hidden is suppressed
		revealed []NodeID // the population under an explicit reveal
	}{
		{"direct", base, wholeStore, all},
		{"wal", NewWALEngine(base, nil), wholeStore, all},
		{"traced", NewTracedEngine(base), wholeStore, all},
		{"wal over traced", NewWALEngine(NewTracedEngine(base), nil), wholeStore, all},
		{"namespace", NewNamespacedEngine(base, "a"), tenantA, tenantARevealed},
		{"namespace over traced", NewNamespacedEngine(NewTracedEngine(base), "a"), tenantA, tenantARevealed},
		{"traced over namespace", NewTracedEngine(NewNamespacedEngine(base, "a")), tenantA, tenantARevealed},
		{"namespace over wal over traced", NewNamespacedEngine(NewWALEngine(NewTracedEngine(base), nil), "a"), tenantA, tenantARevealed},
	}
	for _, tc := range views {
		t.Run(tc.name, func(t *testing.T) {
			for _, chunkSize := range []int{1, 10} {
				nodes, chunks := nor56StreamIDs(t, ctx, tc.view, chunkSize)
				require.Equal(t, tc.hidden, nodes, "node stream")
				require.Equal(t, tc.hidden, chunks, "chunk stream, chunk size %d", chunkSize)
			}
			release := base.BeginQueryRevealScope(true)
			nodes, chunks := nor56StreamIDs(t, ctx, tc.view, 10)
			release()
			require.Equal(t, tc.revealed, nodes, "revealed node stream")
			require.Equal(t, tc.revealed, chunks, "revealed chunk stream")
		})
	}
}

// TestNOR56ChunkStreamsPropagateCallbackErrorsAndCancellation: the chunk
// stream returns the callback's own error, ends quietly on ErrIterationStopped
// and stops on a cancelled context, directly and through a namespace over
// tracing.
func TestNOR56ChunkStreamsPropagateCallbackErrorsAndCancellation(t *testing.T) {
	base := fixture.Own(t, fixture.Track(t, NewMemoryEngine(), ErrStorageClosed))
	ctx := context.Background()
	tenant := NewNamespacedEngine(base, "a")
	for _, id := range []NodeID{"one", "two", "three"} {
		_, err := tenant.CreateNode(ctx, &Node{ID: id})
		require.NoError(t, err)
	}
	require.NoError(t, base.SetManualSuppression(ctx, StoredEntityNodeKind, "a:two", true))
	sentinel := errors.New("stop here")
	for name, view := range map[string]Engine{"direct": base, "namespace over traced": NewNamespacedEngine(NewTracedEngine(base), "a")} {
		t.Run(name, func(t *testing.T) {
			streamer, ok := ResolveStreamingEngine(view)
			require.True(t, ok)
			calls := 0
			err := streamer.StreamNodeChunks(ctx, 1, func([]*Node) error { calls++; return sentinel })
			require.ErrorIs(t, err, sentinel)
			require.Equal(t, 1, calls, "the stream stops at the first callback error")
			var stopped []NodeID
			require.NoError(t, streamer.StreamNodeChunks(ctx, 1, func(batch []*Node) error {
				stopped = append(stopped, batch[0].ID)
				return ErrIterationStopped
			}))
			require.Len(t, stopped, 1, "ErrIterationStopped ends the stream without an error")
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			err = streamer.StreamNodeChunks(cancelled, 1, func([]*Node) error {
				t.Error("visited after cancellation")
				return nil
			})
			require.ErrorIs(t, err, context.Canceled)
		})
	}
}
