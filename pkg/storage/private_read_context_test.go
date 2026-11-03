package storage

import (
	"context"
	"testing"
	"time"

	fixture "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

func TestEmbeddingCaptureReadsUseCallerContext(t *testing.T) {
	b, err := fixture.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	defer b.Close()
	b.SetEmbeddingsEnabled(false)
	_, err = b.CreateNode(t.Context(), &Node{ID: "test:doc", Properties: map[string]any{"text": "body"}})
	require.NoError(t, err)
	cached, err := b.GetNode(t.Context(), "test:doc")
	require.NoError(t, err)
	require.True(t, cached.EmbeddingRevision().isZero(), "exercise the capture-read branch, not already-bound identity")
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	snapshot, err := SnapshotNodeForEmbedding(canceled, cached)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, snapshot)
	snapshot, err = SnapshotNodeForEmbedding(t.Context(), cached)
	require.NoError(t, err)
	require.NotZero(t, snapshot.EmbeddingRevision())
	same, err := b.embeddingRevisionIsCurrent(canceled, snapshot.ID, snapshot.EmbeddingRevision())
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, same)
	same, err = b.embeddingRevisionIsCurrent(t.Context(), snapshot.ID, snapshot.EmbeddingRevision())
	require.NoError(t, err)
	require.True(t, same)
}

func TestEmbeddingBufferedRevisionComparisonUsesCallerContext(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	bufferedCreate(t, b, &Node{ID: "test:doc", Properties: map[string]any{"text": "body"}})
	ack, err := b.GetNode(t.Context(), "test:doc")
	require.NoError(t, err)
	require.NotZero(t, ack.EmbeddingRevision().bufferedSequence)
	require.NoError(t, b.FlushWriteBehind(context.Background()))
	materialized, err := b.getNodeSnapshot(t.Context(), ack.ID, true)
	require.NoError(t, err)
	require.NotZero(t, materialized.EmbeddingRevision().physical)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	same, err := ack.EmbeddingRevision().MatchesNode(canceled, materialized)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, same)
	same, err = ack.EmbeddingRevision().MatchesNode(t.Context(), materialized)
	require.NoError(t, err)
	require.True(t, same)
}

func TestTransactionEndpointReadsUseCallerContext(t *testing.T) {
	b, err := fixture.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	defer b.Close()
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err = b.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"L"}})
		require.NoError(t, err)
	}
	err = b.CreateEdge(t.Context(), &Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "T"})
	require.NoError(t, err)
	tx, err := b.BeginTransaction(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	func() {
		tx.mu.Lock()
		defer tx.mu.Unlock()
		labels, visible, err := tx.visibleNodeLabelsLocked(canceled, "test:a")
		require.ErrorIs(t, err, context.Canceled)
		require.False(t, visible)
		require.Nil(t, labels)
		_, err = tx.nodeLabelsTxVisibleLocked(canceled, "test:a")
		require.ErrorIs(t, err, context.Canceled)
		_, err = tx.incidentEdgeTypesTxLocked(canceled, "test:a", true)
		require.ErrorIs(t, err, context.Canceled)
		labels, visible, err = tx.visibleNodeLabelsLocked(t.Context(), "test:a")
		require.NoError(t, err)
		require.True(t, visible)
		require.Equal(t, []string{"L"}, labels)
		_, visible, err = tx.visibleNodeLabelsLocked(t.Context(), "test:missing")
		require.NoError(t, err)
		require.False(t, visible)
		counts, err := tx.incidentEdgeTypesTxLocked(t.Context(), "test:a", true)
		require.NoError(t, err)
		require.Equal(t, map[string]int64{"T": 1}, counts)
	}()
	require.ErrorIs(t, tx.CreateEdge(canceled, &Edge{ID: "test:new", StartNode: "test:a", EndNode: "test:b", Type: "T"}), context.Canceled)
	require.NoError(t, tx.CreateEdge(t.Context(), &Edge{ID: "test:new", StartNode: "test:a", EndNode: "test:b", Type: "T"}))
	require.NoError(t, tx.Commit())
	_, err = b.GetEdge(t.Context(), "test:new")
	require.NoError(t, err)
}

// The commit tail is already beyond durable acceptance. Cancellation may stop
// preparation but must not turn that committed outcome into a rejected event.
func TestEmbeddingAcceptedOutcomeSurvivesCancellation(t *testing.T) {
	for _, stopOwner := range []bool{false, true} {
		t.Run(map[bool]string{false: "request", true: "retained_completion"}[stopOwner], func(t *testing.T) {
			b := newSidecarTestBadger(t)
			node := publicationProofPayload(t, b)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			published := 0
			b.OnNodeUpdated(func(got *Node) {
				published++
				require.Equal(t, node.ID, got.ID)
				require.Equal(t, node.ChunkEmbeddings, got.ChunkEmbeddings)
			})
			restore := setCommitTailHook(func() {
				cancel()
				if stopOwner {
					b.work.Cancel()
				}
			})
			defer restore()
			require.NoError(t, b.UpdateNodeEmbeddingSidecar(ctx, node))
			require.ErrorIs(t, ctx.Err(), context.Canceled)
			if stopOwner {
				require.ErrorIs(t, b.work.Context().Err(), context.Canceled)
			}
			require.Equal(t, 1, published, "durably accepted outcome must reach its existing listener")
		})
	}
}
