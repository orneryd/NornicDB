package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The public sidecar writer retains a bound snapshot until both storage and
// worker delivery finish. Transferring that snapshot must not clone its vectors,
// whether or not storage has listeners. Unbound callers retain their own input.
func TestEmbeddingOutcomeSnapshotOwnership(t *testing.T) {
	for _, mode := range []string{"bound_no_listeners", "bound_listener", "unbound_listener"} {
		t.Run(mode, func(t *testing.T) {
			b, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
			require.NoError(t, err)
			defer b.Close()
			_, err = b.CreateNode(&Node{ID: "test:doc", Properties: map[string]any{"text": "original"}})
			require.NoError(t, err)
			payload, err := b.GetNode("test:doc")
			require.NoError(t, err)
			payload, err = SnapshotNodeForEmbedding(payload)
			require.NoError(t, err)
			payload.ChunkEmbeddings = [][]float32{{1, 2}}
			payload.EmbedMeta = map[string]any{"chunk_count": 1}
			bound := mode != "unbound_listener"
			published := make(chan *Node, 1)
			if bound {
				payload.SetEmbeddingAttempt(NewEmbeddingAttempt(CaptureVectorWorkOrigin(), 1, 1, payload.EmbeddingRevision(), func() (func(), bool) { return func() {}, true }, func(node *Node) { published <- node }))
			}
			resume, entered := make(chan struct{}), make(chan struct{})
			var unblock sync.Once
			defer unblock.Do(func() { close(resume) })
			listener := make(chan *Node, 1)
			var blockerDone chan error
			if mode == "bound_listener" {
				b.OnNodeUpdated(func(node *Node) {
					close(entered)
					<-resume
					listener <- node
				})
			} else if !bound {
				b.OnNodeCreated(func(node *Node) {
					if node.ID == "test:blocker" {
						close(entered)
						<-resume
					}
				})
				b.OnNodeUpdated(func(node *Node) { listener <- node })
				blockerDone = make(chan error, 1)
				go func() { _, err := b.CreateNode(&Node{ID: "test:blocker"}); blockerDone <- err }()
				select {
				case <-entered:
				case <-time.After(3 * time.Second):
					t.Fatal("public listener did not hold delivery")
				}
			}
			done := make(chan error, 1)
			go func() { done <- b.UpdateNodeEmbeddingSidecar(payload) }()
			if mode == "bound_listener" {
				select {
				case <-entered:
				case <-time.After(3 * time.Second):
					t.Fatal("accepted outcome did not enter the public listener")
				}
				select {
				case <-done:
					t.Fatal("bound writer returned while its snapshot was still borrowed")
				default:
				}
			}
			if !bound {
				select {
				case err := <-done:
					require.NoError(t, err)
				case <-time.After(3 * time.Second):
					t.Fatal("unbound writer waited for unrelated delivery")
				}
				// Returning transfers no caller ownership: queued data must be detached.
				payload.Properties["text"] = "caller edit"
				payload.ChunkEmbeddings[0][0] = 99
			}
			unblock.Do(func() { close(resume) })
			if bound {
				select {
				case err := <-done:
					require.NoError(t, err)
				case <-time.After(3 * time.Second):
					t.Fatal("bound outcome did not finish delivery")
				}
				require.Same(t, payload, <-published, "the worker transfers its accepted snapshot through the queue")
			}
			if blockerDone != nil {
				require.NoError(t, <-blockerDone)
			}
			if mode != "bound_no_listeners" {
				observed := <-listener
				if bound {
					require.Same(t, payload, observed, "listeners borrow the same owned snapshot")
				} else {
					require.NotSame(t, payload, observed)
					require.Equal(t, "original", observed.Properties["text"])
					require.Equal(t, float32(1), observed.ChunkEmbeddings[0][0])
				}
			}
		})
	}
}

func TestEmbeddingRevisionCapturedBufferedUpdates(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	b.writeBehind.Close()
	bufferedCreate(t, b, &Node{ID: "test:doc", Properties: map[string]any{"text": "before"}})
	require.NoError(t, b.FlushWriteBehind())
	old, err := b.GetNode("test:doc")
	require.NoError(t, err)
	old, err = SnapshotNodeForEmbedding(old)
	require.NoError(t, err)
	stamp := old.UpdatedAt
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.UpdateGraphNode(NodeUpdateRequest{After: &Node{ID: old.ID, UpdatedAt: stamp, Properties: map[string]any{"text": "after"}}, Policy: ClientNodeEdits}))
	require.NoError(t, tx.Commit())
	ack, err := b.GetNode("test:doc")
	require.NoError(t, err)
	require.True(t, stamp.Equal(ack.UpdatedAt))
	same, err := b.embeddingRevisionIsCurrent(ack.ID, old.EmbeddingRevision())
	require.NoError(t, err)
	require.False(t, same)
	same, err = b.embeddingRevisionIsCurrent(ack.ID, ack.EmbeddingRevision())
	require.NoError(t, err)
	require.True(t, same)
	require.NoError(t, b.FlushWriteBehind())
	materialized, err := b.GetNode("test:doc")
	require.NoError(t, err)
	materialized, err = SnapshotNodeForEmbedding(materialized)
	require.NoError(t, err)
	same, err = ack.EmbeddingRevision().MatchesNode(materialized)
	require.NoError(t, err)
	require.True(t, same)
	tx, err = b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.DeleteGraphNode(ack.ID, CascadeEdges))
	require.NoError(t, tx.Commit())
	same, err = b.embeddingRevisionIsCurrent(ack.ID, ack.EmbeddingRevision())
	require.ErrorIs(t, err, ErrNotFound)
	require.False(t, same)
	require.NoError(t, b.FlushWriteBehind())
	same, err = b.embeddingRevisionIsCurrent(ack.ID, ack.EmbeddingRevision())
	require.ErrorIs(t, err, ErrNotFound)
	require.False(t, same)
}
