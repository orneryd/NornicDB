package storage

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embeddingsource"
	"github.com/stretchr/testify/require"
)

// Keep delivery busy while public sidecar and body operations commit. Both
// branches must suppress the obsolete revision at the same delivery boundary,
// whether or not a worker token accompanies the public sidecar operation.
func TestReviewR73QueuedOutcomeRevisionBarrier(t *testing.T) {
	for _, mode := range []string{"update", "delete", "unbound_update", "unbound_delete"} {
		t.Run(mode, func(t *testing.T) {
			disk, err := NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			defer disk.Close()
			store := NewNamespacedEngine(disk, "queued")
			_, err = store.CreateNode(&Node{ID: "doc", Properties: map[string]any{"id": "same", "text": "antiquecobalt"}})
			require.NoError(t, err)
			payload, err := store.GetNode("doc")
			require.NoError(t, err)
			payload, err = SnapshotNodeForEmbedding(payload)
			require.NoError(t, err)
			payload.ChunkEmbeddings = [][]float32{{1, 0}}
			payload.EmbedMeta = map[string]any{"chunk_count": 1, embeddingsource.MetaKey: (embeddingsource.Source{Properties: []string{"id"}}).Metadata()}
			var publications atomic.Int64
			bound := mode == "update" || mode == "delete"
			if bound {
				payload.SetEmbeddingAttempt(NewEmbeddingAttempt(1, 1, payload.EmbeddingRevision(), func() (func(), bool) { return func() {}, true }, func(*Node) { publications.Add(1) }))
			}
			entered, resume := make(chan struct{}), make(chan struct{})
			var unblock sync.Once
			defer unblock.Do(func() { close(resume) })
			store.OnNodeCreated(func(node *Node) {
				if node.ID == "blocker" {
					close(entered)
					<-resume
				}
			})
			var mu sync.Mutex
			var events []string
			store.OnNodeUpdated(func(node *Node) { mu.Lock(); events = append(events, node.Properties["text"].(string)); mu.Unlock() })
			store.OnNodeDeleted(func(NodeID) { mu.Lock(); events = append(events, "deleted"); mu.Unlock() })
			blockedDone := make(chan error, 1)
			go func() { _, err := store.CreateNode(&Node{ID: "blocker"}); blockedDone <- err }()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("notification drainer did not enter the public listener")
			}
			committed := make(chan struct{})
			var first atomic.Bool
			restore := setCommitTailHook(func() {
				if first.CompareAndSwap(false, true) {
					close(committed)
				}
			})
			defer restore()
			done := make(chan error, 1)
			go func() { done <- store.UpdateNodeEmbeddingSidecar(payload) }()
			select {
			case <-committed:
			case <-time.After(3 * time.Second):
				t.Fatal("sidecar did not commit")
			}
			deletion := mode == "delete" || mode == "unbound_delete"
			if deletion {
				require.NoError(t, store.DeleteNode("doc"))
			} else {
				current, err := store.GetNode("doc")
				require.NoError(t, err)
				current.Properties["text"] = "novelquartz"
				require.NoError(t, store.UpdateNode(current))
			}
			unblock.Do(func() { close(resume) })
			select {
			case err = <-done:
				if bound {
					require.ErrorIs(t, err, ErrEmbeddingSourceChanged)
				} else {
					require.NoError(t, err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("obsolete outcome did not finish")
			}
			require.NoError(t, <-blockedDone)
			WaitForListeners(store)
			mu.Lock()
			observed := append([]string(nil), events...)
			mu.Unlock()
			want := "novelquartz"
			if deletion {
				want = "deleted"
				_, err = store.GetNode("doc")
				require.ErrorIs(t, err, ErrNotFound)
			}
			require.Equal(t, []string{want}, observed, "the newer event must be the only published body")
			require.Zero(t, publications.Load(), "obsolete worker outcomes cannot publish or count success")
		})
	}
}
