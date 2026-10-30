package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embeddingsource"
	"github.com/stretchr/testify/require"
)

// Interpose only at the existing post-commit scheduling hook. No stored or
// queue state is fabricated: the competing update/delete uses the public API.
func TestReviewR73OutcomePublicationCommitOrder(t *testing.T) {
	for _, deletion := range []bool{false, true} {
		t.Run(map[bool]string{false: "update", true: "delete"}[deletion], func(t *testing.T) {
			disk, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
			require.NoError(t, err)
			defer disk.Close()
			disk.SetEmbeddingsEnabled(true)
			store := NewNamespacedEngine(disk, "order")
			_, err = store.CreateNode(&Node{ID: "doc", Properties: map[string]any{"id": "same", "text": "antiquecobalt"}})
			require.NoError(t, err)
			payload, err := store.GetNode("doc")
			require.NoError(t, err)
			payload, err = SnapshotNodeForEmbedding(payload)
			require.NoError(t, err)
			payload.ChunkEmbeddings = [][]float32{{1, 0}}
			payload.EmbedMeta = map[string]any{"chunk_count": 1, embeddingsource.MetaKey: (embeddingsource.Source{Properties: []string{"id"}}).Metadata()}
			var mu sync.Mutex
			var events []string
			var publication string
			store.OnNodeUpdated(func(n *Node) { mu.Lock(); events = append(events, n.Properties["text"].(string)); mu.Unlock() })
			store.OnNodeDeleted(func(NodeID) { mu.Lock(); events = append(events, "deleted"); mu.Unlock() })
			payload.SetEmbeddingAttempt(NewEmbeddingAttempt(1, 1, payload.EmbeddingRevision(), func() (func(), bool) { return func() {}, true }, func(n *Node) {
				mu.Lock()
				publication = n.Properties["text"].(string)
				mu.Unlock()
			}))
			committed, resume := make(chan struct{}), make(chan struct{})
			var first atomic.Bool
			restore := setCommitTailHook(func() {
				if first.CompareAndSwap(false, true) {
					close(committed)
					<-resume
				}
			})
			defer restore()
			var release sync.Once
			defer release.Do(func() { close(resume) })
			done := make(chan error, 1)
			go func() { done <- store.UpdateNodeEmbeddingSidecar(payload) }()
			select {
			case <-committed:
			case <-time.After(3 * time.Second):
				t.Fatal("sidecar did not commit")
			}
			mutated := make(chan error, 1)
			go func() {
				if deletion {
					mutated <- store.DeleteNode("doc")
					return
				}
				current, readErr := store.GetNode("doc")
				if readErr != nil {
					mutated <- readErr
					return
				}
				current.Properties["text"] = "novelquartz"
				mutated <- store.UpdateNode(NodeUpdateRequest{After: current})
			}()
			// A branch may correctly exclude body writers until publication.
			// Give an unprotected writer the opportunity to finish, then release
			// the sidecar without requiring the protected branch to deadlock.
			mutationDone := false
			select {
			case err = <-mutated:
				require.NoError(t, err)
				mutationDone = true
			case <-time.After(150 * time.Millisecond):
			}
			release.Do(func() { close(resume) })
			if !mutationDone {
				select {
				case err = <-mutated:
					require.NoError(t, err)
				case <-time.After(3 * time.Second):
					t.Fatal("body mutation did not finish")
				}
			}
			select {
			case err = <-done:
				if err != nil {
					require.ErrorIs(t, err, ErrEmbeddingSourceChanged)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("sidecar publication did not finish")
			}
			require.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(events) >= 1 }, time.Second, time.Millisecond)
			mu.Lock()
			observed := append([]string(nil), events...)
			published := publication
			mu.Unlock()
			t.Logf("events=%v workerPublication=%s", observed, published)
			if len(observed) == 1 {
				require.Empty(t, published, "suppressed stale outcomes must not run the worker callback")
			} else {
				require.Len(t, observed, 2)
				require.Equal(t, "antiquecobalt", published)
			}
			if deletion {
				_, err = store.GetNode("doc")
				require.ErrorIs(t, err, ErrNotFound)
				require.Equal(t, "deleted", observed[len(observed)-1], "an earlier embedding commit must not resurrect a deleted search entry")
			} else {
				current, err := store.GetNode("doc")
				require.NoError(t, err)
				require.Equal(t, "novelquartz", current.Properties["text"])
				require.Equal(t, "novelquartz", observed[len(observed)-1], "an earlier embedding commit must not roll back a newer full-text event")
			}
		})
	}
}
