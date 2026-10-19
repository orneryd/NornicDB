package storage

import (
	"github.com/orneryd/nornicdb/pkg/embeddingsource"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
	"time"
)

func TestEmbeddingAttemptCachedBodySnapshot(t *testing.T) {
	disk, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	defer disk.Close()
	disk.SetEmbeddingsEnabled(false)
	store := NewNamespacedEngine(disk, "snapshot")
	_, err = store.CreateNode(&Node{ID: "doc", Properties: map[string]any{"text": "before"}})
	require.NoError(t, err)
	hits := atomic.LoadInt64(&disk.cacheHits)
	cached, err := store.GetNode("doc")
	require.NoError(t, err)
	require.Equal(t, hits+1, atomic.LoadInt64(&disk.cacheHits), "ordinary reads keep the written-node cache fast path")
	updated := CopyNode(cached)
	updated.Properties["text"] = "after"
	require.NoError(t, store.UpdateNode(updated))
	snapshot, err := SnapshotNodeForEmbedding(cached)
	require.NoError(t, err)
	require.Equal(t, cached.ID, snapshot.ID, "namespace translation must survive the storage snapshot reader")
	require.Equal(t, "before", cached.Properties["text"])
	require.Equal(t, "after", snapshot.Properties["text"])
	require.NotZero(t, snapshot.EmbeddingRevision)
}

func TestEmbeddingAttemptBufferedBodyRevision(t *testing.T) {
	for _, updated := range []bool{false, true} {
		t.Run(map[bool]string{false: "same_commit_after_flush", true: "idempotent_update"}[updated], func(t *testing.T) {
			disk := newWriteBehindTestEngine(t, time.Hour)
			bufferedCreate(t, disk, &Node{ID: "test:doc", Properties: map[string]any{"text": "same"}})
			payload, err := disk.GetNode("test:doc")
			require.NoError(t, err)
			payload.EmbedMeta = map[string]any{"embedding_failed": true}
			published := 0
			payload.EmbeddingAttempt = NewEmbeddingAttempt(1, 1, payload.EmbeddingRevision, func() (func(), bool) { return func() {}, true }, func(*Node) { published++ })
			if updated {
				current, err := disk.GetNode("test:doc")
				require.NoError(t, err)
				tx, err := disk.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, tx.SetImplicit(true))
				require.NoError(t, tx.SetNamespace("test"))
				require.NoError(t, tx.UpdateNode(current))
				require.NoError(t, tx.Commit())
			}
			require.NoError(t, disk.FlushWriteBehind())
			err = disk.UpdateNodeEmbeddingSidecar(payload)
			if updated {
				require.ErrorIs(t, err, ErrEmbeddingSourceChanged)
				require.Zero(t, published)
			} else {
				require.NoError(t, err)
				require.Equal(t, 1, published)
			}
		})
	}
}

// One acceptance matrix covers authoritative publication and exact terminal
// revision checks. In particular, no worker Enqueue can hide a storage defect.
func TestEmbeddingAttemptAuthoritativeOutcome(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		t.Run(map[bool]string{false: "vectors", true: "terminal"}[terminal], func(t *testing.T) {
			disk, err := NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			defer disk.Close()
			disk.SetEmbeddingsEnabled(true)
			store := NewNamespacedEngine(disk, "attempt")
			_, err = store.CreateNode(&Node{ID: "doc", Properties: map[string]any{"id": "same", "text": "old"}})
			require.NoError(t, err)
			payload, err := store.GetNode("doc")
			require.NoError(t, err)
			require.NotZero(t, payload.EmbeddingRevision)
			payload.EmbedMeta = map[string]any{embeddingsource.MetaKey: (embeddingsource.Source{Properties: []string{"id"}}).Metadata()}
			if terminal {
				payload.EmbedMeta["embedding_failed"] = true
			} else {
				payload.ChunkEmbeddings = [][]float32{{1, 0}}
				payload.EmbedMeta["chunk_count"] = 1
			}
			published := 0
			payload.EmbeddingAttempt = NewEmbeddingAttempt(7, 11, payload.EmbeddingRevision, func() (func(), bool) { return func() {}, true }, func(node *Node) {
				published++
				require.Equal(t, "current", node.Properties["text"])
				require.NotEmpty(t, node.ChunkEmbeddings)
			})
			current, err := store.GetNode("doc")
			require.NoError(t, err)
			stamp := current.UpdatedAt
			current.Properties["text"] = "current"
			require.NoError(t, store.UpdateNode(current))
			current, err = store.GetNode("doc")
			require.NoError(t, err)
			require.True(t, stamp.Equal(current.UpdatedAt))
			require.NotEqual(t, payload.EmbeddingRevision, current.EmbeddingRevision)
			err = store.UpdateNodeEmbeddingSidecar(payload)
			if terminal {
				require.ErrorIs(t, err, ErrEmbeddingSourceChanged)
				require.Zero(t, published)
			} else {
				require.NoError(t, err)
				require.Equal(t, 1, published)
			}
			saved, err := store.GetNode("doc")
			require.NoError(t, err)
			require.Equal(t, "current", saved.Properties["text"])
			require.Nil(t, saved.EmbeddingAttempt, "attempt identity must never be encoded")
			require.NotEqual(t, true, saved.EmbedMeta["embedding_failed"])
		})
	}
}

func TestEmbeddingPendingCounterNamespaceAndExclusion(t *testing.T) {
	disk, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	defer disk.Close()
	disk.SetEmbeddingsEnabled(true)
	for _, id := range []NodeID{"one:a", "one:b", "two:a"} {
		_, err := disk.CreateNode(&Node{ID: id, Properties: map[string]any{"text": "pending"}})
		require.NoError(t, err)
	}
	store := NewNamespacedEngine(disk, "one")
	seen := map[NodeID]bool{"a": true} // claimed/DLQ, also present in MAIN
	require.Equal(t, 1, CountPendingEmbeddingIDs(store, func(id []byte) bool { return !seen[NodeID(id)] }))
	require.Equal(t, 2, CountPendingEmbeddingIDs(store, nil))
	require.Equal(t, 3, disk.PendingEmbeddingsCount())
}
