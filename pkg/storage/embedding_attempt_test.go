package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"bytes"
	"encoding/gob"
	"encoding/json"
	"github.com/orneryd/nornicdb/pkg/embeddingsource"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
	"time"
)

func TestEmbeddingAttemptRuntimeIdentityNeverEncoded(t *testing.T) {
	node := &Node{ID: "identity:doc", Labels: []string{"Doc"}, Properties: map[string]any{"text": "body"}}
	state := node.runtimeState()
	state.embeddingRevision = EmbeddingBodyRevision{physical: 91, bufferedSequence: 7}
	state.embeddingAttempt = NewEmbeddingAttempt(CaptureVectorWorkOrigin(), 3, 5, state.embeddingRevision, func() (func(), bool) {
		t.Fatal("serialization must not call the attempt owner")
		return nil, false
	}, func(*Node) { t.Fatal("serialization must not publish") })
	node.setRuntimeState(state)
	for _, format := range []string{"gob", "msgpack", "json"} {
		t.Run(format, func(t *testing.T) {
			var decoded Node
			switch format {
			case "gob":
				var body bytes.Buffer
				require.NoError(t, gob.NewEncoder(&body).Encode(node))
				require.NoError(t, gob.NewDecoder(&body).Decode(&decoded))
			case "msgpack":
				body, err := encodeValue(node)
				require.NoError(t, err)
				require.NoError(t, decodeValue(body, &decoded))
			case "json":
				body, err := json.Marshal(node)
				require.NoError(t, err)
				require.NoError(t, json.Unmarshal(body, &decoded))
			}
			require.Equal(t, node.ID, decoded.ID)
			require.Equal(t, node.Properties, decoded.Properties)
			require.Zero(t, decoded.runtimeState().embeddingRevision)
			require.Nil(t, decoded.runtimeState().embeddingAttempt)
		})
	}
	copy := CopyNode(node)
	require.Equal(t, node.runtimeState().embeddingRevision, copy.runtimeState().embeddingRevision)
	require.Same(t, node.runtimeState().embeddingAttempt, copy.runtimeState().embeddingAttempt, "in-memory decorators keep the live token")
}

func TestEmbeddingAttemptCachedBodySnapshot(t *testing.T) {
	disk, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
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
	require.NotZero(t, snapshot.runtimeState().embeddingRevision)
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
			state := payload.runtimeState()
			state.embeddingAttempt = NewEmbeddingAttempt(CaptureVectorWorkOrigin(), 1, 1, state.embeddingRevision, func() (func(), bool) { return func() {}, true }, func(*Node) { published++ })
			payload.setRuntimeState(state)
			if updated {
				current, err := disk.GetNode("test:doc")
				require.NoError(t, err)
				tx, err := disk.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, tx.SetImplicit(true))
				require.NoError(t, tx.SetNamespace("test"))
				require.NoError(t, tx.UpdateGraphNode(current, ClientNodeEdits))
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
			disk, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
			require.NoError(t, err)
			defer disk.Close()
			disk.SetEmbeddingsEnabled(true)
			store := NewNamespacedEngine(disk, "attempt")
			_, err = store.CreateNode(&Node{ID: "doc", Properties: map[string]any{"id": "same", "text": "old"}})
			require.NoError(t, err)
			payload, err := store.GetNode("doc")
			require.NoError(t, err)
			require.NotZero(t, payload.runtimeState().embeddingRevision)
			payload.EmbedMeta = map[string]any{embeddingsource.MetaKey: (embeddingsource.Source{Properties: []string{"id"}}).Metadata()}
			if terminal {
				payload.EmbedMeta["embedding_failed"] = true
			} else {
				payload.ChunkEmbeddings = [][]float32{{1, 0}}
				payload.EmbedMeta["chunk_count"] = 1
			}
			published := 0
			state := payload.runtimeState()
			state.embeddingAttempt = NewEmbeddingAttempt(CaptureVectorWorkOrigin(), 7, 11, state.embeddingRevision, func() (func(), bool) { return func() {}, true }, func(node *Node) {
				published++
				require.Equal(t, "current", node.Properties["text"])
				require.NotEmpty(t, node.ChunkEmbeddings)
			})
			payload.setRuntimeState(state)
			current, err := store.GetNode("doc")
			require.NoError(t, err)
			stamp := current.UpdatedAt
			current.Properties["text"] = "current"
			require.NoError(t, store.UpdateNode(current))
			current, err = store.GetNode("doc")
			require.NoError(t, err)
			require.True(t, stamp.Equal(current.UpdatedAt))
			require.NotEqual(t, payload.runtimeState().embeddingRevision, current.runtimeState().embeddingRevision)
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
			require.Nil(t, saved.runtimeState().embeddingAttempt, "attempt identity must never be encoded")
			require.NotEqual(t, true, saved.EmbedMeta["embedding_failed"])
		})
	}
}

func TestEmbeddingPendingCounterNamespaceAndExclusion(t *testing.T) {
	disk, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
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

func TestEmbeddingRevisionIdentityAcrossFlush(t *testing.T) {
	disk := newWriteBehindTestEngine(t, time.Hour)
	bufferedCreate(t, disk, &Node{ID: "test:doc", Properties: map[string]any{"text": "same"}})
	acknowledged, err := disk.GetNode("test:doc")
	require.NoError(t, err)
	revision := acknowledged.EmbeddingRevision()
	same, err := disk.embeddingRevisionIsCurrent("test:doc", revision)
	require.NoError(t, err)
	require.True(t, same, "the acknowledged body is current before flush")
	require.NoError(t, disk.FlushWriteBehind())
	materialized, err := disk.getNodeSnapshot("test:doc", true)
	require.NoError(t, err)
	same, err = revision.MatchesNode(materialized)
	require.NoError(t, err)
	require.True(t, same, "flush is not a new body attempt")
	same, err = disk.embeddingRevisionIsCurrent("test:doc", revision)
	require.NoError(t, err)
	require.True(t, same)
	stamp := materialized.UpdatedAt
	require.NoError(t, disk.UpdateNode(materialized))
	updated, err := disk.getNodeSnapshot("test:doc", true)
	require.NoError(t, err)
	require.True(t, stamp.Equal(updated.UpdatedAt))
	same, err = revision.MatchesNode(updated)
	require.NoError(t, err)
	require.False(t, same, "an idempotent update is a new body revision")
	same, err = disk.embeddingRevisionIsCurrent("test:doc", revision)
	require.NoError(t, err)
	require.False(t, same)
}
