package storage

import (
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// #889: an embedding writeback lands only while the stored node still has the
// properties and labels the worker embedded. Otherwise nothing is written and
// the pending marker the change set stays, so the new content is embedded.

func sidecarPending(t *testing.T, b *BadgerEngine, id NodeID) bool {
	t.Helper()
	pending := false
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		_, err := txn.Get(pendingEmbedKey(id))
		pending = err == nil
		return nil
	}))
	return pending
}

func TestEmbeddingSidecar_SourceChangedIsNotWritten(t *testing.T) {
	for _, change := range []struct {
		name  string
		apply func(*Node)
	}{
		{"property with a new UpdatedAt", func(n *Node) {
			n.Properties = map[string]any{"title": "v2"}
			n.UpdatedAt = n.UpdatedAt.Add(time.Second)
		}},
		{"property keeping UpdatedAt", func(n *Node) { n.Properties = map[string]any{"title": "v2"} }},
		{"added property", func(n *Node) { n.Properties["extra"] = int64(1) }},
		{"label", func(n *Node) { n.Labels = []string{"Doc", "Draft"} }},
	} {
		t.Run(change.name, func(t *testing.T) {
			b := newSidecarTestBadger(t)
			b.SetEmbeddingsEnabled(true)
			now := time.Now().UTC().Truncate(time.Millisecond)
			node := sidecarTestNode(t, b, "source-changed", map[string]any{"title": "v1"}, now)
			embedded := embeddingWriteback(t, b, node.ID, [][]float32{{0.1, 0.2}}, map[string]any{"has_embedding": true, "chunk_count": 1}, time.Time{})

			changed, err := b.GetNode(node.ID)
			require.NoError(t, err)
			change.apply(changed)
			require.NoError(t, b.UpdateNode(changed))
			require.True(t, sidecarPending(t, b, node.ID))

			require.ErrorIs(t, b.UpdateNodeEmbeddingSidecar(embedded), ErrEmbeddingSourceChanged)
			require.True(t, sidecarPending(t, b, node.ID), "the change's pending marker must stay")
			got, err := b.GetNode(node.ID)
			require.NoError(t, err)
			require.Empty(t, got.ChunkEmbeddings)
			require.Empty(t, got.EmbedMeta)

			// The worker's next pass embeds the current content.
			current := embeddingWriteback(t, b, node.ID, [][]float32{{0.3, 0.4}}, map[string]any{"has_embedding": true, "chunk_count": 1}, time.Time{})
			require.NoError(t, b.UpdateNodeEmbeddingSidecar(current))
			require.False(t, sidecarPending(t, b, node.ID))
			got, err = b.GetNode(node.ID)
			require.NoError(t, err)
			require.Equal(t, [][]float32{{0.3, 0.4}}, got.ChunkEmbeddings)
		})
	}
}

// A business write that commits after the writeback checked the node but
// before it committed fails the writeback's commit (#889).
func TestEmbeddingSidecar_ConcurrentChangeIsNotWritten(t *testing.T) {
	b := newSidecarTestBadger(t)
	b.SetEmbeddingsEnabled(true)
	now := time.Now().UTC().Truncate(time.Millisecond)
	node := sidecarTestNode(t, b, "source-race", map[string]any{"title": "v1"}, now)
	embedded := embeddingWriteback(t, b, node.ID, [][]float32{{0.1, 0.2}}, map[string]any{"has_embedding": true, "chunk_count": 1}, time.Time{})

	hook := func() {
		changed, err := b.GetNode(node.ID)
		require.NoError(t, err)
		changed.Properties = map[string]any{"title": "v2"}
		require.NoError(t, b.UpdateNode(changed))
	}
	embeddingSourceCheckedHook.Store(&hook)
	t.Cleanup(func() { embeddingSourceCheckedHook.Store(nil) })

	require.ErrorIs(t, b.UpdateNodeEmbeddingSidecar(embedded), ErrEmbeddingSourceChanged)
	embeddingSourceCheckedHook.Store(nil)
	require.True(t, sidecarPending(t, b, node.ID))
	got, err := b.GetNode(node.ID)
	require.NoError(t, err)
	require.Equal(t, "v2", got.Properties["title"])
	require.Empty(t, got.ChunkEmbeddings)
}

// The worker's copy may come from a cache holding Go types the decoded node
// doesn't; the comparison is of stored values.
func TestSameEmbeddingSource(t *testing.T) {
	at := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	node := func(labels []string, props map[string]any) *Node { return &Node{Labels: labels, Properties: props} }
	for _, tc := range []struct {
		name string
		a, b *Node
		same bool
	}{
		{"int and int64", node([]string{"A"}, map[string]any{"n": 1}), node([]string{"A"}, map[string]any{"n": int64(1)}), true},
		{"int and float64", node(nil, map[string]any{"n": 2}), node(nil, map[string]any{"n": 2.0}), true},
		{"[]string and []any", node(nil, map[string]any{"l": []string{"x", "y"}}), node(nil, map[string]any{"l": []any{"x", "y"}}), true},
		{"nested maps", node(nil, map[string]any{"m": map[string]any{"k": []int{1}}}), node(nil, map[string]any{"m": map[string]any{"k": []any{int64(1)}}}), true},
		{"times as instants", node(nil, map[string]any{"t": at}), node(nil, map[string]any{"t": at.In(time.FixedZone("x", 3600))}), true},
		{"nil values", node(nil, map[string]any{"v": nil}), node(nil, map[string]any{"v": nil}), true},
		{"labels in any order", node([]string{"A", "B"}, nil), node([]string{"B", "A"}, nil), true},
		{"other value", node(nil, map[string]any{"s": "a"}), node(nil, map[string]any{"s": "b"}), false},
		{"number and string", node(nil, map[string]any{"n": 1}), node(nil, map[string]any{"n": "1"}), false},
		{"time and string", node(nil, map[string]any{"t": at}), node(nil, map[string]any{"t": "2026"}), false},
		{"nil and value", node(nil, map[string]any{"v": nil}), node(nil, map[string]any{"v": 1}), false},
		{"list lengths", node(nil, map[string]any{"l": []any{1}}), node(nil, map[string]any{"l": []any{1, 2}}), false},
		{"list items", node(nil, map[string]any{"l": []any{1}}), node(nil, map[string]any{"l": []any{2}}), false},
		{"map sizes", node(nil, map[string]any{"m": map[string]any{"a": 1}}), node(nil, map[string]any{"m": map[string]any{}}), false},
		{"map keys", node(nil, map[string]any{"m": map[string]any{"a": 1}}), node(nil, map[string]any{"m": map[string]any{"b": 1}}), false},
		{"map values", node(nil, map[string]any{"m": map[string]any{"a": 1}}), node(nil, map[string]any{"m": map[string]any{"a": 2}}), false},
		{"other property", node(nil, map[string]any{"a": 1}), node(nil, map[string]any{"b": 1}), false},
		{"property count", node(nil, map[string]any{"a": 1}), node(nil, map[string]any{"a": 1, "b": 2}), false},
		{"other label", node([]string{"A"}, nil), node([]string{"B"}, nil), false},
		{"label count", node([]string{"A"}, nil), node([]string{"A", "A"}, nil), false},
		{"other kinds", node(nil, map[string]any{"v": true}), node(nil, map[string]any{"v": []any{true}}), false},
	} {
		require.Equal(t, tc.same, (*BadgerEngine)(nil).sameEmbeddingSource(tc.a, tc.b), tc.name)
		require.Equal(t, tc.same, (*BadgerEngine)(nil).sameEmbeddingSource(tc.b, tc.a), tc.name)
	}
}

// A stored node that can't be read is the writeback's error, and a write that
// fails leaves nothing behind (#889).
func TestEmbeddingSidecar_SourceReadAndWriteFailures(t *testing.T) {
	t.Run("undecodable stored node", func(t *testing.T) {
		b := newSidecarTestBadger(t)
		writeRawValue(t, b, nodeKey("test:corrupt"), []byte{0xFF, 0x00})
		err := b.UpdateNodeEmbeddingSidecar(&Node{ID: "test:corrupt", ChunkEmbeddings: [][]float32{{0.1}}})
		require.Error(t, err)
		require.NotErrorIs(t, err, ErrEmbeddingSourceChanged)
	})
	t.Run("chunk write fails", func(t *testing.T) {
		b, _ := createTestBadgerEngineOnDisk(t)
		t.Cleanup(func() { _ = b.Close() })
		// The node key fits Badger's key limit; its embedding chunk keys,
		// five bytes longer, don't.
		node := &Node{ID: NodeID("test:" + strings.Repeat("k", 65000-len("test:")-2)), Labels: []string{"Doc"}, Properties: map[string]any{"title": "v1"}}
		writeRawNode(t, b, node)
		payload := CopyNode(node)
		payload.ChunkEmbeddings = [][]float32{{0.1}}
		payload.EmbedMeta = map[string]any{"has_embedding": true}
		require.Error(t, b.UpdateNodeEmbeddingSidecar(payload))
		require.NoError(t, b.withView(func(txn *badger.Txn) error {
			_, err := txn.Get(embeddingMetaKey(node.ID))
			require.ErrorIs(t, err, badger.ErrKeyNotFound)
			return nil
		}))
	})
}
