package storage

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// TestEmbeddingTextPolicy covers which properties and labels feed managed
// embedding text, and when two copies of a node share an embedding source
// (#963).
func TestEmbeddingTextPolicy(t *testing.T) {
	all := EmbeddingTextPolicy{IncludeLabels: true}
	require.True(t, all.FeedsText("title"))
	require.False(t, all.FeedsText("id"))
	require.False(t, all.FeedsText("embedding"))
	require.True(t, IsEmbeddingMetadataProperty("updatedAt"))
	require.False(t, IsEmbeddingMetadataProperty("title"))
	textOnly := EmbeddingTextPolicy{Include: []string{"text"}}
	require.True(t, textOnly.FeedsText("text"))
	require.False(t, textOnly.FeedsText("tags"))
	excluding := EmbeddingTextPolicy{Exclude: []string{"tags"}}
	require.False(t, excluding.FeedsText("tags"))
	require.True(t, excluding.FeedsText("text"))

	base := &Node{Labels: []string{"A", "B"}, Properties: map[string]any{"text": "x", "tags": []any{"a"}, "id": "1"}}
	for _, tc := range []struct {
		name   string
		policy EmbeddingTextPolicy
		other  *Node
		same   bool
	}{
		{"identical, labels reordered", all, &Node{Labels: []string{"B", "A"}, Properties: map[string]any{"text": "x", "tags": []string{"a"}, "id": "1"}}, true},
		{"an id change", all, &Node{Labels: []string{"A", "B"}, Properties: map[string]any{"text": "x", "tags": []any{"a"}, "id": "2"}}, true},
		{"a fed property changed", all, &Node{Labels: []string{"A", "B"}, Properties: map[string]any{"text": "y", "tags": []any{"a"}, "id": "1"}}, false},
		{"a fed property added", all, &Node{Labels: []string{"A", "B"}, Properties: map[string]any{"text": "x", "tags": []any{"a"}, "id": "1", "more": 1}}, false},
		{"a fed property removed", all, &Node{Labels: []string{"A", "B"}, Properties: map[string]any{"text": "x", "id": "1"}}, false},
		{"a label added", all, &Node{Labels: []string{"A", "B", "C"}, Properties: base.Properties}, false},
		{"an unfed property changed", textOnly, &Node{Labels: []string{"A", "B"}, Properties: map[string]any{"text": "x", "tags": []any{"b"}}}, true},
		{"an unfed property added", textOnly, &Node{Labels: []string{"A", "B"}, Properties: map[string]any{"text": "x", "tags": []any{"a"}, "id": "1", "more": 1}}, true},
		{"labels not fed", textOnly, &Node{Labels: []string{"C"}, Properties: map[string]any{"text": "x"}}, true},
	} {
		require.Equal(t, tc.same, tc.policy.SameSource(base, tc.other), tc.name)
		require.Equal(t, tc.same, tc.policy.SameSource(tc.other, base), tc.name)
	}

	var engine *BadgerEngine
	require.Equal(t, defaultEmbeddingTextPolicy, engine.embeddingTextPolicyOrDefault())
	engine = &BadgerEngine{}
	include := []string{"text"}
	engine.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Include: include})
	include[0] = "changed"
	require.Equal(t, []string{"text"}, engine.embeddingTextPolicyOrDefault().Include)
	require.True(t, sameChunkEmbeddings([][]float32{{1, 2}}, [][]float32{{1, 2}}))
	require.False(t, sameChunkEmbeddings([][]float32{{1, 2}}, [][]float32{{1, 3}}))
	require.False(t, sameChunkEmbeddings([][]float32{{1, 2}}, nil))
}

// TestTransactionUpdateKeepsOrInvalidatesManagedEmbeddings: an update in a
// transaction that leaves the embedding source unchanged keeps the node's
// sidecar embeddings for readers; one that changes it deletes the sidecar,
// so the node is unembedded and pending (#963).
func TestTransactionUpdateKeepsOrInvalidatesManagedEmbeddings(t *testing.T) {
	b := newSidecarTestBadger(t)
	b.SetEmbeddingsEnabled(true)
	b.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Include: []string{"text"}})
	now := time.Now().UTC().Truncate(time.Millisecond)
	node := sidecarTestNode(t, b, "keep", map[string]any{"text": "boiler invoice", "tags": []any{"a"}}, now)
	require.NoError(t, b.UpdateNodeEmbeddingSidecar(embeddingWriteback(t, b, node.ID, [][]float32{{0.1, 0.2}}, map[string]any{"has_embedding": true, "chunk_count": 1}, now)))

	update := func(change func(*Node)) {
		tx, err := b.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, tx.SetNamespace("test"))
		current, err := tx.GetNode(node.ID)
		require.NoError(t, err)
		change(current)
		require.NoError(t, tx.UpdateNode(current))
		require.NoError(t, tx.Commit())
	}
	sidecarPresent := func() bool {
		present := false
		require.NoError(t, b.withView(func(txn *badger.Txn) error {
			_, err := txn.Get(embeddingMetaKey(node.ID))
			present = err == nil
			return nil
		}))
		return present
	}

	// An unfed property: the vectors stay, read from the sidecar.
	update(func(n *Node) { n.Properties["tags"] = []any{"a", "b"} })
	got, err := b.GetNode(node.ID)
	require.NoError(t, err)
	require.Equal(t, [][]float32{{0.1, 0.2}}, got.ChunkEmbeddings)
	require.True(t, sidecarPresent())
	b.invalidateCachesAfterRestore()
	got, err = b.GetNode(node.ID)
	require.NoError(t, err)
	require.Equal(t, [][]float32{{0.1, 0.2}}, got.ChunkEmbeddings)

	// A caller clearing the vectors for an unfed change gets them back too.
	update(func(n *Node) {
		n.Properties["tags"] = []any{"c"}
		n.ChunkEmbeddings = nil
		n.EmbedMeta = nil
	})
	got, err = b.GetNode(node.ID)
	require.NoError(t, err)
	require.Len(t, got.ChunkEmbeddings, 1)

	// A fed property: the vectors and the sidecar go, and the node is
	// pending for the embed worker.
	update(func(n *Node) { n.Properties["text"] = "boiler receipt" })
	got, err = b.GetNode(node.ID)
	require.NoError(t, err)
	require.Empty(t, got.ChunkEmbeddings)
	require.False(t, sidecarPresent())
	b.invalidateCachesAfterRestore()
	got, err = b.GetNode(node.ID)
	require.NoError(t, err)
	require.Empty(t, got.ChunkEmbeddings)
	pending := b.FindNodeNeedingEmbedding()
	require.NotNil(t, pending)
	require.Equal(t, node.ID, pending.ID)

	// Vectors the caller sets are written as given.
	update(func(n *Node) {
		n.Properties["text"] = "boiler statement"
		n.ChunkEmbeddings = [][]float32{{0.5, 0.6}}
	})
	got, err = b.GetNode(node.ID)
	require.NoError(t, err)
	require.Equal(t, [][]float32{{0.5, 0.6}}, got.ChunkEmbeddings)
}

// TestTransactionUpdateKeepsInlineEmbeddings: embeddings stored in the node
// body (no sidecar) stay in the body across an update that leaves the
// embedding source unchanged.
func TestTransactionUpdateKeepsInlineEmbeddings(t *testing.T) {
	b := newSidecarTestBadger(t)
	b.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Include: []string{"text"}})
	id := NodeID("test:inline")
	_, err := b.CreateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"text": "x", "tags": "a"}, ChunkEmbeddings: [][]float32{{1, 2}}})
	require.NoError(t, err)
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	current, err := tx.GetNode(id)
	require.NoError(t, err)
	current.Properties["tags"] = "b"
	current.ChunkEmbeddings = nil
	require.NoError(t, tx.UpdateNode(current))
	require.NoError(t, tx.Commit())
	b.invalidateCachesAfterRestore()
	got, err := b.GetNode(id)
	require.NoError(t, err)
	require.Equal(t, [][]float32{{1, 2}}, got.ChunkEmbeddings)

	// New vectors a caller writes with a changed text are written as given.
	tx, err = b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	current, err = tx.GetNode(id)
	require.NoError(t, err)
	current.Properties["text"] = "y"
	current.ChunkEmbeddings = [][]float32{{3, 4}}
	require.NoError(t, tx.UpdateNode(current))
	require.NoError(t, tx.Commit())
	got, err = b.GetNode(id)
	require.NoError(t, err)
	require.Equal(t, [][]float32{{3, 4}}, got.ChunkEmbeddings)
	require.Nil(t, copyNodeForCaller(nil))
}

// TestTransactionStaleCopyDoesNotRestoreInvalidatedEmbeddings: a statement
// that writes one node twice from the same row copy (REMOVE n:Embedded,
// n.text SET n.revision = 3) invalidates the managed embeddings on the
// first write; the second write carries the copy's old embeddings back and
// must not store them again. Vectors the caller sets after the invalidation
// are still written as given. Reported by the Personal Documents
// integration (I26).
func TestTransactionStaleCopyDoesNotRestoreInvalidatedEmbeddings(t *testing.T) {
	b := newSidecarTestBadger(t)
	b.SetEmbeddingsEnabled(true)
	b.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Include: []string{"text"}})
	now := time.Now().UTC().Truncate(time.Millisecond)

	embedded := func(id string) *Node {
		node := sidecarTestNode(t, b, id, map[string]any{"text": "boiler invoice " + id, "revision": int64(1)}, now)
		require.NoError(t, b.UpdateNodeEmbeddingSidecar(embeddingWriteback(t, b, node.ID, [][]float32{{0.1, 0.2}}, map[string]any{"has_embedding": true, "chunk_count": 1}, now)))
		got, err := b.GetNode(node.ID)
		require.NoError(t, err)
		require.Len(t, got.ChunkEmbeddings, 1)
		return node
	}
	twoWrites := func(id NodeID, second func(*Node)) {
		tx, err := b.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, tx.SetNamespace("test"))
		row, err := tx.GetNode(id)
		require.NoError(t, err)
		require.Len(t, row.ChunkEmbeddings, 1, "the row copy carries the embeddings it was read with")
		delete(row.Properties, "text")
		require.NoError(t, tx.UpdateNode(CopyNode(row)))
		second(row)
		require.NoError(t, tx.UpdateNode(row))
		require.NoError(t, tx.Commit())
	}

	stale := embedded("stale")
	twoWrites(stale.ID, func(row *Node) { row.Properties["revision"] = int64(2) })
	got, err := b.GetNode(stale.ID)
	require.NoError(t, err)
	require.Empty(t, got.ChunkEmbeddings)
	require.Nil(t, got.EmbedMeta)
	require.Equal(t, int64(2), got.Properties["revision"])
	b.invalidateCachesAfterRestore()
	got, err = b.GetNode(stale.ID)
	require.NoError(t, err)
	require.Empty(t, got.ChunkEmbeddings, "nor after a restart")

	fresh := embedded("fresh")
	twoWrites(fresh.ID, func(row *Node) { row.ChunkEmbeddings = [][]float32{{0.5, 0.6}} })
	got, err = b.GetNode(fresh.ID)
	require.NoError(t, err)
	require.Equal(t, [][]float32{{0.5, 0.6}}, got.ChunkEmbeddings)
}

// carriesDroppedEmbeddings: the dropped vectors, or no vectors with their
// metadata, are a stale copy; other vectors are the caller's, and no
// embedding state at all carries nothing back.
func TestCarriesDroppedEmbeddings(t *testing.T) {
	dropped := &Node{ChunkEmbeddings: [][]float32{{0.1, 0.2}}, EmbedMeta: map[string]any{"has_embedding": true}}
	require.True(t, carriesDroppedEmbeddings(&Node{ChunkEmbeddings: [][]float32{{0.1, 0.2}}}, dropped))
	require.True(t, carriesDroppedEmbeddings(&Node{EmbedMeta: map[string]any{"has_embedding": true}}, dropped))
	require.False(t, carriesDroppedEmbeddings(&Node{ChunkEmbeddings: [][]float32{{0.5, 0.6}}}, dropped))
	require.False(t, carriesDroppedEmbeddings(&Node{}, dropped))
}
