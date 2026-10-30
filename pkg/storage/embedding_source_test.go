package storage

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embeddingsource"
	"github.com/stretchr/testify/require"
)

func TestManagedEmbeddingSourceSelectorAllocations(t *testing.T) {
	source := &Node{Properties: map[string]any{"text": "same", "id": "consumed"}, EmbedMeta: map[string]any{embeddingsource.MetaKey: (embeddingsource.Source{Text: true, Include: []string{"text"}, Properties: []string{"id"}}).Metadata()}}
	current := &Node{Properties: map[string]any{"text": "same", "id": "consumed", "unconsumed": "new"}}
	require.Zero(t, testing.AllocsPerRun(100, func() {
		if !EmbeddingSourceMatches(source, current, EmbeddingTextPolicy{}) {
			panic("source changed")
		}
	}), "preserving comparisons must not allocate selector slices")
}

func TestManagedEmbeddingSourceTransaction(t *testing.T) {
	b := newSidecarTestBadger(t)
	b.SetEmbeddingsEnabled(true)
	b.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Include: []string{"text"}})
	node := sidecarTestNode(t, b, "source-contract", map[string]any{"text": "x", "id": "old", "updatedAt": "old"}, time.Now().UTC().Truncate(time.Millisecond))
	meta := map[string]any{"has_embedding": true, "chunk_count": 1, embeddingsource.MetaKey: (embeddingsource.Source{Text: true, Include: []string{"text"}, Properties: []string{"id"}}).Metadata()}
	require.NoError(t, b.UpdateNodeEmbeddingSidecar(embeddingWriteback(t, b, node.ID, [][]float32{{1, 2}}, meta, node.UpdatedAt)))
	update := func(key string) {
		tx, err := b.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, tx.SetNamespace("test"))
		n, err := tx.GetNode(node.ID)
		require.NoError(t, err)
		n.Properties[key] = "new"
		n.UpdatedAt = n.UpdatedAt.Add(time.Second)
		require.NoError(t, tx.UpdateGraphNode(n, ClientNodeEdits))
		require.NoError(t, tx.Commit())
	}
	update("updatedAt")
	n, err := b.GetNode(node.ID)
	require.NoError(t, err)
	require.NotEmpty(t, n.ChunkEmbeddings, "unconsumed metadata preserves readable managed vectors")
	update("id")
	n, err = b.GetNode(node.ID)
	require.NoError(t, err)
	require.Empty(t, n.ChunkEmbeddings, "consumed metadata invalidates managed vectors")
	require.NotNil(t, b.FindNodeNeedingEmbedding())
}

func TestManagedEmbeddingSourceAcceptance(t *testing.T) {
	for _, key := range []string{"id", "updatedAt"} {
		t.Run(key, func(t *testing.T) {
			b := newSidecarTestBadger(t)
			b.SetEmbeddingsEnabled(true)
			n := sidecarTestNode(t, b, "accept", map[string]any{"text": "x", "id": "old", "updatedAt": "old"}, time.Now().UTC().Truncate(time.Millisecond))
			meta := map[string]any{"has_embedding": true, "chunk_count": 1, embeddingsource.MetaKey: (embeddingsource.Source{Properties: []string{"id"}}).Metadata()}
			payload := embeddingWriteback(t, b, n.ID, [][]float32{{1, 2}}, meta, n.UpdatedAt)
			current, err := b.GetNode(n.ID)
			require.NoError(t, err)
			current.Properties[key] = "new"
			current.UpdatedAt = current.UpdatedAt.Add(time.Second)
			require.NoError(t, b.UpdateNode(current))
			err = b.UpdateNodeEmbeddingSidecar(payload)
			if key == "id" {
				require.ErrorIs(t, err, ErrEmbeddingSourceChanged)
				return
			}
			require.NoError(t, err)
			got, err := b.GetNode(n.ID)
			require.NoError(t, err)
			require.NotEmpty(t, got.ChunkEmbeddings, "acceptance must stamp the validated current body")
		})
	}
}

func TestManagedEmbeddingSourceExactTimestampValue(t *testing.T) {
	old := &Node{Properties: map[string]any{"updatedAt": int64(1792416500000000000)}, EmbedMeta: map[string]any{embeddingsource.MetaKey: (embeddingsource.Source{Properties: []string{"updatedAt"}}).Metadata()}}
	current := CopyNode(old)
	current.Properties["updatedAt"] = int64(1792416500000000001)
	require.False(t, EmbeddingSourceMatches(old, current, EmbeddingTextPolicy{}))
	current.Properties["updatedAt"] = float64(1792416500000000000)
	require.True(t, EmbeddingSourceMatches(old, current, EmbeddingTextPolicy{}))
	old.Properties["updatedAt"] = int64(1792416500000000001)
	require.False(t, EmbeddingSourceMatches(old, current, EmbeddingTextPolicy{}))
}
