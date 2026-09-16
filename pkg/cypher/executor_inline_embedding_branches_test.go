package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type queryEmbedderStub struct {
	embedFn      func(ctx context.Context, text string) ([]float32, error)
	typedEmbedFn func(ctx context.Context, text, inputType string) ([]float32, error)
	chunkFn      func(text string, maxTokens, overlap int) ([]string, error)
}

func (q *queryEmbedderStub) Embed(ctx context.Context, text string) ([]float32, error) {
	if q.embedFn != nil {
		return q.embedFn(ctx, text)
	}
	return []float32{1, 2}, nil
}

func (q *queryEmbedderStub) EmbedWithInputType(ctx context.Context, text, inputType string) ([]float32, error) {
	if q.typedEmbedFn != nil {
		return q.typedEmbedFn(ctx, text, inputType)
	}
	return q.Embed(ctx, text)
}

func (q *queryEmbedderStub) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	if q.chunkFn != nil {
		return q.chunkFn(text, maxTokens, overlap)
	}
	return []string{text}, nil
}

type updateNodeErrEngine struct {
	storage.Engine
	err error
}

func (e *updateNodeErrEngine) UpdateNode(node *storage.Node) error {
	if e.err != nil {
		return e.err
	}
	return e.Engine.UpdateNode(node)
}

type getNodeErrEngine struct {
	storage.Engine
	err error
}

func (e *getNodeErrEngine) GetNode(id storage.NodeID) (*storage.Node, error) {
	if e.err != nil {
		return nil, e.err
	}
	return e.Engine.GetNode(id)
}

func TestApplyInlineEmbeddingMutations_Branches(t *testing.T) {
	ctx := context.Background()

	t.Run("empty_ids_noop", func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "inline_embed_empty_ids"))
		require.NoError(t, exec.applyInlineEmbeddingMutations(ctx, map[string]struct{}{}))
	})

	t.Run("missing_embedder", func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "inline_embed_no_embedder"))
		err := exec.applyInlineEmbeddingMutations(ctx, map[string]struct{}{"n1": {}})
		require.Error(t, err)
		require.Contains(t, err.Error(), "WITH EMBEDDING requires configured embedder")
	})

	t.Run("missing_node_not_found_is_ignored", func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "inline_embed_missing_node"))
		exec.SetEmbedder(&queryEmbedderStub{})
		require.NoError(t, exec.applyInlineEmbeddingMutations(ctx, map[string]struct{}{"missing": {}}))
	})

	t.Run("get_node_error_surfaces", func(t *testing.T) {
		boom := errors.New("get node boom")
		base := storage.NewNamespacedEngine(newTestMemoryEngine(t), "inline_embed_get_err")
		exec := NewStorageExecutor(&getNodeErrEngine{Engine: base, err: boom})
		exec.SetEmbedder(&queryEmbedderStub{})
		err := exec.applyInlineEmbeddingMutations(ctx, map[string]struct{}{"n1": {}})
		require.ErrorIs(t, err, boom)
	})

	t.Run("chunk_error_surfaces", func(t *testing.T) {
		store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "inline_embed_chunk_err")
		n, err := store.CreateNode(&storage.Node{ID: storage.NodeID("n1"), Labels: []string{"Doc"}, Properties: map[string]interface{}{"content": "hello"}})
		require.NoError(t, err)
		exec := NewStorageExecutor(store)
		exec.SetEmbedder(&queryEmbedderStub{chunkFn: func(string, int, int) ([]string, error) {
			return nil, errors.New("chunk boom")
		}})
		err = exec.applyInlineEmbeddingMutations(ctx, map[string]struct{}{string(n): {}})
		require.Error(t, err)
		require.Contains(t, err.Error(), "chunking failed")
	})

	t.Run("embed_error_surfaces", func(t *testing.T) {
		store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "inline_embed_embed_err")
		n, err := store.CreateNode(&storage.Node{ID: storage.NodeID("n1"), Labels: []string{"Doc"}, Properties: map[string]interface{}{"content": "hello"}})
		require.NoError(t, err)
		exec := NewStorageExecutor(store)
		exec.SetEmbedder(&queryEmbedderStub{embedFn: func(context.Context, string) ([]float32, error) {
			return nil, errors.New("embed boom")
		}})
		err = exec.applyInlineEmbeddingMutations(ctx, map[string]struct{}{string(n): {}})
		require.Error(t, err)
		require.Contains(t, err.Error(), "embed failed")
	})

	t.Run("empty_embed_vector_surfaces", func(t *testing.T) {
		store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "inline_embed_empty_vector")
		n, err := store.CreateNode(&storage.Node{ID: storage.NodeID("n1"), Labels: []string{"Doc"}, Properties: map[string]interface{}{"content": "hello"}})
		require.NoError(t, err)
		exec := NewStorageExecutor(store)
		exec.SetEmbedder(&queryEmbedderStub{embedFn: func(context.Context, string) ([]float32, error) {
			return []float32{}, nil
		}})
		err = exec.applyInlineEmbeddingMutations(ctx, map[string]struct{}{string(n): {}})
		require.Error(t, err)
		require.Contains(t, err.Error(), "empty vector")
	})

	t.Run("update_node_error_surfaces", func(t *testing.T) {
		boom := errors.New("update boom")
		base := storage.NewNamespacedEngine(newTestMemoryEngine(t), "inline_embed_update_err")
		n, err := base.CreateNode(&storage.Node{ID: storage.NodeID("n1"), Labels: []string{"Doc"}, Properties: map[string]interface{}{"content": "hello"}})
		require.NoError(t, err)
		exec := NewStorageExecutor(&updateNodeErrEngine{Engine: base, err: boom})
		exec.SetEmbedder(&queryEmbedderStub{})
		err = exec.applyInlineEmbeddingMutations(ctx, map[string]struct{}{string(n): {}})
		require.ErrorIs(t, err, boom)
	})

	t.Run("typed_embedder_uses_document_input_type", func(t *testing.T) {
		store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "inline_embed_document_type")
		n, err := store.CreateNode(&storage.Node{ID: storage.NodeID("n1"), Labels: []string{"Doc"}, Properties: map[string]interface{}{"content": "hello"}})
		require.NoError(t, err)
		var gotInputTypes []string
		exec := NewStorageExecutor(store)
		exec.SetEmbedder(&queryEmbedderStub{
			chunkFn: func(string, int, int) ([]string, error) {
				return []string{"chunk one", "chunk two"}, nil
			},
			typedEmbedFn: func(ctx context.Context, text, inputType string) ([]float32, error) {
				gotInputTypes = append(gotInputTypes, inputType)
				return []float32{1, 2}, nil
			},
		})
		err = exec.applyInlineEmbeddingMutations(ctx, map[string]struct{}{string(n): {}})
		require.NoError(t, err)
		require.Equal(t, []string{"document", "document"}, gotInputTypes)
	})
}

func TestExecutorConfusableAndEmbeddingSuffixHelpers(t *testing.T) {
	cases := []rune{'→', '←', '—', '（', '）', '［', '］', '｛', '｝', '，', '：', '；', '．', '＝', '＜', '＞', '＄'}
	for _, r := range cases {
		repl, ok := cypherSyntaxConfusableReplacement(r)
		require.True(t, ok)
		require.NotEmpty(t, repl)
	}
	_, ok := cypherSyntaxConfusableReplacement('A')
	require.False(t, ok)

	stripped, found := stripWithEmbeddingSuffix("CREATE (n) WITH EMBEDDING RETURN n")
	require.True(t, found)
	require.Equal(t, "CREATE (n) RETURN n", stripped)

	stripped, found = stripWithEmbeddingSuffix("CREATE (n) WITH EMBEDDING")
	require.True(t, found)
	require.Equal(t, "CREATE (n)", stripped)

	stripped, found = stripWithEmbeddingSuffix("WITH EMBEDDING RETURN n")
	require.False(t, found)
	require.Equal(t, "WITH EMBEDDING RETURN n", stripped)

	stripped, found = stripWithEmbeddingSuffix("CREATE (n)")
	require.False(t, found)
	require.Equal(t, "CREATE (n)", stripped)
}
