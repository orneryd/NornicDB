package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type inlineEmbeddingTestEmbedder struct {
	model string
	dims  int
}

func (e *inlineEmbeddingTestEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	out := make([]float32, e.dims)
	for i := range out {
		out[i] = float32(len(text) + i)
	}
	return out, nil
}

func (e *inlineEmbeddingTestEmbedder) ChunkText(text string, _ int, _ int) ([]string, error) {
	if len(text) > 10 {
		return []string{text[:len(text)/2], text[len(text)/2:]}, nil
	}
	return []string{text}, nil
}

func (e *inlineEmbeddingTestEmbedder) Model() string   { return e.model }
func (e *inlineEmbeddingTestEmbedder) Dimensions() int { return e.dims }
func (e *inlineEmbeddingTestEmbedder) Backend() string { return "cpu" } // Plan 04-05 D-06

func TestWithEmbedding_CreateMutation(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	exec.SetEmbedder(&inlineEmbeddingTestEmbedder{model: "test-inline", dims: 3})

	ctx := context.Background()
	res, err := exec.Execute(ctx, "CREATE (n:Doc {id:'d1', content:'hello world'}) WITH EMBEDDING RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)

	nodes, err := store.GetNodesByLabel("Doc")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.NotEmpty(t, nodes[0].ChunkEmbeddings)
	require.NotNil(t, nodes[0].EmbedMeta)
	require.Equal(t, true, nodes[0].EmbedMeta["has_embedding"])
	require.Equal(t, "test-inline", nodes[0].EmbedMeta["embedding_model"])
	require.Equal(t, 3, nodes[0].EmbedMeta["embedding_dimensions"])
}

func TestWithEmbedding_MultiCreateMutation(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	exec.SetEmbedder(&inlineEmbeddingTestEmbedder{model: "test-inline", dims: 3})

	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (a:Doc {id:'a1', content:'alpha text'}), (b:Doc {id:'b1', content:'beta text'}) WITH EMBEDDING RETURN count(*) AS created", nil)
	require.NoError(t, err)

	nodes, err := store.GetNodesByLabel("Doc")
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	for _, n := range nodes {
		require.NotEmpty(t, n.ChunkEmbeddings)
		require.NotNil(t, n.EmbedMeta)
		require.Equal(t, true, n.EmbedMeta["has_embedding"])
	}
}

func TestWithEmbedding_MergeSetMutation(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	exec.SetEmbedder(&inlineEmbeddingTestEmbedder{model: "test-inline", dims: 4})

	ctx := context.Background()
	_, err := exec.Execute(ctx, "MERGE (n:Doc {id:'m1'}) SET n.content='seed content' WITH EMBEDDING RETURN count(n) AS c", nil)
	require.NoError(t, err)

	node, err := store.GetFirstNodeByLabel("Doc")
	require.NoError(t, err)
	require.NotNil(t, node)
	require.NotEmpty(t, node.ChunkEmbeddings)
	require.NotNil(t, node.EmbedMeta)
	require.Equal(t, "test-inline", node.EmbedMeta["embedding_model"])
	require.EqualValues(t, 4, node.EmbedMeta["embedding_dimensions"])
}

func TestWithEmbedding_MatchSetMutation(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	exec.SetEmbedder(&inlineEmbeddingTestEmbedder{model: "test-inline", dims: 4})

	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (n:Doc {id:'d1', content:'old'}) RETURN n", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "MATCH (n:Doc {id:'d1'}) SET n.content='updated text', n.version=2 WITH EMBEDDING RETURN n.id", nil)
	require.NoError(t, err)

	nodes, err := store.GetNodesByLabel("Doc")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.NotEmpty(t, nodes[0].ChunkEmbeddings)
	require.NotNil(t, nodes[0].EmbedMeta)
	require.EqualValues(t, 2, nodes[0].Properties["version"])
}

func TestWithEmbedding_UnwindMergePipeline(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	exec.SetEmbedder(&inlineEmbeddingTestEmbedder{model: "test-inline", dims: 2})

	ctx := context.Background()
	params := map[string]interface{}{
		"hops": []interface{}{
			map[string]interface{}{"hopId": "h1", "runID": "r1"},
			map[string]interface{}{"hopId": "h2", "runID": "r1"},
		},
	}
	_, err := exec.Execute(ctx, "UNWIND $hops AS hop MERGE (h:BenchmarkHop {hopId: hop.hopId}) SET h.benchmarkRun = hop.runID WITH EMBEDDING RETURN count(h) AS prepared", params)
	require.NoError(t, err)

	nodes, err := store.GetNodesByLabel("BenchmarkHop")
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	for _, n := range nodes {
		require.NotEmpty(t, n.ChunkEmbeddings)
		require.NotNil(t, n.EmbedMeta)
		require.Equal(t, true, n.EmbedMeta["has_embedding"])
	}
}

func TestWithEmbedding_ExplicitTransaction(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	exec.SetEmbedder(&inlineEmbeddingTestEmbedder{model: "test-inline", dims: 3})

	ctx := context.Background()
	_, err := exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (n:TxDoc {id:'tx1', content:'tx content'}) WITH EMBEDDING RETURN count(n)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)

	nodes, err := store.GetNodesByLabel("TxDoc")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.NotEmpty(t, nodes[0].ChunkEmbeddings)
	require.NotNil(t, nodes[0].EmbedMeta)
}

func TestWithEmbedding_ANTLRMode(t *testing.T) {
	cleanup := config.WithANTLRParser()
	defer cleanup()

	t.Run("create_return", func(t *testing.T) {
		base := newTestMemoryEngine(t)
		store := storage.NewNamespacedEngine(base, "test")
		exec := NewStorageExecutor(store)
		exec.SetEmbedder(&inlineEmbeddingTestEmbedder{model: "test-inline", dims: 3})

		ctx := context.Background()
		_, err := exec.Execute(ctx, "CREATE (n:Doc {id:'antlr-create', content:'hello world'}) WITH EMBEDDING RETURN count(n) AS c", nil)
		require.NoError(t, err)

		nodes, err := store.GetNodesByLabel("Doc")
		require.NoError(t, err)
		require.Len(t, nodes, 1)
		require.NotEmpty(t, nodes[0].ChunkEmbeddings)
		require.NotNil(t, nodes[0].EmbedMeta)
	})

	t.Run("create_without_return", func(t *testing.T) {
		base := newTestMemoryEngine(t)
		store := storage.NewNamespacedEngine(base, "test")
		exec := NewStorageExecutor(store)
		exec.SetEmbedder(&inlineEmbeddingTestEmbedder{model: "test-inline", dims: 2})

		ctx := context.Background()
		_, err := exec.Execute(ctx, "CREATE (n:Doc {id:'antlr-no-return', content:'standalone'}) WITH EMBEDDING", nil)
		require.NoError(t, err)

		nodes, err := store.GetNodesByLabel("Doc")
		require.NoError(t, err)
		require.Len(t, nodes, 1)
		require.NotEmpty(t, nodes[0].ChunkEmbeddings)
		require.NotNil(t, nodes[0].EmbedMeta)
	})
}
