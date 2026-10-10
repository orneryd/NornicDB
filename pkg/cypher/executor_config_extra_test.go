package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/vectorspace"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testEmbedder struct{}

func (testEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	return []float32{1, 2, 3}, nil
}

func (testEmbedder) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	return chunkTestText(text, maxTokens, overlap)
}

type testInferenceManager struct{}

func (testInferenceManager) Generate(ctx context.Context, prompt string, params heimdall.GenerateParams) (string, error) {
	return "ok", nil
}

func (testInferenceManager) Chat(ctx context.Context, req heimdall.ChatRequest) (*heimdall.ChatResponse, error) {
	return &heimdall.ChatResponse{ID: "test", Object: "chat.completion", Model: "mock"}, nil
}

func TestStorageExecutor_ConfigSettersAndFlush(t *testing.T) {
	inner := newTestMemoryEngine(t)
	defer inner.Close()
	async := inner
	defer async.Close()

	exec := NewStorageExecutor(async)
	require.NotNil(t, exec)

	// Vector registry
	reg := vectorspace.NewIndexRegistry()
	exec.SetVectorRegistry(reg)
	assert.Same(t, reg, exec.GetVectorRegistry())
	exec.SetVectorRegistry(nil)
	assert.NotNil(t, exec.GetVectorRegistry())

	// Embedder
	emb := testEmbedder{}
	exec.SetEmbedder(emb)
	assert.NotNil(t, exec.GetEmbedder())

	// Inference manager
	mgr := testInferenceManager{}
	exec.SetInferenceManager(mgr)
	assert.NotNil(t, exec.GetInferenceManager())

	// Dimensions + defer flush
	exec.SetDefaultEmbeddingDimensions(256)
	assert.Equal(t, 256, exec.GetDefaultEmbeddingDimensions())
	exec.SetDeferFlush(true)
	assert.True(t, exec.deferFlush)
	exec.SetDeferFlush(false)
	assert.False(t, exec.deferFlush)

	// Flush on async engine path
	err := exec.Flush()
	require.NoError(t, err)
}

func TestStorageExecutor_NodeMutatedAndUseDatabaseContext(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))

	var got string
	exec.SetNodeMutatedCallback(func(nodeID string) {
		got = nodeID
	})
	exec.notifyNodeMutated("node-123")
	assert.Equal(t, "node-123", got)

	// No callback should be safe
	exec.SetNodeMutatedCallback(nil)
	exec.notifyNodeMutated("node-456")
	assert.Equal(t, "node-123", got)

	// Context extraction helper
	assert.Equal(t, "", GetUseDatabaseFromContext(context.Background()))
	ctx := context.WithValue(context.Background(), ctxKeyUseDatabase, "tenant_a")
	assert.Equal(t, "tenant_a", GetUseDatabaseFromContext(ctx))
}
