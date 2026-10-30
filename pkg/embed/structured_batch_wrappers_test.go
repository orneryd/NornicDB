package embed

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type structuredBatchProbe struct {
	batchCalls  int
	singleCalls int
}

func (p *structuredBatchProbe) Embed(context.Context, string) ([]float32, error) {
	return []float32{1}, nil
}

func (p *structuredBatchProbe) EmbedBatch(_ context.Context, texts []string) ([][]float32, error) {
	return make([][]float32, len(texts)), nil
}

func (p *structuredBatchProbe) EmbedDocumentPropertyChunks(_ context.Context, fallbackText string, _ map[string]any, _, _ int) (*DocumentChunkResult, error) {
	p.singleCalls++
	return &DocumentChunkResult{Chunks: []string{fallbackText}, Embeddings: [][]float32{{1}}, Model: p.Model()}, nil
}

func (p *structuredBatchProbe) EmbedDocumentPropertyBatchChunks(_ context.Context, fallbackTexts []string, _ []map[string]any, _, _ int) ([]*DocumentChunkResult, error) {
	p.batchCalls++
	results := make([]*DocumentChunkResult, len(fallbackTexts))
	for i, text := range fallbackTexts {
		results[i] = &DocumentChunkResult{Chunks: []string{text}, Embeddings: [][]float32{{1}}, Model: p.Model()}
	}
	return results, nil
}

func (*structuredBatchProbe) UsesDocumentProperties() bool { return true }
func (*structuredBatchProbe) ChunkText(text string, _, _ int) ([]string, error) {
	return []string{text}, nil
}
func (*structuredBatchProbe) Dimensions() int { return 1 }
func (*structuredBatchProbe) Model() string   { return "structured-batch-probe" }
func (*structuredBatchProbe) Backend() string { return "test" }

func TestEmbeddingWrappersPreserveStructuredDocumentBatching(t *testing.T) {
	provider := &structuredBatchProbe{}
	wrapped := NewTracedEmbedder(NewCachedEmbedder(provider, 16))
	batcher, ok := any(wrapped).(DocumentPropertyBatchChunkEmbedder)
	require.True(t, ok, "embedding wrappers must preserve structured batch capability")

	results, err := batcher.EmbedDocumentPropertyBatchChunks(
		context.Background(),
		[]string{"one", "two", "three"},
		[]map[string]any{{"image": "first"}, {"image": "second"}, {"image": "third"}},
		512,
		0,
	)
	require.NoError(t, err)
	require.Len(t, results, 3)
	require.Equal(t, 1, provider.batchCalls)
	require.Zero(t, provider.singleCalls)
}
