package nornicdb

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/embeddingutil"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// tilingDocumentEmbedder chunks a document on its side, as Voyage's
// contextualized auto-chunking does: verbatim chunks that tile the text,
// whitespace kept, with a chunker version (so the chunk texts are kept too).
type tilingDocumentEmbedder struct{}

func tileAtNotices(text string) []string {
	at := strings.Index(text, "Notices")
	return []string{text[:at], text[at:]}
}

func (tilingDocumentEmbedder) Embed(context.Context, string) ([]float32, error) {
	return []float32{1, 0}, nil
}
func (tilingDocumentEmbedder) EmbedBatch(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for index := range out {
		out[index] = []float32{1, 0}
	}
	return out, nil
}
func (tilingDocumentEmbedder) ChunkText(text string, _, _ int) ([]string, error) {
	return tileAtNotices(text), nil
}
func (tilingDocumentEmbedder) Dimensions() int { return 2 }
func (tilingDocumentEmbedder) Model() string   { return "context-model" }
func (tilingDocumentEmbedder) Backend() string { return "cpu" }
func (e tilingDocumentEmbedder) EmbedDocumentChunks(_ context.Context, text string, _, _ int) (*embed.DocumentChunkResult, error) {
	chunks := tileAtNotices(text)
	embeddings, _ := e.EmbedBatch(context.Background(), chunks)
	return &embed.DocumentChunkResult{Chunks: chunks, Embeddings: embeddings, Model: e.Model(), ChunkerVersion: "1.0.0"}, nil
}
func (e tilingDocumentEmbedder) EmbedDocumentBatchChunks(ctx context.Context, texts []string, maxTokens, overlap int) ([]*embed.DocumentChunkResult, error) {
	results := make([]*embed.DocumentChunkResult, len(texts))
	for index, text := range texts {
		results[index], _ = e.EmbedDocumentChunks(ctx, text, maxTokens, overlap)
	}
	return results, nil
}

// localChunkEmbedder has only NornicDB's deterministic chunking, whose
// chunks are trimmed.
type localChunkEmbedder struct{}

func (localChunkEmbedder) Embed(context.Context, string) ([]float32, error) {
	return []float32{1, 0}, nil
}
func (localChunkEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	return tilingDocumentEmbedder{}.EmbedBatch(ctx, texts)
}
func (localChunkEmbedder) Dimensions() int { return 2 }
func (localChunkEmbedder) Model() string   { return "local-model" }
func (localChunkEmbedder) Backend() string { return "cpu" }
func (localChunkEmbedder) ChunkText(text string, _, _ int) ([]string, error) {
	chunks := tileAtNotices(text)
	for index := range chunks {
		chunks[index] = strings.TrimSpace(chunks[index])
	}
	return chunks, nil
}

// Every worker path stores where each chunk lies in the node's string
// properties (#1060): the batches of resolved providers and of one
// provider, and single nodes chunked by the provider or locally.
func TestEmbedWorkerRecordsChunkSpans(t *testing.T) {
	want := []embeddingutil.ChunkSpan{
		{Chunk: 0, Property: "text", Start: 0, End: 20},
		{Chunk: 1, Property: "text", Start: 21, End: 42},
	}
	for name, setup := range map[string]func(*EmbedWorker){
		"resolved batch": func(worker *EmbedWorker) {
			worker.config.EmbedBatchSize = 32
			worker.SetEmbedderResolver(func(storage.NodeID) (embed.Embedder, error) { return tilingDocumentEmbedder{}, nil })
		},
		"document batch": func(worker *EmbedWorker) {
			worker.config.EmbedBatchSize = 32
			worker.embedder = tilingDocumentEmbedder{}
		},
		"single node, provider chunks": func(worker *EmbedWorker) {
			worker.config.EmbedBatchSize = 1
			worker.embedder = tilingDocumentEmbedder{}
		},
		"single node, local chunks": func(worker *EmbedWorker) {
			_, isDocumentEmbedder := any(localChunkEmbedder{}).(embed.DocumentChunkEmbedder)
			require.False(t, isDocumentEmbedder)
			worker.config.EmbedBatchSize = 1
			worker.embedder = localChunkEmbedder{}
		},
	} {
		t.Run(name, func(t *testing.T) {
			engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			node := &storage.Node{ID: "docs:a", Labels: []string{"Doc"}, Properties: map[string]any{"text": "Rent is due monthly. Notices go to Müller."}}
			_, err := engine.CreateNode(node)
			require.NoError(t, err)
			worker := &EmbedWorker{
				storage: &sequenceEmbeddingEngine{Engine: engine, nodes: []*storage.Node{node}},
				config:  &EmbedWorkerConfig{MaxRetries: 3, ChunkSize: 512},
				ctx:     context.Background(), trigger: make(chan struct{}, 1),
			}
			setup(worker)
			require.True(t, worker.processNextBatch())
			stored, err := engine.GetNode(node.ID)
			require.NoError(t, err)
			require.Len(t, stored.ChunkEmbeddings, 2)
			spans, ok := embeddingutil.StoredChunkSpans(stored.EmbedMeta)
			require.True(t, ok)
			require.Equal(t, want, spans)
		})
	}
}
