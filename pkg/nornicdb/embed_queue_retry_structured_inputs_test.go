package nornicdb

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/embeddingsource"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type retryMetadataPropertyEmbedder struct {
	*mockEmbedder
	key      string
	declared bool
	chunked  bool
	textless bool
}

func (*retryMetadataPropertyEmbedder) UsesDocumentProperties() bool { return true }
func (p *retryMetadataPropertyEmbedder) EmbedDocumentPropertyChunks(ctx context.Context, text string, properties map[string]any, _, _ int) (*embed.DocumentChunkResult, error) {
	_, err := p.mockEmbedder.Embed(ctx, text)
	if err != nil {
		return nil, err
	}
	marker := float32(0)
	if properties[p.key] == "old" {
		marker = 1
	} else if properties[p.key] == "new" {
		marker = 2
	}
	result := &embed.DocumentChunkResult{Chunks: []string{text}, Embeddings: [][]float32{{marker, 1}}, Model: p.Model()}
	if p.declared {
		result.Source = &embeddingsource.Source{Text: true, Properties: []string{p.key}}
	}
	if p.chunked {
		result.Chunks = []string{text[:len(text)/2], text[len(text)/2:]}
		result.Embeddings = [][]float32{{marker, 1}, {marker, 1}}
	}
	if p.textless {
		result.Source = &embeddingsource.Source{Properties: []string{p.key}}
		result.Chunks = []string{"constant"}
	}
	return result, nil
}

type retryMetadataPropertyBatchEmbedder struct{ *retryMetadataPropertyEmbedder }

func (p *retryMetadataPropertyBatchEmbedder) EmbedDocumentPropertyBatchChunks(ctx context.Context, texts []string, properties []map[string]any, size, overlap int) ([]*embed.DocumentChunkResult, error) {
	results := make([]*embed.DocumentChunkResult, len(texts))
	for i := range texts {
		result, err := p.EmbedDocumentPropertyChunks(ctx, texts[i], properties[i], size, overlap)
		if err != nil {
			return nil, err
		}
		results[i] = result
	}
	return results, nil
}

func TestEmbedWorkerRetryStructuredMetadataInputs(t *testing.T) {
	for _, route := range []string{"direct-single", "resolver-single", "resolver-batch"} {
		for _, wrapper := range []string{"plain", "cached", "traced", "cached-traced", "traced-cached"} {
			for _, key := range []string{"id", "createdAt", "updatedAt"} {
				for _, change := range []string{"replace", "add", "remove", "unchanged"} {
					t.Run(route+"/"+wrapper+"/"+key+"/"+change, func(t *testing.T) {
						count := 1
						if route == "resolver-batch" {
							count = 2
						}
						worker, queue, base := nodeSaveRetryFixture(t, "whole", count, 1)
						ids := append([]storage.NodeID(nil), queue.pending...)
						for _, id := range ids {
							node, err := queue.GetNode(id)
							require.NoError(t, err)
							if change != "add" {
								node.Properties[key] = "old"
							}
							require.NoError(t, queue.UpdateNode(storage.NodeUpdateRequest{After: node}))
						}
						structured := &retryMetadataPropertyEmbedder{mockEmbedder: base, key: key}
						var provider embed.Embedder = structured
						worker.config.EmbedBatchSize = 1
						if route == "resolver-batch" {
							provider = &retryMetadataPropertyBatchEmbedder{structured}
							worker.config.EmbedBatchSize = 4
						}
						switch wrapper {
						case "cached":
							provider = embed.NewCachedEmbedder(provider, 8)
						case "traced":
							provider = embed.NewTracedEmbedder(provider)
						case "cached-traced":
							provider = embed.NewCachedEmbedder(embed.NewTracedEmbedder(provider), 8)
						case "traced-cached":
							provider = embed.NewTracedEmbedder(embed.NewCachedEmbedder(provider, 8))
						}
						worker.SetEmbedder(provider)
						if route != "direct-single" {
							worker.SetEmbedderResolver(func(storage.NodeID) (embed.Embedder, error) { return provider, nil })
						}
						require.True(t, worker.processNextBatch())
						require.Equal(t, count, base.GetEmbedCount())
						require.Zero(t, worker.processed.Load())
						for _, id := range ids {
							node, err := queue.GetNode(id)
							require.NoError(t, err)
							before := copyNodeForEmbedding(node)
							if change == "remove" {
								delete(node.Properties, key)
							} else if change != "unchanged" {
								node.Properties[key] = "new"
							}
							require.True(t, (storage.EmbeddingTextPolicy{IncludeLabels: true}).SameSource(before, node), "text policy omits this provider-visible input")
							require.NoError(t, queue.UpdateNode(storage.NodeUpdateRequest{After: node}))
						}

						// Every rejected save needs a fresh
						// provider attempt. MAIN-first collection processes those
						// DLQ attempts one cycle at a time; drain every affected ID.
						for pass := 0; pass < count && worker.processed.Load() < int64(count); pass++ {
							makeNodeSaveRetriesReady(worker)
							require.True(t, worker.processNextBatch())
						}
						require.Equal(t, int64(count), worker.processed.Load())
						marker, calls := float32(2), 2*count
						if change == "remove" {
							marker = 0
						} else if change == "unchanged" {
							marker = 1
						}
						for _, id := range ids {
							saved, err := queue.GetNode(id)
							require.NoError(t, err)
							require.Equal(t, marker, saved.ChunkEmbeddings[0][0], "stored vector must represent current structured inputs")
						}
						require.Equal(t, calls, base.GetEmbedCount(), "every rejected save requests current structured inputs again")
					})
				}
			}
		}
	}
}
