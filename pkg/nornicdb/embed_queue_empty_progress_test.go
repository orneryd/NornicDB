package nornicdb

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type reviewEmptyResultProvider struct{ zero bool }

func (*reviewEmptyResultProvider) Model() string   { return "empty-result-progress" }
func (*reviewEmptyResultProvider) Dimensions() int { return 2 }
func (*reviewEmptyResultProvider) Backend() string { return "cpu" }
func (*reviewEmptyResultProvider) Close() error    { return nil }
func (*reviewEmptyResultProvider) ChunkText(text string, _, _ int) ([]string, error) {
	return []string{text}, nil
}
func (p *reviewEmptyResultProvider) Embed(context.Context, string) ([]float32, error) {
	if p.zero {
		return []float32{}, nil
	}
	return nil, nil
}

func (p *reviewEmptyResultProvider) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	result := make([][]float32, len(texts))
	for i, text := range texts {
		result[i], _ = p.Embed(ctx, text)
	}
	return result, nil
}

type reviewEmptyDocumentProvider struct {
	*reviewEmptyResultProvider
	nilDocument bool
	noVectors   bool
}

func (p *reviewEmptyDocumentProvider) EmbedDocumentBatchChunks(ctx context.Context, texts []string, _, _ int) ([]*embed.DocumentChunkResult, error) {
	result := make([]*embed.DocumentChunkResult, len(texts))
	for i, text := range texts {
		if !p.nilDocument {
			vector, _ := p.Embed(ctx, text)
			result[i] = &embed.DocumentChunkResult{Chunks: []string{text}, Model: p.Model()}
			if !p.noVectors {
				result[i].Embeddings = [][]float32{vector}
			}
		}
	}
	return result, nil
}

// Exercise real provider dispatch, claims, retry deadlines and terminal writes;
// the shared owner's progress decision must agree for every empty result shape.
func TestEmbedWorkerEmptyResultRetryProgress(t *testing.T) {
	for _, mode := range []string{"local_nil", "local_zero", "document_no_vectors", "document_nil_vector", "document_zero_vector", "document_nil_result", "resolved_local", "resolved_document"} {
		for _, rejectMarker := range []bool{false, true} {
			name := mode + "/accepted_marker"
			if rejectMarker {
				name = mode + "/rejected_marker"
			}
			t.Run(name, func(t *testing.T) {
				rejections := 0
				if rejectMarker {
					rejections = 1
				}
				worker, queue, _ := nodeSaveRetryFixture(t, "whole", 1, rejections)
				worker.config.MaxRetries = 2
				worker.config.ProviderRetryBackoff = time.Hour
				worker.config.ProviderRetryBackoffMax = time.Hour
				base := &reviewEmptyResultProvider{zero: mode == "local_zero" || mode == "document_zero_vector"}
				var provider embed.Embedder = base
				switch mode {
				case "document_no_vectors", "document_nil_vector", "document_zero_vector", "document_nil_result", "resolved_document":
					provider = &reviewEmptyDocumentProvider{reviewEmptyResultProvider: base, nilDocument: mode == "document_nil_result", noVectors: mode == "document_no_vectors"}
				}
				worker.SetEmbedder(provider)
				if mode == "resolved_document" || mode == "resolved_local" {
					worker.SetEmbedderResolver(func(_ storage.NodeID) (embed.Embedder, error) { return provider, nil })
				}
				require.Equal(t, embedBatchWorked, worker.processNextBatchResult(), "MAIN-to-DLQ admission is progress")
				require.Equal(t, int64(1), worker.failed.Load())
				require.Equal(t, embedBatchIdle, worker.processNextBatchResult(), "backoff cannot perform another request")
				makeNodeSaveRetriesReady(worker)
				assert.Equal(t, embedBatchIdle, worker.processNextBatchResult(), "a nonterminal repeated empty result is not progress")
				require.Equal(t, int64(2), worker.failed.Load())
				require.Zero(t, worker.Stats().Parked)
				require.Zero(t, queue.attempts["node-000"], "the retry allowance is not exhausted")
				makeNodeSaveRetriesReady(worker)
				assert.Equal(t, embedBatchIdle, worker.processNextBatchResult(), "terminal marker handling uses the same failed-retry decision")
				if rejectMarker {
					require.Zero(t, worker.Stats().Parked)
					require.Contains(t, worker.nodeRetries, storage.NodeID("node-000"), "a rejected marker keeps retry ownership")
					makeNodeSaveRetriesReady(worker)
					assert.Equal(t, embedBatchIdle, worker.processNextBatchResult(), "a later accepted marker cannot invent embedding progress")
				}
				require.Equal(t, 1, worker.Stats().Parked)
				require.Empty(t, worker.nodeRetries)
				saved, err := queue.GetNode("node-000")
				require.NoError(t, err)
				require.Equal(t, true, saved.EmbedMeta["embedding_failed"])
				require.Empty(t, saved.ChunkEmbeddings)
			})
		}
	}
}
