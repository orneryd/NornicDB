package nornicdb

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/embeddingsource"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type acceptanceLocalProvider struct {
	shape     string
	documents map[string]int
}

func (*acceptanceLocalProvider) Model() string   { return "acceptance-owner" }
func (*acceptanceLocalProvider) Dimensions() int { return 2 }
func (*acceptanceLocalProvider) Backend() string { return "cpu" }
func (*acceptanceLocalProvider) Close() error    { return nil }
func (*acceptanceLocalProvider) ChunkText(string, int, int) ([]string, error) {
	return []string{"first chunk", "second chunk"}, nil
}
func (p *acceptanceLocalProvider) Embed(context.Context, string) ([]float32, error) {
	vectors := p.vectors(1, p.shape)
	if len(vectors) == 0 {
		return nil, nil
	}
	return vectors[0], nil
}
func (p *acceptanceLocalProvider) EmbedBatch(_ context.Context, texts []string) ([][]float32, error) {
	return p.vectors(len(texts), p.shape), nil
}
func (*acceptanceLocalProvider) vectors(count int, shape string) [][]float32 {
	vectors := make([][]float32, count)
	for i := range vectors {
		vectors[i] = []float32{1, 0}
	}
	if count == 0 {
		return vectors
	}
	last := count - 1
	switch shape {
	case "nil_later":
		vectors[last] = nil
	case "empty_later":
		vectors[last] = []float32{}
	case "nan_later":
		vectors[last][1] = float32(math.NaN())
	case "posinf_later":
		vectors[last][1] = float32(math.Inf(1))
	case "neginf_later":
		vectors[last][1] = float32(math.Inf(-1))
	case "wrong_dimension":
		vectors[last] = []float32{1}
	case "too_few":
		vectors = vectors[:last]
	case "too_many":
		vectors = append(vectors, []float32{0, 1})
	}
	return vectors
}

type acceptanceTypedProvider struct{ *acceptanceLocalProvider }

func (p *acceptanceTypedProvider) EmbedWithInputType(ctx context.Context, text, _ string) ([]float32, error) {
	return p.Embed(ctx, text)
}
func (p *acceptanceTypedProvider) EmbedBatchWithInputType(ctx context.Context, texts []string, _ string) ([][]float32, error) {
	return p.EmbedBatch(ctx, texts)
}

type acceptanceDocumentProvider struct{ *acceptanceLocalProvider }

func (p *acceptanceDocumentProvider) document(text string) *embed.DocumentChunkResult {
	p.documents[text]++
	shape := p.shape
	if strings.Contains(text, "healthy") {
		shape = "valid"
	}
	result := &embed.DocumentChunkResult{Chunks: []string{"first chunk", "second chunk"}, Embeddings: p.vectors(2, shape), Model: p.Model()}
	if shape == "image_only" {
		result.Chunks = nil
		result.Source = &embeddingsource.Source{Properties: []string{"_embedding_content"}}
	}
	return result
}
func (p *acceptanceDocumentProvider) EmbedDocumentChunks(_ context.Context, text string, _, _ int) (*embed.DocumentChunkResult, error) {
	return p.document(text), nil
}
func (p *acceptanceDocumentProvider) EmbedDocumentBatchChunks(_ context.Context, texts []string, _, _ int) ([]*embed.DocumentChunkResult, error) {
	results := make([]*embed.DocumentChunkResult, len(texts))
	for i, text := range texts {
		results[i] = p.document(text)
	}
	return results, nil
}

type acceptancePropertyProvider struct{ *acceptanceDocumentProvider }

func (*acceptancePropertyProvider) UsesDocumentProperties() bool { return true }
func (p *acceptancePropertyProvider) EmbedDocumentPropertyChunks(_ context.Context, text string, _ map[string]any, _, _ int) (*embed.DocumentChunkResult, error) {
	return p.document(text), nil
}
func (p *acceptancePropertyProvider) EmbedDocumentPropertyBatchChunks(_ context.Context, texts []string, _ []map[string]any, _, _ int) ([]*embed.DocumentChunkResult, error) {
	results := make([]*embed.DocumentChunkResult, len(texts))
	for i, text := range texts {
		results[i] = p.document(text)
	}
	return results, nil
}

func acceptanceWorker(t *testing.T, mode, shape string, nodes, rejections int) (*EmbedWorker, *rejectingSidecarQueue, *acceptanceLocalProvider) {
	t.Helper()
	worker, queue, _ := nodeSaveRetryFixture(t, "whole", nodes, rejections)
	worker.config.MaxRetries = 2
	worker.config.ProviderRetryBackoff = time.Hour
	worker.config.ProviderRetryBackoffMax = time.Hour
	base := &acceptanceLocalProvider{shape: shape, documents: make(map[string]int)}
	var provider embed.Embedder = base
	if mode == "typed_local" {
		provider = &acceptanceTypedProvider{base}
	} else if mode != "local" {
		document := &acceptanceDocumentProvider{base}
		provider = document
		if strings.Contains(mode, "structured") {
			provider = &acceptancePropertyProvider{document}
		}
	}
	if strings.Contains(mode, "cached") {
		provider = embed.NewCachedEmbedder(provider, 8)
	}
	if strings.Contains(mode, "traced") {
		provider = embed.NewTracedEmbedder(provider)
	}
	if strings.Contains(mode, "single") {
		worker.config.EmbedBatchSize = 1
	}
	worker.SetEmbedder(provider)
	if strings.Contains(mode, "resolved") || strings.Contains(mode, "grouped") {
		worker.SetEmbedderResolver(func(storage.NodeID) (embed.Embedder, error) { return provider, nil })
	}
	return worker, queue, base
}

// Real dispatch/claims and NamespacedEngine/Badger outcomes must agree on
// usability for the WHOLE result, before any success mutation/publication.
func TestEmbedWorkerResultAcceptance(t *testing.T) {
	modes := []string{"local", "typed_local", "document_single", "document_batch", "resolved_single", "resolved_batch", "structured_single", "resolved_structured_single", "grouped_structured", "cached_document_single", "cached_document_batch", "cached_resolved_batch", "cached_structured_single", "cached_grouped_structured", "traced_cached_document_batch"}
	for _, mode := range modes {
		for _, shape := range []string{"valid", "image_only", "nil_later", "empty_later", "nan_later", "posinf_later", "neginf_later", "wrong_dimension", "too_few", "too_many"} {
			if shape == "image_only" && (mode == "local" || mode == "typed_local") {
				continue
			}
			for _, rejectMarker := range []bool{false, true} {
				if rejectMarker && (shape == "valid" || shape == "image_only") {
					continue
				}
				name := mode + "/" + shape + "/accepted_marker"
				if rejectMarker {
					name = mode + "/" + shape + "/rejected_marker"
				}
				t.Run(name, func(t *testing.T) {
					rejections := 0
					if rejectMarker {
						rejections = 1
					}
					worker, queue, _ := acceptanceWorker(t, mode, shape, 1, rejections)
					published := 0
					worker.SetOnEmbedded(func(*storage.Node) { published++ })
					require.Equal(t, embedBatchWorked, worker.processNextBatchResult())
					saved, err := queue.GetNode("node-000")
					require.NoError(t, err)
					if shape == "valid" || shape == "image_only" {
						require.Equal(t, int64(1), worker.processed.Load())
						require.Equal(t, 1, published)
						require.Len(t, saved.ChunkEmbeddings, 2)
						require.Equal(t, true, saved.EmbedMeta["has_embedding"])
						require.Empty(t, worker.nodeRetries)
						return
					}
					require.Zero(t, published, "unusable whole results cannot publish completion")
					require.Zero(t, worker.processed.Load())
					require.Equal(t, int64(1), worker.failed.Load())
					require.Len(t, worker.nodeRetries, 1)
					require.Equal(t, 1, worker.PendingEmbeddingsCount())
					require.Zero(t, queue.attempts["node-000"], "no vector or metadata commit before complete acceptance")
					require.Empty(t, saved.ChunkEmbeddings)
					require.Empty(t, saved.EmbedMeta)
					require.Equal(t, embedBatchIdle, worker.processNextBatchResult(), "not-due DLQ does no work")
					makeNodeSaveRetriesReady(worker)
					require.Equal(t, embedBatchIdle, worker.processNextBatchResult(), "repeated rejection uses shared failure progress")
					require.Equal(t, int64(2), worker.failed.Load())
					require.Zero(t, queue.attempts["node-000"])
					makeNodeSaveRetriesReady(worker)
					require.Equal(t, embedBatchIdle, worker.processNextBatchResult(), "terminal outcome uses shared failure progress")
					if rejectMarker {
						require.Contains(t, worker.nodeRetries, storage.NodeID("node-000"))
						require.Zero(t, worker.Stats().Parked)
						makeNodeSaveRetriesReady(worker)
						require.Equal(t, embedBatchIdle, worker.processNextBatchResult())
					}
					require.Empty(t, worker.nodeRetries)
					require.Equal(t, 1, worker.Stats().Parked)
					saved, err = queue.GetNode("node-000")
					require.NoError(t, err)
					require.Empty(t, saved.ChunkEmbeddings)
					require.Equal(t, true, saved.EmbedMeta["embedding_failed"])
					require.Equal(t, false, saved.EmbedMeta["has_embedding"])
					require.Zero(t, published)
				})
			}
		}
	}
}

func TestEmbedWorkerResultAcceptanceHealthySibling(t *testing.T) {
	for _, mode := range []string{"document_batch", "resolved_batch", "grouped_structured", "cached_resolved_batch"} {
		t.Run(mode, func(t *testing.T) {
			worker, queue, provider := acceptanceWorker(t, mode, "nil_later", 2, 0)
			healthy, err := queue.GetNode("node-001")
			require.NoError(t, err)
			healthy.Properties["text"] = "healthy sibling document"
			require.NoError(t, queue.UpdateNode(storage.NodeUpdateRequest{After: healthy}))
			published := make(map[storage.NodeID]int)
			worker.SetOnEmbedded(func(node *storage.Node) { published[node.ID]++ })
			require.Equal(t, embedBatchWorked, worker.processNextBatchResult())
			require.Equal(t, int64(1), worker.processed.Load(), "only the usable sibling completes")
			require.Equal(t, 1, published["node-001"])
			require.Zero(t, published["node-000"])
			require.Contains(t, worker.nodeRetries, storage.NodeID("node-000"))
			for pass := 0; pass < 2; pass++ {
				makeNodeSaveRetriesReady(worker)
				assert.Equal(t, embedBatchIdle, worker.processNextBatchResult())
			}
			require.Equal(t, 1, published["node-001"])
			for text, count := range provider.documents {
				if strings.Contains(text, "healthy") {
					require.Equal(t, 1, count, "a successful sibling must not be requested again")
				}
			}
			require.Equal(t, 1, worker.Stats().Parked)
		})
	}
}

type acceptanceMisalignedBatchProvider struct{ *acceptanceLocalProvider }

func (*acceptanceMisalignedBatchProvider) ChunkText(string, int, int) ([]string, error) {
	return []string{"0", "1", "2", "3"}, nil
}
func (p *acceptanceMisalignedBatchProvider) EmbedBatch(_ context.Context, texts []string) ([][]float32, error) {
	count := len(texts)
	if texts[0] == "0" {
		count++
	} else {
		count--
	}
	return p.vectors(count, "valid"), nil
}

// A total-count-only replacement for the old micro-batch guard would let an
// extra vector in one response cancel a missing vector in the next response.
func TestEmbedWorkerResultAcceptanceMicroBatchCardinality(t *testing.T) {
	worker, queue, _ := nodeSaveRetryFixture(t, "whole", 1, 0)
	worker.config.EmbedBatchSize = 2
	worker.SetEmbedder(&acceptanceMisalignedBatchProvider{&acceptanceLocalProvider{shape: "valid"}})
	require.Equal(t, embedBatchWorked, worker.processNextBatchResult())
	require.Zero(t, worker.processed.Load(), "each micro-batch's input association must survive acceptance consolidation")
	require.Equal(t, int64(1), worker.failed.Load())
	require.Contains(t, worker.nodeRetries, storage.NodeID("node-000"))
	require.Zero(t, queue.attempts["node-000"])
}
