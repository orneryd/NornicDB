package embed

import (
	"context"
	"log/slog"
)

// Provider request partitioning is owned by internal/providerbatch so the
// embedding and rerank providers share one implementation.

// mergeProviderBatch reuses a complete response and allocates an aggregate
// destination only when multiple requests must be combined.
func mergeProviderBatch(results, vectors [][]float32, start, count int) [][]float32 {
	if len(vectors) == count {
		return vectors
	}
	if results == nil {
		results = make([][]float32, count)
	}
	copy(results[start:], vectors)
	return results
}

// Provider responses contain request totals, not per-input attribution. Log
// once per successful response, including responses later rejected as unusable,
// so billable work isn't lost or multiplied by the document count.
func recordEmbeddingUsage(ctx context.Context, provider, model string, inputs, totalTokens int) {
	if !slog.Default().Enabled(ctx, slog.LevelDebug) {
		return
	}
	slog.DebugContext(ctx, "embedding provider usage", "provider", provider, "model", model, "inputs", inputs, "total_tokens", totalTokens)
}

// providerBatchUsage keeps the provider's request total with the corresponding
// half-open range of embedding inputs until document boundaries are restored.
type providerBatchUsage struct {
	start, end, totalTokens int
}

// attributeDocumentUsage credits only requests whose entire input range belongs
// to one document. Multiple chunks and multiple requests may share that owner;
// requests crossing a document boundary remain attributable only in debug logs.
func attributeDocumentUsage(results []*DocumentChunkResult, usage []providerBatchUsage) {
	if len(usage) == 0 {
		return
	}
	start, request := 0, 0
	for _, result := range results {
		end := start + len(result.Embeddings)
		for request < len(usage) && usage[request].end <= end {
			if usage[request].start >= start {
				result.TotalTokens += usage[request].totalTokens
			}
			request++
		}
		start = end
	}
}
