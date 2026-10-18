package embed

import (
	"context"
	"fmt"
	"log/slog"
)

// forProviderBatches owns request partitioning. We use UTF-8 bytes as an upper
// bound on text tokens, capped at the model's per-input limit: an input beyond
// that limit is either truncated or rejected by the provider as before.
// A zero limit means the provider documents no aggregate bound.
func forProviderBatches(ctx context.Context, count, maxInputs, maxTokens int, tokens func(int) int, send func(int, int) error) error {
	for start := 0; start < count; {
		if err := ctx.Err(); err != nil {
			return err
		}
		end, total := start, 0
		for end < count && (maxInputs == 0 || end-start < maxInputs) {
			n := tokens(end)
			if end > start && maxTokens > 0 && total+n > maxTokens {
				break
			}
			total += n
			end++
		}
		if err := send(start, end); err != nil {
			return fmt.Errorf("embedding inputs %d..%d: %w", start, end-1, err)
		}
		start = end
	}
	return nil
}

// Provider responses contain request totals, not per-input attribution. Log
// once per successful response, including responses later rejected as unusable,
// so billable work isn't lost or multiplied by the document count.
func recordEmbeddingUsage(ctx context.Context, provider, model string, inputs, totalTokens int) {
	slog.DebugContext(ctx, "embedding provider usage", "provider", provider, "model", model, "inputs", inputs, "total_tokens", totalTokens)
}
