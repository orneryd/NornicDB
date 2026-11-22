package providerbatch

import (
	"context"
	"log/slog"
)

// RecordUsage reports the token accounting a provider returned for one request
// it completed: one record per completed request, so billable work is neither
// lost nor multiplied by the inputs a request carried and never replaced by a
// locally computed estimate.
//
// operation names what the request did ("embedding", "rerank"); inputs is the
// number of inputs that request carried. A caller records usage as soon as the
// provider response exists, before deciding whether its payload is usable, so a
// response later rejected as unusable still reports the tokens it cost.
func RecordUsage(ctx context.Context, operation, provider, model string, inputs, totalTokens int) {
	if !slog.Default().Enabled(ctx, slog.LevelDebug) {
		return
	}
	slog.DebugContext(ctx, operation+" provider usage", "provider", provider, "model", model, "inputs", inputs, "total_tokens", totalTokens)
}
