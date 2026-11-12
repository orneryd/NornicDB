// Package providerbatch owns request partitioning for provider APIs that bound
// a request by an input count and by an aggregate token budget.
//
// Text is counted in UTF-8 bytes, which is a conservative upper bound on the
// number of text tokens: a provider tokenizer never yields more tokens than the
// input has bytes. A zero limit means the provider documents no such bound, so
// that limit never splits a request.
package providerbatch

import (
	"context"
	"fmt"
)

// ForBatches splits count inputs into consecutive requests and sends each one
// exactly once, in order.
//
// maxInputs bounds the inputs in one request and maxTokens bounds the request's
// aggregate cost as reported by tokens; both values are the provider's own
// documented ceilings. An input that is larger than maxTokens on its own is
// still sent alone: the provider's own refusal stays visible instead of being
// hidden or pre-empted here, and no input text is shortened.
//
// A failing send stops the sequence and its error is returned with the failed
// input range, so a caller that must fail as a whole cannot mistake earlier
// successes for a result.
func ForBatches(ctx context.Context, operation string, count, maxInputs, maxTokens int, tokens func(int) int, send func(int, int) error) error {
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
			return fmt.Errorf("%s %d..%d: %w", operation, start, end-1, err)
		}
		start = end
	}
	return nil
}
