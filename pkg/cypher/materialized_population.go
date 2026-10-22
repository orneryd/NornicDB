package cypher

import (
	"context"
	"iter"
)

// materializedBatch is one cancellation interval of a materialized population:
// its items population[start:end].
type materializedBatch struct {
	start int
	end   int
}

// materializedBatches owns advancement over a materialized query population
// (rows, result rows, groups) of length n. Every pass over one goes through
// it, so a consumer cannot drain a large population after its query is
// canceled or expired: before each batch it observes ctx, and on cancellation
// it records the failure and yields that error once instead of another batch.
// The consumer returns its failure there; the rows it has not reached are
// never reported as a complete result. A nested pass (the inner rows of a
// product) is observed once per outer row, so the work between two
// observations stays one batch of the inner population.
//
// A single row is never checked: its streaming producer already observes ctx
// once per row (materializedRowCheckpoint keeps the same rule).
//
// Batches are index ranges: the consumer slices its own population, which
// therefore never flows into the iterator (a population passed here would
// leak to the heap, and with it a caller's one-row slice literal).
//
// Its sources are checked where they enter the executor: storage streams and
// visitors go through storage_stream.go's scan adapter
// (TestStorageStreamCallSites), and no allocation is sized from a client's
// LIMIT, SKIP, k or topK (TestClientSizedAllocations). Streaming producers,
// sorting and recursive key/value traversal keep their own checkpoints.
func materializedBatches(ctx context.Context, n int) iter.Seq2[materializedBatch, error] {
	return func(yield func(materializedBatch, error) bool) {
		for start := 0; start < n; start += rowCancellationInterval {
			if n > 1 {
				if err := ctx.Err(); err != nil {
					recordExpressionFailure(ctx, err)
					yield(materializedBatch{}, err)
					return
				}
			}
			if !yield(materializedBatch{start: start, end: min(start+rowCancellationInterval, n)}, nil) {
				return
			}
		}
	}
}
