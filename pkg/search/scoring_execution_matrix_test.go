package search

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

type matrixVectorGetter struct {
	reads, cancelAt int
	cancel          context.CancelFunc
}

func (g *matrixVectorGetter) GetVector(string) ([]float32, bool) {
	g.reads++
	if g.reads == g.cancelAt {
		g.cancel()
	}
	return []float32{1, 0}, true
}

// The CPU and generic consumers must obey the same bounded lease as compressed
// pools, including cancellation on the last read and at either side of a batch.
func TestScoringExecutionGeneratedCancellationMatrix(t *testing.T) {
	for _, cancelAt := range []int{1, 64, 65, 256, 300} {
		for _, stage := range []string{"final", "completion", "cpu_stage", "generic_stage"} {
			t.Run(fmt.Sprintf("%s/read_%d", stage, cancelAt), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				getter := &matrixVectorGetter{cancelAt: cancelAt, cancel: cancel}
				p := NewVectorSearchPipeline(nil, NewCPUExactScorer(getter))
				pool := make([]Candidate, 300)
				for i := range pool {
					pool[i] = Candidate{ID: fmt.Sprintf("doc-%03d", i), Score: 1}
				}
				var rows []ScoredCandidate
				var err error
				switch stage {
				case "final":
					rows, err = p.scoreFinal(ctx, []float32{1, 0}, pool)
				case "completion":
					rows, err = p.scoreAllNodes(ctx, []float32{1, 0}, pool, 0)
				default:
					if stage == "generic_stage" {
						p.exactScorer = finalCancellationScorer{p.exactScorer}
					}
					rows, _, err = p.scoreNodes(ctx, []float32{1, 0}, pool, 0, 0, math.Inf(1))
				}
				require.ErrorIs(t, err, context.Canceled)
				require.Empty(t, rows)
				require.GreaterOrEqual(t, getter.reads, cancelAt)
				require.LessOrEqual(t, getter.reads, cancelAt+searchBatchSize-1)
			})
		}
	}
}
