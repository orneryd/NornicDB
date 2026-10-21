package search

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Cancel from the actual vector read, including the last member: cancellation
// must survive the handoff from scoring to both consumers of final scores.
type cancelAtVectorRead struct{ cancel context.CancelFunc }

func (g cancelAtVectorRead) GetVector(string) ([]float32, bool) {
	g.cancel()
	return []float32{1, 0}, true
}

func TestFinalScoringCancellation(t *testing.T) {
	for _, path := range []string{"final", "completion", "stage"} {
		t.Run(path, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			p := NewVectorSearchPipeline(nil, NewCPUExactScorer(cancelAtVectorRead{cancel}))
			pool := []Candidate{{ID: "doc", Score: 1}}
			switch path {
			case "final":
				scored, err := p.scoreFinal(ctx, []float32{1, 0}, pool)
				require.ErrorIs(t, err, context.Canceled)
				require.Empty(t, scored)
			case "completion":
				scored, err := p.scoreAllNodes(ctx, []float32{1, 0}, pool, 0)
				require.ErrorIs(t, err, context.Canceled)
				require.Empty(t, scored)
			case "stage":
				node := &stageNode{id: "doc", first: pool[0]}
				// Use the generic final-scoring consumer, as compressed and file-backed
				// batches do, rather than the in-memory CPU accumulator specialization.
				p.exactScorer = finalCancellationScorer{p.exactScorer}
				require.ErrorIs(t, p.scoreStageBatch(ctx, []float32{1, 0}, []*stageNode{node}, nil), context.Canceled)
				require.False(t, node.scored)
			}
		})
	}
}

type finalCancellationScorer struct{ ExactScorer }
