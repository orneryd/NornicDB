package search

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// Component contracts for the scoring sub-boundaries a public request cannot
// inject into (see testing/search/contract/cancellation.go for the mapping):
// cancellation is emitted from the real vector read and must survive the
// handoff from scoring to both consumers of final scores.

// cancelAtVectorRead cancels from the actual vector read, including the last
// member: cancellation must survive the handoff from scoring to both consumers
// of final scores.
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

// cancelOnRead cancels on one selected vector read, so the last case cancels on
// the trailing member of a one-batch pool. A batch is the reader's cancellation
// boundary, so the current batch finishes its reads and the check after it stops
// the pass: no partial scored pool may escape, and a stage node must not keep a
// best score.
type cancelOnRead struct {
	cancel context.CancelFunc
	at     int
	reads  int
}

func (g *cancelOnRead) GetVector(string) ([]float32, bool) {
	g.reads++
	if g.reads == g.at {
		g.cancel()
	}
	return []float32{1, 0}, true
}

func TestFinalScoringCancellationFromAnyRead(t *testing.T) {
	pool := []Candidate{{ID: "a", Score: 1}, {ID: "b", Score: 1}, {ID: "c", Score: 1}}
	for _, path := range []string{"final", "completion", "stage"} {
		for _, at := range []int{1, 3} {
			t.Run(fmt.Sprintf("%s/read_%d_of_3", path, at), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				getter := &cancelOnRead{cancel: cancel, at: at}
				p := NewVectorSearchPipeline(nil, NewCPUExactScorer(getter))
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
					p.exactScorer = finalCancellationScorer{p.exactScorer}
					nodes := make([]*stageNode, len(pool))
					for index := range pool {
						nodes[index] = &stageNode{id: pool[index].ID, first: pool[index]}
					}
					require.ErrorIs(t, p.scoreStageBatch(ctx, []float32{1, 0}, nodes, nil), context.Canceled)
					for _, node := range nodes {
						require.False(t, node.scored)
					}
				}
				// Three candidates are one batch: its reads complete, and the
				// boundary after them reports the cancellation.
				require.Equal(t, len(pool), getter.reads, "the current batch is the cancellation boundary")
			})
		}
	}
}

// TestExactPoolCancellationIsBounded is the values-path twin of
// TestQuantizedPoolCancellationIsBounded: CPU vectors and compressed codes
// share one batch gate and one release, so a canceled exact pool scan stops
// within its current batch, returns no partial scored pool, and releases the
// borrowed reader exactly once.
func TestExactPoolCancellationIsBounded(t *testing.T) {
	for _, cancelAt := range []int{1, 64, 65, 256} {
		t.Run(fmt.Sprint(cancelAt), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			visited, released := 0, 0
			getter := vectorGetterFunc(func(string) ([]float32, bool) {
				visited++
				if visited == cancelAt {
					cancel()
				}
				return []float32{1, 0}, true
			})
			release := func() { released++ }
			scored, err := scoreVectorPool(ctx, []float32{1, 0}, getter, nil, release, make([]Candidate, 256))
			require.ErrorIs(t, err, context.Canceled)
			require.Empty(t, scored)
			require.GreaterOrEqual(t, visited, cancelAt)
			require.LessOrEqual(t, visited, cancelAt+63, "cancellation must stop the scan within its current batch")
			require.Equal(t, 1, released, "a canceled pool scan must release its reader exactly once")
		})
	}
}

// vectorGetterFunc adapts a read closure to the production VectorGetter.
type vectorGetterFunc func(string) ([]float32, bool)

func (f vectorGetterFunc) GetVector(id string) ([]float32, bool) { return f(id) }
