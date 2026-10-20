package search

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// Done is consulted as the scoring pass starts. Cancel there to exercise an
// in-flight cancellation after the graph reader has pinned its codes, without
// sleeps or a collection-sized timing race.
type cancelPinnedScoringContext struct {
	context.Context
	cancel       context.CancelFunc
	graph        *HNSWIndex
	once         sync.Once
	readerPinned bool
}

func (c *cancelPinnedScoringContext) Done() <-chan struct{} {
	if c.graph.mu.TryLock() {
		c.graph.mu.Unlock()
	} else {
		c.once.Do(func() { c.readerPinned = true; c.cancel() })
	}
	return c.Context.Done()
}
func TestQuantizedFinalScoringCancellationReleasesReader(t *testing.T) {
	for _, path := range []string{"final", "completion", "stage"} {
		t.Run(path, func(t *testing.T) {
			graph := NewHNSWIndex(2, DefaultHNSWConfig())
			require.NoError(t, graph.Add("doc", []float32{1, 0}))
			require.NoError(t, graph.SetQuantization(context.Background(), HNSWQuantizationConfig{Mode: HNSWQuantizationBinary}))
			root, cancel := context.WithCancel(context.Background())
			defer cancel()
			signal := &cancelPinnedScoringContext{Context: root, cancel: cancel, graph: graph}
			ctx := context.WithValue(signal, hnswQuantizationOptionsKey{}, hnswQuantizationOptions{rescore: false, oversampling: 1})
			p := NewVectorSearchPipeline(NewHNSWCandidateGen(graph), NewCPUExactScorer(nil))
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
				require.ErrorIs(t, p.scoreStageBatch(ctx, []float32{1, 0}, []*stageNode{node}, nil), context.Canceled)
				require.False(t, node.scored)
			}
			require.True(t, signal.readerPinned, "cancellation happened while the graph reader was pinned")
			require.True(t, graph.mu.TryLock(), "the canceled pass must release the reader for graph updates")
			graph.mu.Unlock()
		})
	}
}

func TestQuantizedFinalScoringPreCanceledDoesNotScore(t *testing.T) {
	graph := NewHNSWIndex(2, DefaultHNSWConfig())
	require.NoError(t, graph.Add("doc", []float32{1, 0}))
	require.NoError(t, graph.SetQuantization(context.Background(), HNSWQuantizationConfig{Mode: HNSWQuantizationBinary}))
	root, cancel := context.WithCancel(context.Background())
	cancel()
	ctx := context.WithValue(root, hnswQuantizationOptionsKey{}, hnswQuantizationOptions{rescore: false, oversampling: 1})
	p := NewVectorSearchPipeline(NewHNSWCandidateGen(graph), NewCPUExactScorer(nil))
	scored, err := p.scoreFinal(ctx, []float32{1, 0}, []Candidate{{ID: "doc"}})
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, scored)
	require.True(t, graph.mu.TryLock())
	graph.mu.Unlock()
}
