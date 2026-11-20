package linkpredict

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestV152RCCHybridLoopIgnoresCancellation logs, at each semantic callback the
// loop at hybrid.go:255-262 makes, whether the request context was already
// cancelled. A healthy control establishes the candidate count; with the
// context cancelled before Predict the loop still calls the callback for every
// candidate.
func TestV152RCCHybridLoopIgnoresCancellation(t *testing.T) {
	graph := buildTestGraph()
	var source storage.NodeID
	run := func(ctx context.Context) (calls, cancelledCalls, preds int) {
		scorer := NewHybridScorer(DefaultHybridConfig())
		scorer.SetSemanticScorer(func(ctx context.Context, _, _ storage.NodeID) float64 {
			calls++
			if ctx.Err() != nil {
				cancelledCalls++
				return 0
			}
			return 0.5
		})
		preds = len(scorer.Predict(ctx, graph, source, 10))
		return
	}
	// Pick a source with more than one topology candidate.
	for id := range graph {
		source = id
		if c, _, _ := run(context.Background()); c > 1 {
			break
		}
	}
	controlCalls, controlCancelled, controlPreds := run(context.Background())
	t.Logf("RCC_LINKPREDICT_CONTROL owner=pkg/linkpredict/hybrid.go:255-262 source=%s calls=%d cancelled_calls=%d predictions=%d", source, controlCalls, controlCancelled, controlPreds)
	require.Greater(t, controlCalls, 1)
	require.Zero(t, controlCancelled)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls, cancelledCalls, preds := run(ctx)
	t.Logf("RCC_LINKPREDICT_TRIGGER owner=pkg/linkpredict/hybrid.go:255-262 source=%s calls=%d cancelled_calls=%d predictions=%d", source, calls, cancelledCalls, preds)
	assert.LessOrEqual(t, calls, 1, "a cancelled request must stop the candidate loop")
	assert.Zero(t, preds, "a cancelled request must not return scored predictions")
}
