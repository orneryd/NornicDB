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
	// Source s reaches five candidates t1..t5 through two hubs.
	graph := Graph{"s": {"h1": {}, "h2": {}}, "h1": {"s": {}}, "h2": {"s": {}}}
	for _, target := range []storage.NodeID{"t1", "t2", "t3", "t4", "t5"} {
		graph[target] = NodeSet{"h1": {}, "h2": {}}
		graph["h1"][target] = struct{}{}
		graph["h2"][target] = struct{}{}
	}
	source := storage.NodeID("s")
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
