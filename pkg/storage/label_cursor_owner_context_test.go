package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// These controls enter through the public reader. The older scoped-collection
// control directly called useScopedCandidates and could not prove this route.
func TestPublicLabelCursorOwnerCancellation(t *testing.T) {
	for _, owner := range []string{"nodeIndexReader).Next", "useScopedCandidates"} {
		t.Run(owner, func(t *testing.T) {
			b := createTestBadgerEngine(t)
			// A foreign numeric entry forces the scoped dictionary producer on
			// first use, rather than after a successful own-namespace callback.
			_, err := b.CreateNode(&Node{ID: "foreign:first", Labels: []string{"Target"}})
			require.NoError(t, err)
			for i := 0; i < 20; i++ {
				_, err = b.CreateNode(&Node{ID: NodeID(fmt.Sprintf("test:%02d", i)), Labels: []string{"Target"}})
				require.NoError(t, err)
			}
			ctx := preparationContext(t, owner, 2)
			seen := 0
			err = b.StreamNodesWithOptions(ctx, StreamNodesOptions{QueryNodes: true, Prefix: "test:", Label: "Target"}, func(*Node) error { seen++; return nil })
			t.Logf("owner=%s observed=%s hits=%d callbacks=%d read=%v", owner, ctx.observed, ctx.hits, seen, err)
			require.GreaterOrEqual(t, ctx.hits, 2)
			require.ErrorIs(t, err, context.Canceled)
			require.Zero(t, seen)
			nodes, err := b.GetNodesByLabelInScope("test:", "Target")
			require.NoError(t, err)
			require.Len(t, nodes, 20)
		})
	}
}
