package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStoredEdgeStreamPreparationCancellation(t *testing.T) {
	for _, edgeType := range []string{"REL", ""} {
		t.Run("type="+edgeType, func(t *testing.T) {
			b := newReadSnapshotTestEngine(t, true)
			for _, id := range []NodeID{"test:a", "test:b"} {
				_, err := b.CreateNode(&Node{ID: id})
				require.NoError(t, err)
			}
			require.NoError(t, b.FlushWriteBehind())
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			defer tx.Rollback()
			require.NoError(t, tx.SetImplicit(true))
			for i := 0; i < 20; i++ {
				require.NoError(t, tx.CreateEdge(&Edge{ID: EdgeID(fmt.Sprintf("test:r%d", i)), StartNode: "test:a", EndNode: "test:b", Type: "REL"}))
			}
			require.NoError(t, tx.Commit())
			require.Greater(t, b.writeBehind.PendingOps(), 0)
			ctx := preparationContext(t, "visitEntityOverlayLocked.func1", 2)
			calls := 0
			err = b.StreamStoredEdgesByType(ctx, edgeType, func(*Edge) error { calls++; return nil })
			t.Logf("type=%q hits=%d callbacks=%d err=%v", edgeType, ctx.hits, calls, err)
			require.GreaterOrEqual(t, ctx.hits, 2)
			require.ErrorIs(t, err, context.Canceled)
			require.Zero(t, calls, "capture cancels before callbacks")
			calls = 0
			require.NoError(t, b.StreamStoredEdgesByType(context.Background(), edgeType, func(*Edge) error { calls++; return nil }))
			require.Equal(t, 20, calls)
		})
	}
}
