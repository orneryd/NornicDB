package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadSnapshotPreparationCancellation(t *testing.T) {
	for _, edges := range []bool{false, true} {
		t.Run(fmt.Sprintf("edges=%t", edges), func(t *testing.T) {
			b := newReadSnapshotTestEngine(t, true)
			if edges {
				for _, id := range []NodeID{"test:a", "test:b"} {
					_, err := b.CreateNode(&Node{ID: id})
					require.NoError(t, err)
				}
				require.NoError(t, b.FlushWriteBehind())
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				defer tx.Rollback()
				require.NoError(t, tx.SetImplicit(true))
				for i := 0; i < 12; i++ {
					require.NoError(t, tx.CreateEdge(&Edge{ID: EdgeID(fmt.Sprintf("test:r%d", i)), StartNode: "test:a", EndNode: "test:b", Type: "REL"}))
				}
				require.NoError(t, tx.Commit())
			} else {
				for i := 0; i < 12; i++ {
					bufferedCreate(t, b, &Node{ID: NodeID(fmt.Sprintf("test:n%d", i))})
				}
			}
			require.Greater(t, b.writeBehind.PendingOps(), 0)
			ctx := preparationContext(t, "visitEntityOverlayLocked.func1", 2)
			snapshot, err := OpenReadSnapshot(ctx, NewNamespacedEngine(b, "test"))
			if snapshot != nil {
				snapshot.Close()
			}
			require.GreaterOrEqual(t, ctx.hits, 2, "cancel inside snapshot population capture")
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, snapshot, "partial capture cannot publish a snapshot")
			retry, err := OpenReadSnapshot(context.Background(), NewNamespacedEngine(b, "test"))
			require.NoError(t, err)
			defer retry.Close()
			if edges {
				require.Len(t, snapshotEdgeNotes(t, retry), 12)
			} else {
				require.Len(t, snapshotNodeText(t, retry), 12)
			}
		})
	}
}
