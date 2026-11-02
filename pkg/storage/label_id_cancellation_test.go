package storage

import (
	"context"
	"fmt"
	"testing"

	fixture "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// The ready-index path must honor the same caller cancellation as rebuild
// discovery. Cancel from the public visitor after real delivery starts.
func TestReadyLabelIDStreamStopsAfterCallerCancellation(t *testing.T) {
	engine, err := fixture.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	const count = 1000
	nodes := make([]*Node, count)
	for i := range nodes {
		nodes[i] = &Node{ID: NodeID(fmt.Sprintf("test:cancel-%04d", i)), Labels: []string{"CancelLabel"}}
	}
	require.NoError(t, engine.BulkCreateNodes(t.Context(), nodes))
	for _, cancelAt := range []int{1, count} {
		t.Run(fmt.Sprintf("cancel-at-%d", cancelAt), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			seen := 0
			err := engine.ForEachNodeIDByLabelInScope(ctx, "test:", "CancelLabel", func(NodeID) bool {
				seen++
				if seen == cancelAt {
					cancel()
				}
				return true
			})
			require.ErrorIs(t, err, context.Canceled)
			require.Positive(t, seen)
			if cancelAt == 1 {
				require.Less(t, seen, count, "the canceled caller must not receive the complete remaining population")
			} else {
				require.Equal(t, count, seen, "cancellation during final delivery must still be reported")
			}
		})
	}
}
