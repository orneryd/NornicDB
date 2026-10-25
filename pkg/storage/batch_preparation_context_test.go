package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBatchPreparationCancellation(t *testing.T) {
	for _, owner := range []string{"readNodeBatch.func1", "readNodeBatch.func2", "readNodeBatch.func3", "readNodeBatch"} {
		t.Run(owner, func(t *testing.T) {
			b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: owner == "readNodeBatch.func2", WriteBehindInterval: time.Hour})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, b.Close()) })
			var ids []NodeID
			for i := 0; i < 12; i++ {
				id := NodeID(fmt.Sprintf("test:batch%d", i))
				_, err := b.CreateNode(&Node{ID: id, Labels: []string{"P"}})
				require.NoError(t, err)
				ids = append(ids, id)
			}
			ordinary, err := b.BatchGetNodes(ids)
			require.NoError(t, err)
			require.Len(t, ordinary, 12)
			if owner == "readNodeBatch.func2" || owner == "readNodeBatch.func3" {
				// A cache miss forces the captured read rather than the all-cached
				// shortcut, without changing or replacing either native reader.
				ids = append([]NodeID{"test:missing"}, ids...)
			}
			read := func(ctx context.Context) (map[NodeID]*Node, error) {
				if reader, ok := any(b).(interface {
					BatchGetNodesWithContext(context.Context, []NodeID) (map[NodeID]*Node, error)
				}); ok {
					return reader.BatchGetNodesWithContext(ctx, ids)
				}
				return b.BatchGetNodes(ids)
			}
			ctx := preparationContext(t, owner, 2)
			rows, err := read(ctx)
			require.GreaterOrEqual(t, ctx.hits, 2)
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, rows)
			rows, err = read(context.Background())
			require.NoError(t, err)
			require.Len(t, rows, 12)
		})
	}
}
