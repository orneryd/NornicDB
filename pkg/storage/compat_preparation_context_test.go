package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// The compatibility source hides optional streaming interfaces. The composite
// therefore collects the legacy result before adding source provenance.
func TestCompositeChunkPreparationCancellation(t *testing.T) {
	base := newTestMemoryEngine(t)
	for i := 0; i < 4; i++ {
		_, err := base.CreateNode(&Node{ID: NodeID(fmt.Sprintf("test:p%d", i)), Labels: []string{"P"}})
		require.NoError(t, err)
	}
	c := NewCompositeEngine(map[string]Engine{"one": preparationLegacyLabelEngine{base}}, map[string]string{"one": "test"}, map[string]string{"one": "read"})
	ctx := preparationContext(t, "materializedValuesWithContext", 2)
	seen := 0
	err := c.StreamNodeChunks(ctx, 8, func(nodes []*Node) error { seen += len(nodes); return nil })
	require.GreaterOrEqual(t, ctx.hits, 2)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, seen)
	require.NoError(t, c.StreamNodeChunks(context.Background(), 8, func(nodes []*Node) error {
		seen += len(nodes)
		for _, node := range nodes {
			require.NotEmpty(t, node.Database)
		}
		return nil
	}))
	require.Equal(t, 4, seen)
}
