package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestExactNumericConsumerPathMinMax(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	match := &TraversalMatch{EndNode: nodePatternInfo{variable: "n"}}
	for _, values := range [][2]interface{}{
		{float64(9007199254740992), int64(9007199254740993)},
		{int64(9007199254740993), float64(9007199254740992)},
	} {
		paths := []PathResult{
			{Nodes: []*storage.Node{{ID: "a", Properties: map[string]interface{}{"v": values[0]}}}},
			{Nodes: []*storage.Node{{ID: "b", Properties: map[string]interface{}{"v": values[1]}}}},
		}
		require.Equal(t, float64(9007199254740992), exec.aggregatePathMinMax(context.Background(), paths, match, "n.v", false))
		require.Equal(t, int64(9007199254740993), exec.aggregatePathMinMax(context.Background(), paths, match, "n.v", true))
	}
}
