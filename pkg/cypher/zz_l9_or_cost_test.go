package cypher

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func l9OrCostFixture(t *testing.T) *StorageExecutor {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "l9or")
	nodes := make([]*storage.Node, 1000)
	for i := range nodes {
		id := fmt.Sprintf("n%05d", i)
		key := int64(0)
		if i == 1 {
			key = 1
		}
		nodes[i] = &storage.Node{ID: storage.NodeID(id), Labels: []string{"O"}, Properties: map[string]any{"id": id, "status": "common", "k": key}}
	}
	require.NoError(t, store.BulkCreateNodes(t.Context(), nodes))
	executor := NewStorageExecutor(store, QueryCachePolicy{})
	ctx := t.Context()
	for _, prop := range []string{"id", "k", "status"} {
		_, err := executor.Execute(ctx, fmt.Sprintf("CREATE INDEX or_%s FOR (n:O) ON (n.%s)", prop, prop), nil)
		require.NoError(t, err)
	}
	return executor
}

func TestL9IndexedORInsideAND(t *testing.T) {
	executor := l9OrCostFixture(t)
	ctx := t.Context()
	for _, shape := range []struct {
		name, predicate string
		count int64
	}{
		{"or_only", "n.k=1 OR n.id='n00002'", 2},
		{"and_only", "n.id='n00002' AND n.status='common'", 1},
		{"nested_or", "(n.k=1 OR n.id='n00002') AND n.status='common'", 2},
	} {
		t.Run(shape.name, func(t *testing.T) {
			query := "MATCH (n:O) WHERE " + shape.predicate + " RETURN count(n)"
			_, err := executor.Execute(ctx, query, nil)
			require.NoError(t, err)
			for range 3 {
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				result, err := executor.Execute(ctx, query, nil)
				runtime.ReadMemStats(&after)
				require.NoError(t, err)
				require.Equal(t, [][]any{{shape.count}}, result.Rows)
				allocated := after.TotalAlloc - before.TotalAlloc
				t.Logf("L9 OR %s allocated=%d count=%d", shape.name, allocated, shape.count)
				require.Less(t, allocated, uint64(250000), "selective postings should avoid a full population read")
			}
		})
	}
}
