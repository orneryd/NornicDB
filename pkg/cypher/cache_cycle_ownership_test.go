package cypher

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCachedResultCyclesKeepCallerOwnership(t *testing.T) {
	mapping := map[string]any{}
	mapping["self"] = mapping
	list := make([]any, 1)
	list[0] = list
	for name, value := range map[string]any{"map": mapping, "list": list} {
		t.Run(name, func(t *testing.T) {
			cache := NewSmartQueryCache(4)
			cache.PutWithLabels("RETURN 1", nil, &ExecuteResult{Rows: [][]any{{value}}}, time.Hour, nil)
			first, found := cache.Get("RETURN 1", nil)
			require.True(t, found)
			second, found := cache.Get("RETURN 1", nil)
			require.True(t, found)
			a, b := first.Rows[0][0], second.Rows[0][0]
			require.NotEqual(t, borrowedRowIdentity(a), borrowedRowIdentity(b))
			switch v := a.(type) {
			case map[string]any:
				require.Equal(t, borrowedRowIdentity(v), borrowedRowIdentity(v["self"]))
				v["extra"] = true
				require.NotContains(t, b.(map[string]any), "extra")
			case []any:
				require.Equal(t, borrowedRowIdentity(v), borrowedRowIdentity(v[0]))
				v[0] = "changed"
				require.IsType(t, []any{}, b.([]any)[0])
			}
		})
	}
}

func TestTransportVectorImagesPreserveOwnedGoDelivery(t *testing.T) {
	node := &storage.Node{ID: "transport:n", Properties: map[string]any{"name": "alpha"},
		NamedEmbeddings: map[string][]float32{"title": {1, 2}}, ChunkEmbeddings: [][]float32{{3, 4}}}
	edge := &storage.Edge{ID: "transport:e", StartNode: node.ID, EndNode: "transport:other", Type: "LINK", Properties: map[string]any{"weight": int64(7)}}
	cache := NewSmartQueryCache(4)
	cache.putWithLabels("key", &ExecuteResult{Rows: [][]any{{node, edge}}}, time.Hour, nil)
	first, _, entities, found := cache.getWithTraceForEmbedding("key", 0, transportDelivery)
	require.True(t, found)
	require.True(t, entities, "borrowed images still record materialized access")
	second, _, _, found := cache.getWithTraceForEmbedding("key", 0, transportDelivery)
	require.True(t, found)
	require.Same(t, first.Rows[0][0], second.Rows[0][0])
	require.Same(t, first.Rows[0][1], second.Rows[0][1])
	owned, _, _, found := cache.getWithTraceForEmbedding("key", 0, ownedDelivery)
	require.True(t, found)
	owned.Rows[0][0].(*storage.Node).ChunkEmbeddings[0][0] = 99
	owned.Rows[0][0].(*storage.Node).NamedEmbeddings["title"][0] = 99
	owned.Rows[0][1].(*storage.Edge).Properties["weight"] = int64(99)
	require.Equal(t, float32(3), first.Rows[0][0].(*storage.Node).ChunkEmbeddings[0][0])
	require.Equal(t, float32(1), first.Rows[0][0].(*storage.Node).NamedEmbeddings["title"][0])
	require.Equal(t, int64(7), first.Rows[0][1].(*storage.Edge).Properties["weight"])
}
