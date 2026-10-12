package cypher

import (
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// BenchmarkOptimisticMetadataLargeBatch pins the O(1) dedup in the optimistic
// metadata helpers. The previous linear scan over the accumulating ID slice
// made a statement creating N relationships quadratic (N²/2 string compares);
// at 48k edges that was ~1.15 billion comparisons and dominated the seed.
func BenchmarkOptimisticMetadataLargeBatch(b *testing.B) {
	const n = 50_000
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := &ExecuteResult{}
		for j := 0; j < n; j++ {
			addOptimisticRelationshipID(res, storage.EdgeID(fmt.Sprintf("e-%d", j)))
		}
	}
}

func TestPredicateHelpers_Branches(t *testing.T) {
	node := &storage.Node{
		ID:              "n1",
		Properties:      map[string]interface{}{"id": "custom-id", "p": 42},
		EmbedMeta:       map[string]interface{}{"has_embedding": true},
		ChunkEmbeddings: [][]float32{{0.1, 0.2}},
	}

	v, ok := getNodePropertyValue(node, "has_embedding")
	require.True(t, ok)
	require.Equal(t, true, v)

	node.EmbedMeta = nil
	v, ok = getNodePropertyValue(node, "has_embedding")
	require.True(t, ok)
	require.Equal(t, true, v)

	node.ChunkEmbeddings = nil
	v, ok = getNodePropertyValue(node, "has_embedding")
	require.True(t, ok)
	require.Equal(t, false, v)

	v, ok = getBindingNodeValue(node, "id")
	require.True(t, ok)
	require.Equal(t, "custom-id", v)

	delete(node.Properties, "id")
	v, ok = getBindingNodeValue(node, "id")
	require.True(t, ok)
	require.Equal(t, "n1", v)

	_, ok = getBindingNodeValue(nil, "id")
	require.False(t, ok)

	set, nonComparable := buildComparableMembershipIndex([]interface{}{nil, "x", 1, []int{1, 2}})
	require.Len(t, set, 2)
	require.Len(t, nonComparable, 1)

	eq := func(a interface{}, b interface{}) bool {
		as, aok := a.([]int)
		bs, bok := b.([]int)
		if aok && bok {
			if len(as) != len(bs) {
				return false
			}
			for i := range as {
				if as[i] != bs[i] {
					return false
				}
			}
			return true
		}
		return a == b
	}

	require.True(t, evaluateComparableMembership("x", set, nonComparable, eq))
	require.True(t, evaluateComparableMembership([]int{1, 2}, set, nonComparable, eq))
	require.False(t, evaluateComparableMembership(nil, set, nonComparable, eq))
	require.False(t, evaluateComparableMembership("missing", set, nonComparable, eq))

	require.False(t, isComparableValue(nil))
	require.True(t, isComparableValue("x"))
	require.False(t, isComparableValue([]int{1}))
}

func TestOptimisticMetadataHelpers_Branches(t *testing.T) {
	addOptimisticNodeID(nil, "n1")
	addOptimisticRelationshipID(nil, "r1")

	res := &ExecuteResult{}
	addOptimisticNodeID(res, "")
	addOptimisticRelationshipID(res, "")
	require.Nil(t, res.Metadata)

	meta := ensureOptimisticMeta(res)
	require.NotNil(t, meta)
	require.NotNil(t, res.Metadata)

	addOptimisticNodeID(res, "n1")
	addOptimisticNodeID(res, "n1")
	addOptimisticNodeID(res, "n2")
	require.Equal(t, []string{"n1", "n2"}, meta.CreatedNodeIDs)

	addOptimisticRelationshipID(res, "r1")
	addOptimisticRelationshipID(res, "r1")
	addOptimisticRelationshipID(res, "r2")
	require.Equal(t, []string{"r1", "r2"}, meta.CreatedRelationshipIDs)

	res2 := &ExecuteResult{Metadata: map[string]interface{}{"optimistic": optimisticMutationMeta{CreatedNodeIDs: []string{"x"}}}}
	meta2 := ensureOptimisticMeta(res2)
	require.Equal(t, []string{"x"}, meta2.CreatedNodeIDs)
	require.IsType(t, &optimisticMutationMeta{}, res2.Metadata["optimistic"])

	res3 := &ExecuteResult{Metadata: map[string]interface{}{"optimistic": "invalid"}}
	meta3 := ensureOptimisticMeta(res3)
	require.NotNil(t, meta3)
	require.Empty(t, meta3.CreatedNodeIDs)
}
