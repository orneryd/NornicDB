package cypher

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestExactNumericConsumerFamilies(t *testing.T) {
	for name, key := range map[string]func(interface{}) string{
		"correlated_call": callSubqueryLookupKeyString,
		"path_distinct": joinedValueKey,
		"batch_match": propEqKeyBatch,
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, key(int64(9007199254740992)), key(float64(9007199254740992)))
			require.Equal(t, key(int64(0)), key(math.Copysign(0, -1)))
			require.NotEqual(t, key(int64(9007199254740992)), key(int64(9007199254740993)))
		})
	}
	t.Run("batch_unsigned_identity", func(t *testing.T) {
		require.NotEqual(t, propEqKeyBatch(int64(-1)), propEqKeyBatch(uint64(math.MaxUint64)))
	})
	t.Run("batch_property_lists", func(t *testing.T) {
		require.Equal(t, propEqKeyBatch([]int64{9007199254740992}), propEqKeyBatch([]interface{}{float64(9007199254740992)}))
		require.NotEqual(t, propEqKeyBatch([]int64{9007199254740992}), propEqKeyBatch([]int64{9007199254740993}))
	})
	t.Run("membership_unsigned_identity", func(t *testing.T) {
		require.Equal(t, membershipKey(uint64(1)<<63), membershipKey(float64(0x1p63)))
	})
	t.Run("unwind_distinct_precondition", func(t *testing.T) {
		require.False(t, unwindItemsAreDistinctComparable([]interface{}{int64(9007199254740992), float64(9007199254740992)}))
		require.False(t, unwindItemsAreDistinctComparable([]interface{}{int64(0), math.Copysign(0, -1)}))
		require.True(t, unwindItemsAreDistinctComparable([]interface{}{int64(9007199254740992), int64(9007199254740993)}))
	})
	t.Run("temporal_scalar_fallback", func(t *testing.T) {
		require.True(t, valuesEqual(int64(9007199254740992), float64(9007199254740992)))
		require.True(t, valuesEqual(int64(0), math.Copysign(0, -1)))
		require.False(t, valuesEqual(int64(9007199254740993), float64(9007199254740992)))
		require.False(t, valuesEqual(math.NaN(), math.NaN()))
	})
}

func TestExactNumericConsumerPathAggregates(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	match := &TraversalMatch{EndNode: nodePatternInfo{variable: "n"}}
	var paths []PathResult
	for i, v := range []interface{}{int64(9007199254740992), float64(9007199254740992), int64(9007199254740993), int64(0), math.Copysign(0, -1)} {
		paths = append(paths, PathResult{Nodes: []*storage.Node{{ID: storage.NodeID(fmt.Sprint(i)), Properties: map[string]interface{}{"v": v}}}})
	}
	t.Run("count_distinct", func(t *testing.T) {
		require.Equal(t, int64(3), exec.aggregatePathCount(context.Background(), paths, match, "count(DISTINCT n.v)"))
	})
	t.Run("collect_distinct", func(t *testing.T) {
		values := exec.aggregatePathCollect(context.Background(), paths, match, "collect(DISTINCT n.v)", true)
		require.Len(t, values, 3)
	})
}

func TestExactNumericConsumerDeleteMembership(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic")
	exec := NewStorageExecutor(store)
	for i, v := range []interface{}{int64(9007199254740992), float64(9007199254740992), int64(9007199254740993)} {
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprint(i)), Labels: []string{"ExactDelete"}, Properties: map[string]interface{}{"v": v}})
		require.NoError(t, err)
	}
	rows, used, err := exec.collectDeleteWithLimitCandidates(context.Background(), "MATCH (n:ExactDelete) WHERE n.v IN $vs", "n", 10, map[string]interface{}{"vs": []interface{}{float64(9007199254740992)}})
	require.NoError(t, err)
	require.True(t, used)
	require.Len(t, rows, 2)
	for _, row := range rows {
		require.NotEqual(t, int64(9007199254740993), row.Properties["v"])
	}
}
