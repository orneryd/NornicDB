package cypher

import (
	"container/heap"
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestIndexedOrderExactIntegers(t *testing.T) {
	for _, transaction := range []bool{false, true} {
		for _, indexed := range []bool{false, true} {
			for _, smaller := range []int64{2, 9007199254740992} {
				t.Run(fmt.Sprintf("tx=%t/index=%t/smaller=%d", transaction, indexed, smaller), func(t *testing.T) {
					store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "exact-order")
					exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
					ctx := context.Background()
					larger := smaller + 1
					for _, entry := range []struct {
						id    storage.NodeID
						value int64
					}{
						{"a-larger", larger}, {"z-smaller", smaller},
					} {
						_, err := store.CreateNode(&storage.Node{
							ID: entry.id, Labels: []string{"ExactOrder"},
							Properties: map[string]interface{}{"rank": entry.value, "secondary": -entry.value},
						})
						require.NoError(t, err)
					}
					if indexed {
						_, err := exec.Execute(ctx, "CREATE RANGE INDEX exact_rank FOR (n:ExactOrder) ON (n.rank)", nil)
						require.NoError(t, err)
						require.True(t, store.GetSchema().HasPropertyIndex("ExactOrder", "rank"))
					}
					if transaction {
						_, err := exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						t.Cleanup(func() {
							_, err := exec.Execute(ctx, "ROLLBACK", nil)
							require.NoError(t, err)
						})
					}
					for _, test := range []struct {
						name, query string
						want        [][]interface{}
					}{
						{"ascending limit", "MATCH (n:ExactOrder) WHERE n.rank IS NOT NULL RETURN n.rank ORDER BY n.rank ASC LIMIT 1", [][]interface{}{{smaller}}},
						{"descending limit", "MATCH (n:ExactOrder) WHERE n.rank IS NOT NULL RETURN n.rank ORDER BY n.rank DESC LIMIT 1", [][]interface{}{{larger}}},
						{"no limit", "MATCH (n:ExactOrder) WHERE n.rank IS NOT NULL RETURN n.rank ORDER BY n.rank ASC", [][]interface{}{{smaller}, {larger}}},
						{"with", "MATCH (n:ExactOrder) WHERE n.rank IS NOT NULL WITH n ORDER BY n.rank ASC LIMIT 1 RETURN n.rank", [][]interface{}{{smaller}}},
						{"skip", "MATCH (n:ExactOrder) WHERE n.rank IS NOT NULL RETURN n.rank ORDER BY n.rank ASC SKIP 1 LIMIT 1", [][]interface{}{{larger}}},
						{"secondary key", "MATCH (n:ExactOrder) WHERE n.rank IS NOT NULL RETURN n.rank ORDER BY n.rank ASC, n.secondary ASC LIMIT 1", [][]interface{}{{smaller}}},
						{"without where", "MATCH (n:ExactOrder) RETURN n.rank ORDER BY n.rank ASC LIMIT 1", [][]interface{}{{smaller}}},
					} {
						t.Run(test.name, func(t *testing.T) {
							for repeat := 0; repeat < 5; repeat++ {
								result, err := exec.Execute(ctx, test.query, nil)
								require.NoError(t, err)
								require.Equal(t, test.want, result.Rows, "run %d: %s", repeat, test.query)
								if indexed && !transaction && test.name == "ascending limit" {
									require.True(t, exec.LastHotPathTrace().OuterIndexTopK)
								}
							}
						})
					}
				})
			}
		}
	}
}

func TestIndexedOrderMixedNumericWidening(t *testing.T) {
	const integer int64 = 9007199254740993
	const rounded float64 = 9007199254740992
	require.Zero(t, compareValuesForSort(integer, rounded))
	require.Zero(t, compareValuesForSort(rounded, integer))
	for _, transaction := range []bool{false, true} {
		t.Run(fmt.Sprintf("tx=%t", transaction), func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "mixed-order")
			exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
			ctx := context.Background()
			for _, entry := range []struct {
				id    storage.NodeID
				value interface{}
			}{
				{"a-integer", integer}, {"z-float", rounded},
			} {
				_, err := store.CreateNode(&storage.Node{
					ID: entry.id, Labels: []string{"MixedOrder"},
					Properties: map[string]interface{}{"rank": entry.value, "name": string(entry.id)},
				})
				require.NoError(t, err)
			}
			_, err := exec.Execute(ctx, "CREATE RANGE INDEX mixed_rank FOR (n:MixedOrder) ON (n.rank)", nil)
			require.NoError(t, err)
			if transaction {
				_, err = exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				t.Cleanup(func() {
					_, err := exec.Execute(ctx, "ROLLBACK", nil)
					require.NoError(t, err)
				})
			}
			for _, parameter := range []interface{}{integer, rounded} {
				result, err := exec.Execute(ctx, "MATCH (n:MixedOrder) WHERE n.rank = $value RETURN n.name ORDER BY n.name ASC", map[string]interface{}{"value": parameter})
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{"a-integer"}, {"z-float"}}, result.Rows)
			}
			for _, direction := range []string{"ASC", "DESC"} {
				query := "MATCH (n:MixedOrder) WHERE n.rank IS NOT NULL RETURN n.name ORDER BY n.rank " + direction + ", n.name ASC LIMIT 1"
				result, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{"a-integer"}}, result.Rows)
			}
		})
	}
}

func indexedOrderFixture(tb testing.TB, count int) *StorageExecutor {
	tb.Helper()
	store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "order-ties")
	exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	for i := 0; i < count; i++ {
		_, err := store.CreateNode(&storage.Node{
			ID: storage.NodeID(fmt.Sprintf("node-%06d", i)), Labels: []string{"OrderedItem"},
			Properties: map[string]interface{}{
				"title": "same title", "rank": int64(count - i),
				"payload": int64(count - i), "active": i%3 == 0,
			},
		})
		require.NoError(tb, err)
	}
	_, err := exec.Execute(context.Background(), "CREATE INDEX ordered_title FOR (n:OrderedItem) ON (n.title)", nil)
	require.NoError(tb, err)
	return exec
}

func TestIndexedOrderRetainsTiesBeforePagination(t *testing.T) {
	// The tied group exceeds the old 200-row/4*K over-fetch window. Storage
	// identity order is the reverse of the requested secondary property order.
	const count = 720
	exec := indexedOrderFixture(t, count)
	for _, tc := range []struct {
		name, where, direction string
		filtered               bool
	}{
		{"filtered ascending", "n.active = true", "ASC", true},
		{"filtered descending", "n.active = true", "DESC", true},
		{"not-null ascending", "n.title IS NOT NULL", "ASC", false},
		{"not-null descending", "n.title IS NOT NULL", "DESC", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var expected []int64
			for i := 0; i < count; i++ {
				if !tc.filtered || i%3 == 0 {
					expected = append(expected, int64(count-i))
				}
			}
			sort.Slice(expected, func(i, j int) bool {
				if tc.direction == "DESC" {
					return expected[i] > expected[j]
				}
				return expected[i] < expected[j]
			})
			for _, offset := range []int{0, 24, 193} {
				query := fmt.Sprintf("MATCH (n:OrderedItem) WHERE %s RETURN n.payload AS payload ORDER BY n.title ASC, n.rank %s SKIP %d LIMIT 24", tc.where, tc.direction, offset)
				result, err := exec.Execute(context.Background(), query, nil)
				require.NoError(t, err)
				var actual []int64
				for _, row := range result.Rows {
					actual = append(actual, row[0].(int64))
				}
				require.Equal(t, expected[offset:offset+24], actual, query)
			}
		})
	}
}

func BenchmarkIndexedOrderTiedPage(b *testing.B) {
	exec := indexedOrderFixture(b, 720)
	ctx := context.Background()
	query := "MATCH (n:OrderedItem) WHERE n.active = true RETURN n.payload ORDER BY n.title ASC, n.rank ASC SKIP 24 LIMIT 24"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := exec.Execute(ctx, query, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestIndexedOrderPatternPropertiesAndCancellation(t *testing.T) {
	exec := indexedOrderFixture(t, 720)
	result, err := exec.Execute(context.Background(), "MATCH (n:OrderedItem {active:true}) RETURN n.payload ORDER BY n.title ASC, n.rank ASC LIMIT 3", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(3)}, {int64(6)}, {int64(9)}}, result.Rows)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = exec.tryCollectNodesFromPropertyIndexOrderLimit(ctx, nodePatternInfo{variable: "n", labels: []string{"OrderedItem"}}, "", "n.title ASC,n.rank ASC", 3)
	require.ErrorIs(t, err, context.Canceled)
}

func TestIndexedOrderEmptyIndexPreservesGeneralRead(t *testing.T) {
	store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "null-order")
	exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	_, err := exec.Execute(context.Background(), "CREATE INDEX empty_title FOR (n:NullTitle) ON (n.title)", nil)
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "without-title", Labels: []string{"NullTitle"}, Properties: map[string]interface{}{"payload": "retained"}})
	require.NoError(t, err)
	result, err := exec.Execute(context.Background(), "MATCH (n:NullTitle) RETURN n.payload ORDER BY n.title ASC LIMIT 3", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"retained"}}, result.Rows)
}

func TestIndexedOrderHeapWorstFirst(t *testing.T) {
	h := &indexedOrderHeap{compare: func(a, b *storage.Node) int { return int(a.Properties["rank"].(int64) - b.Properties["rank"].(int64)) }}
	for _, rank := range []int64{2, 1, 3} {
		heap.Push(h, &storage.Node{Properties: map[string]interface{}{"rank": rank}})
	}
	for _, expected := range []int64{3, 2, 1} {
		require.Equal(t, expected, heap.Pop(h).(*storage.Node).Properties["rank"])
	}
	require.Empty(t, h.nodes)
}

func TestIndexedOrderPartialIndexPreservesNullRows(t *testing.T) {
	store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "partial-index")
	exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	ctx := context.Background()
	for i := 0; i < 250; i++ {
		_, err := store.CreateNode(&storage.Node{
			ID: storage.NodeID(fmt.Sprintf("node-%03d", i)), Labels: []string{"OptionalTitle"},
			Properties: map[string]interface{}{"title": "same", "rank": int64(i), "active": i == 0},
		})
		require.NoError(t, err)
	}
	for i := 0; i < 2; i++ {
		_, err := store.CreateNode(&storage.Node{
			ID: storage.NodeID(fmt.Sprintf("missing-%d", i)), Labels: []string{"OptionalTitle"},
			Properties: map[string]interface{}{"rank": int64(500 + i), "active": true},
		})
		require.NoError(t, err)
	}
	_, err := exec.Execute(ctx, "CREATE INDEX optional_title FOR (n:OptionalTitle) ON (n.title)", nil)
	require.NoError(t, err)
	for _, tc := range []struct {
		page string
		want [][]interface{}
	}{
		{"LIMIT 5", [][]interface{}{{int64(0)}, {int64(500)}, {int64(501)}}},
		{"SKIP 1 LIMIT 2", [][]interface{}{{int64(500)}, {int64(501)}}},
	} {
		result, err := exec.Execute(ctx, "MATCH (n:OptionalTitle) WHERE n.active = true RETURN n.rank ORDER BY n.title ASC,n.rank ASC "+tc.page, nil)
		require.NoError(t, err)
		require.Equal(t, tc.want, result.Rows, tc.page)
	}
}

func TestIndexedOrderDescendingPreservesNullRows(t *testing.T) {
	store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "descending-null-order")
	exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	ctx := context.Background()
	for _, node := range []*storage.Node{
		{ID: "missing", Labels: []string{"OptionalTitle"}, Properties: map[string]interface{}{"rank": int64(1), "active": true}},
		{ID: "present", Labels: []string{"OptionalTitle"}, Properties: map[string]interface{}{"title": "a", "rank": int64(2), "active": true}},
	} {
		_, err := store.CreateNode(node)
		require.NoError(t, err)
	}
	_, err := exec.Execute(ctx, "CREATE INDEX optional_title FOR (n:OptionalTitle) ON (n.title)", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:OptionalTitle) WHERE n.active = true RETURN n.rank ORDER BY n.title DESC LIMIT 1", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	require.False(t, exec.LastHotPathTrace().OuterIndexTopK)
}

func TestIndexedOrderTraversalTiedLimit(t *testing.T) {
	exec := indexedOrderFixture(t, 720)
	_, err := exec.storage.CreateNode(&storage.Node{ID: "hub", Labels: []string{"Hub"}, Properties: map[string]interface{}{}})
	require.NoError(t, err)
	for i := 0; i < 720; i++ {
		require.NoError(t, exec.storage.CreateEdge(&storage.Edge{
			ID: storage.EdgeID(fmt.Sprintf("edge-%06d", i)), Type: "HAS",
			StartNode: "hub", EndNode: storage.NodeID(fmt.Sprintf("node-%06d", i)),
		}))
	}
	for _, pattern := range []string{"(h:Hub)-[:HAS]->(n:OrderedItem)", "(n:OrderedItem)<-[:HAS]-(h:Hub)"} {
		for _, direction := range []string{"ASC", "DESC"} {
			// Traversal's indexed seed optimization is used only without SKIP.
			query := fmt.Sprintf("MATCH %s WHERE n.active = true RETURN n.payload ORDER BY n.title ASC, n.rank %s LIMIT 24", pattern, direction)
			result, err := exec.Execute(context.Background(), query, nil)
			require.NoError(t, err)
			require.Len(t, result.Rows, 24, query)
			for i, row := range result.Rows {
				want := int64((i + 1) * 3)
				if direction == "DESC" {
					want = int64(720 - i*3)
				}
				require.Equal(t, want, row[0], query)
			}
		}
	}
}

func TestIndexedOrderTraversalRejectsOtherVariableSortKeys(t *testing.T) {
	store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "cross-variable-order")
	exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	ctx := context.Background()
	for _, node := range []*storage.Node{
		{ID: "original-a", Labels: []string{"Original"}, Properties: map[string]interface{}{"textKey": "a"}},
		{ID: "original-z", Labels: []string{"Original"}, Properties: map[string]interface{}{"textKey": "z"}},
		{ID: "translated-0", Labels: []string{"Translated"}, Properties: map[string]interface{}{"createdAt": "same"}},
		{ID: "translated-1", Labels: []string{"Translated"}, Properties: map[string]interface{}{"createdAt": "same"}},
	} {
		_, err := store.CreateNode(node)
		require.NoError(t, err)
	}
	for _, edge := range []*storage.Edge{
		{ID: "edge-a", Type: "TRANSLATES_TO", StartNode: "original-a", EndNode: "translated-0"},
		{ID: "edge-z", Type: "TRANSLATES_TO", StartNode: "original-z", EndNode: "translated-1"},
	} {
		require.NoError(t, store.CreateEdge(edge))
	}
	_, err := exec.Execute(ctx, "CREATE INDEX translated_created FOR (n:Translated) ON (n.createdAt)", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `
MATCH (o:Original)-[:TRANSLATES_TO]->(t:Translated)
WHERE t.createdAt IS NOT NULL
RETURN o.textKey
ORDER BY t.createdAt ASC, o.textKey DESC
LIMIT 1`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"z"}}, result.Rows)
	require.False(t, exec.LastHotPathTrace().TraversalEndSeedTopK)
}

func TestIndexedOrderUnlabelledMatchPreservesOtherLabels(t *testing.T) {
	exec := indexedOrderFixture(t, 720)
	_, err := exec.storage.CreateNode(&storage.Node{
		ID: "unindexed", Labels: []string{"OtherItem"},
		Properties: map[string]interface{}{"title": "first title", "payload": "unindexed"},
	})
	require.NoError(t, err)
	result, err := exec.Execute(context.Background(), "MATCH (n) WHERE n.title IS NOT NULL RETURN n.payload ORDER BY n.title ASC LIMIT 1", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"unindexed"}}, result.Rows)
	require.False(t, exec.LastHotPathTrace().OuterIndexTopK)
}
