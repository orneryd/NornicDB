package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func toInt64ForTest(t *testing.T, v interface{}) int64 {
	t.Helper()
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case float64:
		return int64(x)
	default:
		t.Fatalf("unexpected numeric type %T (%v)", v, v)
		return 0
	}
}

func TestChainedUnwindDependentRangeSupported(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)

	res, err := exec.Execute(context.Background(), "UNWIND range(1, 5) AS i UNWIND range(1, i) AS j RETURN i, j", nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 15)

	require.Equal(t, int64(1), toInt64ForTest(t, res.Rows[0][0]))
	require.Equal(t, int64(1), toInt64ForTest(t, res.Rows[0][1]))

	last := res.Rows[len(res.Rows)-1]
	require.Equal(t, int64(5), toInt64ForTest(t, last[0]))
	require.Equal(t, int64(5), toInt64ForTest(t, last[1]))
}

func TestChainedUnwindDependentRangeSupportsComputedReturn(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)

	res, err := exec.Execute(context.Background(), "UNWIND range(1, 3) AS i UNWIND range(1, i) AS j RETURN i, j, i + j AS s", nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 6)

	// Last row: i=3, j=3, s=6
	last := res.Rows[len(res.Rows)-1]
	require.Equal(t, int64(3), toInt64ForTest(t, last[0]))
	require.Equal(t, int64(3), toInt64ForTest(t, last[1]))
	require.Equal(t, int64(6), toInt64ForTest(t, last[2]))
}

func TestChainedUnwindPreservesBindingsAcrossWithFilter(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "chained_unwind_with_filter")
	exec := NewStorageExecutor(store)

	res, err := exec.Execute(context.Background(), `
		UNWIND [true, false, null] AS a
		UNWIND [true, false, null] AS b
		WITH a, b WHERE a IS NULL OR b IS NULL
		RETURN a, b, (a AND b) IS NULL = (b AND a) IS NULL AS result
	`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "result"}, res.Columns)
	require.ElementsMatch(t, [][]interface{}{
		{true, nil, true},
		{false, nil, true},
		{nil, true, true},
		{nil, false, true},
		{nil, nil, true},
	}, res.Rows)
}

func TestChainedUnwindsPreserveEveryBindingAtArbitraryArity(t *testing.T) {
	for _, arity := range []int{2, 3, 4, 6} {
		t.Run(fmt.Sprintf("arity_%d", arity), func(t *testing.T) {
			base := newTestMemoryEngine(t)
			store := storage.NewNamespacedEngine(base, fmt.Sprintf("chained_unwinds_%d", arity))
			exec := NewStorageExecutor(store)

			var query strings.Builder
			columns := make([]string, arity)
			for index := 0; index < arity; index++ {
				columns[index] = fmt.Sprintf("v%d", index)
				fmt.Fprintf(&query, "UNWIND [true, false] AS %s\n", columns[index])
			}
			fmt.Fprintf(&query, "RETURN %s", strings.Join(columns, ", "))

			res, err := exec.Execute(context.Background(), query.String(), nil)
			require.NoError(t, err)
			require.Equal(t, columns, res.Columns)
			require.Len(t, res.Rows, 1<<arity)
			for _, row := range res.Rows {
				require.Len(t, row, arity)
			}
		})
	}
}

func TestUnwindPreservesCollectedEntityBindings(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "unwind_collected_entities")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE ({id: 1}), ({id: 2})", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, `
		MATCH (row)
		WITH collect(row) AS rows
		UNWIND rows AS node
		RETURN node.id
	`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"node.id"}, result.Columns)
	require.ElementsMatch(t, [][]interface{}{{int64(1)}, {int64(2)}}, result.Rows)
}

func TestUnwindWildcardProjectionPreservesCompleteScope(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "unwind_wildcard_scope")
	exec := NewStorageExecutor(store)

	result, err := exec.Execute(context.Background(), `
		WITH [1, 2] AS xs, [3, 4] AS ys, [5, 6] AS zs
		UNWIND xs AS x
		UNWIND ys AS y
		UNWIND zs AS z
		RETURN *
	`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"x", "xs", "y", "ys", "z", "zs"}, result.Columns)
	require.Len(t, result.Rows, 8)
}

func TestUnwindEntityMayBeReusedByFollowingMatch(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "unwind_entity_match")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
		CREATE (s:S), (n), (e:E),
		       (s)-[:X]->(e), (s)-[:Y]->(e), (n)-[:Y]->(e)
	`, nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, `
		MATCH (a:S)-[:X]->(b1)
		WITH a, collect(b1) AS bees
		UNWIND bees AS b2
		MATCH (a)-[:Y]->(b2)
		RETURN a, b2
	`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
}
