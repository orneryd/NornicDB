package cypher

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func descendingPopulation(n int) []int {
	items := make([]int, n)
	for index := range items {
		items[index] = n - index
	}
	return items
}

// Each sort is the standard-library sort while the statement runs, and stops
// within one stride of comparisons once it is canceled.
func TestPopulationBulkSortsCancellation(t *testing.T) {
	sorts := map[string]func(ctx context.Context, items []int, compared func()) error{
		"sortPopulation": func(ctx context.Context, items []int, compared func()) error {
			return sortPopulation(ctx, items, func(i, j int) bool { compared(); return items[i] < items[j] })
		},
		"sortPopulationStable": func(ctx context.Context, items []int, compared func()) error {
			return sortPopulationStable(ctx, items, func(i, j int) bool { compared(); return items[i] < items[j] })
		},
		"sortPopulationFunc": func(ctx context.Context, items []int, compared func()) error {
			return sortPopulationFunc(ctx, items, func(left, right int) int { compared(); return cmp.Compare(left, right) })
		},
		"stableSortRows": func(ctx context.Context, items []int, compared func()) error {
			return stableSortRows(ctx, items, func(left, right int) int { compared(); return cmp.Compare(left, right) })
		},
	}
	for name, sortItems := range sorts {
		t.Run(name, func(t *testing.T) {
			items := descendingPopulation(65536)
			require.NoError(t, sortItems(context.Background(), items, func() {}))
			require.True(t, slices.IsSorted(items))

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			comparisons := 0
			err := sortItems(ctx, descendingPopulation(65536), func() {
				comparisons++
				if comparisons == 100 {
					cancel()
				}
			})
			require.ErrorIs(t, err, context.Canceled)
			require.LessOrEqual(t, comparisons, 100+rowCancellationInterval)
		})
	}
}

// A copy longer than one stride observes the statement between strides and
// stops at the first observation that finds it canceled; one stride or less
// is the plain built-in.
func TestPopulationBulkCopiesCancellation(t *testing.T) {
	source := descendingPopulation(3*populationCopyStride + 7)
	canceledAtSecondCheck := func() context.Context {
		parent, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		return &rowCheckCancellationContext{Context: parent, cancel: cancel, after: 2}
	}

	appended, err := appendPopulation(context.Background(), []int{0}, source)
	require.NoError(t, err)
	require.Equal(t, append([]int{0}, source...), appended)
	appended, err = appendPopulation(canceledAtSecondCheck(), []int{0}, source)
	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, appended, 1+populationCopyStride)

	copied := make([]int, len(source))
	count, err := copyPopulation(context.Background(), copied, source)
	require.NoError(t, err)
	require.Equal(t, len(source), count)
	require.Equal(t, source, copied)
	count, err = copyPopulation(canceledAtSecondCheck(), make([]int, len(source)), source)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, populationCopyStride, count)

	cloned, err := clonePopulation(context.Background(), source)
	require.NoError(t, err)
	require.Equal(t, source, cloned)
	_, err = clonePopulation(canceledAtSecondCheck(), source)
	require.ErrorIs(t, err, context.Canceled)

	byID := make(map[storage.NodeID]int, 4*rowCancellationInterval)
	for index := 0; index < 4*rowCancellationInterval; index++ {
		byID[storage.NodeID(fmt.Sprint(index))] = index
	}
	clonedMap, err := clonePopulationMap(context.Background(), byID)
	require.NoError(t, err)
	require.Equal(t, byID, clonedMap)
	clonedMap, err = clonePopulationMap(canceledAtSecondCheck(), byID)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, clonedMap)

	precanceled, cancel := context.WithCancel(context.Background())
	cancel()
	small, err := appendPopulation(precanceled, nil, source[:populationCopyStride])
	require.NoError(t, err)
	require.Equal(t, source[:populationCopyStride], small)
}

// tieGroupSortContext cancels the caller's context the first time the
// statement observes it from inside the indexed order window's tie-group sort.
type tieGroupSortContext struct {
	context.Context
	cancel context.CancelFunc
	fired  atomic.Bool
}

func (c *tieGroupSortContext) Err() error {
	if !c.fired.Load() && stackHasFrame("sortPopulation") && stackHasFrame("collectIndexedOrderWindow") {
		c.fired.Store(true)
		c.cancel()
	}
	return c.Context.Err()
}

// An indexed ORDER BY whose leading key is equal for the whole population
// reads it as one tie group and sorts it by the remaining keys. That sort
// observes the statement: a cancellation inside it fails the query.
func TestIndexedOrderTieGroupExecuteCancellation(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "nornic")
	exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	const n = 4096
	for index := 0; index < n; index++ {
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("tie-%05d", index)), Labels: []string{"Tie"}, Properties: map[string]interface{}{"group": int64(1), "rank": int64((index * 7919) % n)}})
		require.NoError(t, err)
	}
	_, err := exec.Execute(context.Background(), "CREATE INDEX tie_group FOR (n:Tie) ON (n.group)", nil)
	require.NoError(t, err)
	query := fmt.Sprintf("MATCH (n:Tie) WHERE n.group IS NOT NULL RETURN n.rank AS rank ORDER BY n.group, n.rank LIMIT %d", n)

	result, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, n)
	for index, row := range result.Rows {
		require.Equal(t, int64(index), row[0])
	}

	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &tieGroupSortContext{Context: parent, cancel: cancel}
	result, err = exec.Execute(ctx, query, nil)
	require.True(t, ctx.fired.Load(), "the tie group must be sorted through sortPopulation")
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result)
}
