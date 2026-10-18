package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The four holdings the #1412 review found (#1343): each read counts exactly
// once, for as long as it is kept and no longer. Over the limit the
// statement fails with the limit's error; a read that fits the limit runs as
// it does with none; the pools return to zero.

// memoryReviewEngine is an engine with n nodes review-0 … review-(n-1) of
// label Review, p = 1 and k = i.
func memoryReviewEngine(t *testing.T, n int) storage.Engine {
	t.Helper()
	store := newTransactionMemoryEngine(t)
	for i := 0; i < n; i++ {
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("review-%d", i)), Labels: []string{"Review"}, Properties: map[string]interface{}{"p": int64(1), "k": int64(i)}})
		require.NoError(t, err)
	}
	return store
}

// requireWithinAndOver runs fits within limit with the rows it returns with
// no limit, and over within limit to the limit's error, the pools empty after
// each.
func requireWithinAndOver(t *testing.T, executor *StorageExecutor, limit int64, fits, over string) {
	t.Helper()
	ctx := context.Background()
	ConfigureTransactionMemory(0, TransactionMemoryLimits{})
	want, err := executor.Execute(ctx, fits, nil)
	require.NoError(t, err)

	ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: limit})
	got, err := executor.Execute(ctx, fits, nil)
	require.NoError(t, err, fits)
	require.Equal(t, want.Rows, got.Rows)
	requirePoolsEmpty(t, "nornic")

	_, err = executor.Execute(ctx, over, nil)
	requireMemoryError(t, err, "Neo.TransientError.General.TransactionMemoryLimit", "db.memory.transaction.max")
	requirePoolsEmpty(t, "nornic")
}

// MERGE's relationship cache keeps every relationship of a pair across its
// rows: it counts while it keeps them.
func TestMergeRelationshipCacheCountsWhatItKeeps(t *testing.T) {
	resetTransactionMemory(t)
	store := memoryReviewEngine(t, 4)
	for i := 0; i < 20000; i++ {
		require.NoError(t, store.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("many-%d", i)), StartNode: "review-0", EndNode: "review-1", Type: "PAIR", Properties: map[string]interface{}{"p": int64(i + 100)}}))
	}
	for i := 0; i < 5000; i++ {
		require.NoError(t, store.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("few-%d", i)), StartNode: "review-2", EndNode: "review-3", Type: "PAIR", Properties: map[string]interface{}{"p": int64(i + 100)}}))
	}
	executor := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	// Rows that reach the MERGE together share the cache's read of their
	// pair (ORDER BY hands them on together).
	const merge = "UNWIND [0, 1] AS p WITH p ORDER BY p MATCH (a:Review {k: %d}) MATCH (b:Review {k: %d}) MERGE (a)-[r:PAIR {p: p}]->(b) RETURN count(*) AS n"
	requireWithinAndOver(t, executor, 1<<20, fmt.Sprintf(merge, 2, 3), fmt.Sprintf(merge, 0, 1))
}

// An index lookup in an explicit transaction counts its IDs once: a
// read-only transaction's merge with its pending writes is the same list.
func TestExplicitTransactionIndexLookupCountsOnce(t *testing.T) {
	resetTransactionMemory(t)
	store := memoryReviewEngine(t, 12000)
	for i := 0; i < 20000; i++ {
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("review-big-%d", i)), Labels: []string{"Review"}, Properties: map[string]interface{}{"p": int64(2)}})
		require.NoError(t, err)
	}
	executor := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	ctx := context.Background()
	_, err := executor.Execute(ctx, "CREATE INDEX review_p FOR (n:Review) ON (n.p)", nil)
	require.NoError(t, err)

	ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 1 << 20})
	for _, explicit := range []bool{false, true} {
		if explicit {
			_, err := executor.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
		}
		result, err := executor.Execute(ctx, "MATCH (n:Review {p: 1}) RETURN n.p AS p LIMIT 1", nil)
		require.NoError(t, err, "explicit=%v", explicit)
		require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
		_, err = executor.Execute(ctx, "MATCH (n:Review {p: 2}) RETURN n.p AS p LIMIT 1", nil)
		requireMemoryError(t, err, "Neo.TransientError.General.TransactionMemoryLimit", "db.memory.transaction.max")
		if explicit {
			_, _ = executor.Execute(ctx, "ROLLBACK", nil)
		}
		requirePoolsEmpty(t, "nornic")
	}
}

// A traversal seeded from several relationship types holds one type's list
// at a time: a list it dropped no longer counts.
func TestRelationshipTypeSeedsReleaseEachType(t *testing.T) {
	resetTransactionMemory(t)
	store := memoryReviewEngine(t, 2)
	for _, typ := range []string{"LEFT", "RIGHT"} {
		for i := 0; i < 10000; i++ {
			require.NoError(t, store.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("%s-%d", typ, i)), StartNode: "review-0", EndNode: "review-1", Type: typ, Properties: map[string]interface{}{"p": int64(1)}}))
		}
	}
	for i := 0; i < 20000; i++ {
		require.NoError(t, store.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("BIG-%d", i)), StartNode: "review-0", EndNode: "review-1", Type: "BIG", Properties: map[string]interface{}{"p": int64(1)}}))
	}
	executor := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	requireWithinAndOver(t, executor, 1<<20,
		"MATCH (a)-[r:LEFT|RIGHT {p: -1}]->(b) RETURN b",
		"MATCH (a)-[r:BIG {p: -1}]->(b) RETURN b")
}

// A batch's prefetched nodes count until the rows that read them are done:
// the owner the consumer passes holds them, not the reader that found them.
func TestBatchPrefetchHeldByItsConsumer(t *testing.T) {
	resetTransactionMemory(t)
	store := memoryReviewEngine(t, 0)
	distinct := make(map[string]interface{}, 1000)
	rows := make([]map[string]interface{}, 0, 1000)
	for i := 0; i < 1000; i++ {
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("batch-%d", i)), Labels: []string{"Batch"}, Properties: map[string]interface{}{"p": int64(i)}})
		require.NoError(t, err)
		distinct[propEqKeyBatch(int64(i))] = int64(i)
		rows = append(rows, map[string]interface{}{"p": int64(i)})
	}
	executor := NewStorageExecutor(store)
	// The batch index reads each value from the property index; the label
	// scan reads the label.
	_, err := executor.Execute(context.Background(), "CREATE INDEX batch_p FOR (n:Batch) ON (n.p)", nil)
	require.NoError(t, err)
	key := nodeBatchMatchKey{label: "Batch", prop: "p"}
	run := func(limit int64, build func(ctx context.Context, owner *memoryRetention) (int, error)) (int, int64, error) {
		ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: limit})
		tracker := newTestMemoryTracker("nornic")
		ctx := context.WithValue(context.Background(), memoryScopeKey{}, &tracker.root)
		owner := trackedRetention(ctx)
		kept, err := build(ctx, owner)
		held := tracker.used.Load()
		owner.release()
		if err == nil {
			require.Zero(t, tracker.used.Load(), "the consumer's release gives back what the prefetch kept")
		}
		// A failed reservation is given back when the statement ends.
		tracker.end()
		requirePoolsEmpty(t, "nornic")
		return kept, held, err
	}
	// over is a limit the reads fit but the kept nodes don't: the label's
	// list (64 KB) and 1,000 kept nodes (64 KB) for the scan, 1,000 kept
	// nodes for the index lookups.
	over := map[string]int64{"label scan": 96 << 10, "batch index": 32 << 10}
	for name, build := range map[string]func(ctx context.Context, owner *memoryRetention) (int, error){
		"label scan": func(ctx context.Context, owner *memoryRetention) (int, error) {
			found, unique, err := buildNodeBatchLabelMatchIndex(ctx, owner, store, key, distinct)
			if err == nil {
				require.True(t, unique)
			}
			return len(found), err
		},
		"batch index": func(ctx context.Context, owner *memoryRetention) (int, error) {
			found, unique, err := executor.buildRelationshipBatchNodeMatchIndex(ctx, owner, store, rows, []matchClauseSpec{{label: "Batch", propName: "p", rowField: "p"}})
			if err == nil {
				require.True(t, unique)
			}
			return len(found[key]), err
		},
	} {
		t.Run(name, func(t *testing.T) {
			kept, held, err := run(1<<20, build)
			require.NoError(t, err)
			require.Equal(t, 1000, kept)
			require.GreaterOrEqual(t, held, int64(1000*entityReferenceBytes), "the kept nodes count while the consumer holds them")

			_, _, err = run(over[name], build)
			requireMemoryError(t, err, "Neo.TransientError.General.TransactionMemoryLimit", "db.memory.transaction.max")
		})
	}
}
