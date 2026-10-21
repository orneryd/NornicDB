package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// flushPublicationObservation is what the Category creation listener saw.
type flushPublicationObservation struct {
	raw, cached      *ExecuteResult
	rawErr, cacheErr error
}

// awaitFlushPublication waits up to within for the listener's observation.
// FlushWriteBehind is a barrier for durability, publication and the order of
// listener notifications, not for listener completion: the engine dispatches
// a readied notification on whichever goroutine drains the queue, and a drain
// already running elsewhere returns at once (drainCommitNotifications), as
// for every write. Only Close waits for every listener (notifyWG).
func awaitFlushPublication(observations <-chan flushPublicationObservation, within time.Duration) (flushPublicationObservation, bool) {
	select {
	case got := <-observations:
		return got, true
	case <-time.After(within):
		return flushPublicationObservation{}, false
	}
}

// bufferedFlushPublication creates a Category under write-behind, caches a
// query over it, registers a Category creation listener that re-runs the
// query (cached and uncached) unless dropNotification is set, and flushes.
func bufferedFlushPublication(t *testing.T, dropNotification bool) (before *ExecuteResult, observations <-chan flushPublicationObservation, ns *storage.NamespacedEngine, cached *StorageExecutor, versionBefore uint64, hitsBefore int64) {
	t.Helper()
	engine, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	engine.SetDecayEnabled(false)
	ns = storage.NewNamespacedEngine(engine, "nornic")
	cached = NewStorageExecutor(ns)
	uncached := NewStorageExecutorWithQueryCachePolicy(ns, 0, time.Minute)
	ctx := context.Background()
	const query = "MATCH (c:Category)<-[:PART_OF]-(p:Product) RETURN c.categoryName AS categoryName, count(p) AS productCount ORDER BY productCount DESC"
	_, err = cached.Execute(ctx, "CREATE (c:Category {categoryName:'c'})<-[:PART_OF]-(p:Product {id:1})", nil)
	require.NoError(t, err)
	before, err = cached.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"c", int64(1)}}, before.Rows)
	versionBefore, _ = ns.GraphMutationVersion()
	hitsBefore, _, _, _, _ = cached.cache.Stats()
	results := make(chan flushPublicationObservation, 1)
	engine.OnNodeCreated(func(node *storage.Node) {
		if dropNotification || (node.ID != "nornic:c" && (len(node.Labels) == 0 || node.Labels[0] != "Category")) {
			return
		}
		raw, rawErr := uncached.Execute(ctx, query, nil)
		hit, cacheErr := cached.Execute(ctx, query, nil)
		results <- flushPublicationObservation{raw, hit, rawErr, cacheErr}
	})
	require.NoError(t, engine.FlushWriteBehind())
	return before, results, ns, cached, versionBefore, hitsBefore
}

func TestBufferedFlushPreservesQueryViewAndCacheRevision(t *testing.T) {
	before, observations, ns, cached, versionBefore, hitsBefore := bufferedFlushPublication(t, false)
	got, ok := awaitFlushPublication(observations, 10*time.Second)
	require.True(t, ok, "missing category publication callback")
	require.NoError(t, got.rawErr)
	require.NoError(t, got.cacheErr)
	require.Equal(t, before.Rows, got.raw.Rows, "durable and buffered adjacency must name each edge once")
	require.Equal(t, before.Rows, got.cached.Rows, "publication cannot change the logical result")
	versionAfter, _ := ns.GraphMutationVersion()
	require.Equal(t, versionBefore, versionAfter, "persistence must not republish an acknowledged graph change")
	hitsAfter, _, _, _, _ := cached.cache.Stats()
	require.Greater(t, hitsAfter, hitsBefore, "an already valid query cache entry survives persistence")
}

// The bounded wait still fails on a lost notification: with the listener's
// notification dropped, no observation arrives within the bound.
func TestBufferedFlushPublicationWaitCatchesADroppedNotification(t *testing.T) {
	_, observations, _, _, _, _ := bufferedFlushPublication(t, true)
	_, ok := awaitFlushPublication(observations, 500*time.Millisecond)
	require.False(t, ok, "a dropped notification must not pass the wait")
}
