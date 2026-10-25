package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestBufferedFlushPreservesQueryViewAndCacheRevision(t *testing.T) {
	engine, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	engine.SetDecayEnabled(false)
	ns := storage.NewNamespacedEngine(engine, "nornic")
	cached := NewStorageExecutor(ns)
	uncached := NewStorageExecutorWithQueryCachePolicy(ns, 0, time.Minute)
	ctx := context.Background()
	const query = "MATCH (c:Category)<-[:PART_OF]-(p:Product) RETURN c.categoryName AS categoryName, count(p) AS productCount ORDER BY productCount DESC"
	type observation struct {
		raw, cached      *ExecuteResult
		rawErr, cacheErr error
	}
	observations := make(chan observation, 1)
	engine.OnNodeCreated(func(node *storage.Node) {
		if node.ID != "nornic:c" && (len(node.Labels) == 0 || node.Labels[0] != "Category") {
			return
		}
		raw, rawErr := uncached.Execute(ctx, query, nil)
		hit, cacheErr := cached.Execute(ctx, query, nil)
		observations <- observation{raw, hit, rawErr, cacheErr}
	})
	_, err = cached.Execute(ctx, "CREATE (c:Category {categoryName:'c'})<-[:PART_OF]-(p:Product {id:1})", nil)
	require.NoError(t, err)
	before, err := cached.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"c", int64(1)}}, before.Rows)
	versionBefore, _ := ns.GraphMutationVersion()
	hitsBefore, _, _, _, _ := cached.cache.Stats()

	require.NoError(t, engine.FlushWriteBehind())
	var got observation
	select {
	case got = <-observations:
	default:
		t.Fatal("missing category publication callback")
	}
	require.NoError(t, got.rawErr)
	require.NoError(t, got.cacheErr)
	require.Equal(t, before.Rows, got.raw.Rows, "durable and buffered adjacency must name each edge once")
	require.Equal(t, before.Rows, got.cached.Rows, "publication cannot change the logical result")
	versionAfter, _ := ns.GraphMutationVersion()
	require.Equal(t, versionBefore, versionAfter, "persistence must not republish an acknowledged graph change")
	hitsAfter, _, _, _, _ := cached.cache.Stats()
	require.Greater(t, hitsAfter, hitsBefore, "an already valid query cache entry survives persistence")
}
