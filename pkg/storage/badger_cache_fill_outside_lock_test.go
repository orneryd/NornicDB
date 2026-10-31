package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// Author: #1548 implementation lane (Codex). Source arguments below adapt
// TJ's controls to required source-version admission; their schedules remain.
// cacheFillNode copies the body it is handed before it takes nodeCacheMu.
// These tests pin the two contracts that makes it safe to do so: a read that
// races a write still never publishes what it read, and a body is copied
// exactly once, so neither the read's image nor a caller's copy is ever the
// cache's object.

// getStoredBody decodes id's stored record the way getNodeByID's miss decode
// does, without going through a cache.
func (b *BadgerEngine) getStoredBody(id NodeID) (*Node, error) {
	var decoded *Node
	err := b.withView(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey(id))
		if err != nil {
			return err
		}
		return item.Value(func(value []byte) error {
			var decodeErr error
			decoded, decodeErr = b.decodeNode(namespaceForNodeID(id), value)
			return decodeErr
		})
	})
	return decoded, err
}

// The fill path's deep copy is published only if no write to the node cache
// happened since the read captured its generation, in either cache form:
// GetNode fills through cacheFillNode(gen, ..., nodeReadCacheFull) and
// GetQueryNode through cacheFillNode(gen, ..., nodeReadCacheQuery). This
// interleaving is the one the generation check under nodeCacheMu must keep
// dropping now that the copy runs before the lock.
func TestNodeCacheFillFromStorageReadRejectsStaleBodyAfterWrite(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mode    nodeReadCacheMode
		valueOf func(engine *BadgerEngine, id NodeID) (int64, bool)
	}{
		{
			name: "full body",
			mode: nodeReadCacheFull,
			valueOf: func(engine *BadgerEngine, id NodeID) (int64, bool) {
				cached, ok := engine.nodeCache[id]
				if !ok {
					return 0, false
				}
				return cached.Properties["v"].(int64), true
			},
		},
		{
			name: "query body",
			mode: nodeReadCacheQuery,
			valueOf: func(engine *BadgerEngine, id NodeID) (int64, bool) {
				cached, ok := engine.queryNodeCache[id]
				if !ok {
					return 0, false
				}
				return cached.Properties["v"].(int64), true
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
			require.NoError(t, err)
			defer engine.Close()

			const id NodeID = "fill:race"
			_, err = engine.CreateNode(&Node{ID: id, Labels: []string{"L"}, Properties: map[string]any{"v": int64(1)}})
			require.NoError(t, err)

			// A read captures gen, then decodes v=1 from storage.
			gen := engine.nodeCacheGen.current()
			source := engine.db.oracle.published.Load()
			decoded, err := engine.getStoredBody(id)
			require.NoError(t, err)

			// The write lands before the fill reaches the lock.
			err = engine.UpdateNode(NodeUpdateRequest{After: &Node{ID: id, Labels: []string{"L"}, Properties: map[string]any{"v": int64(2)}}})
			require.NoError(t, err)

			engine.cacheFillNode(gen, decoded, tc.mode, source)
			if value, ok := tc.valueOf(engine, id); ok {
				require.EqualValues(t, 2, value, "a stale fill must be dropped")
			}
			got, err := engine.GetNode(id)
			require.NoError(t, err)
			require.EqualValues(t, 2, got.Properties["v"])
		})
	}
}

// A cache fill always owns its own body: the image handed in by a storage read
// or by a caller is deep-copied, so neither side can observe or make a later
// mutation of the cache.
func TestNodeCacheFillIsolatesReadImageAndCallerCopy(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	const id NodeID = "fill:isolated"
	_, err = engine.CreateNode(&Node{ID: id, Labels: []string{"L"}, Properties: map[string]any{"v": int64(1), "keep": "yes"}})
	require.NoError(t, err)
	engine.cacheDeleteNode(id, nodeCacheAllForms) // force the next reads to fill from storage

	// A full fill: the decoded body handed over is not the cached body.
	decoded, err := engine.getStoredBody(id)
	require.NoError(t, err)
	engine.cacheFillNode(engine.nodeCacheGen.current(), decoded, nodeReadCacheFull, engine.db.oracle.published.Load())
	cached, ok := engine.nodeCache[id]
	require.True(t, ok)
	require.False(t, decoded == cached, "the cached body is not the read's image")
	require.Equal(t, decoded.Labels, cached.Labels)
	decoded.Properties["v"] = int64(99)
	decoded.Properties["added"] = true
	decoded.Labels[0] = "Mutated"
	require.Equal(t, map[string]any{"v": int64(1), "keep": "yes"}, engine.nodeCache[id].Properties,
		"cached body ignores a write to the read's image")
	require.Equal(t, []string{"L"}, engine.nodeCache[id].Labels)

	// A query fill: same isolation for the query entry.
	engine.cacheDeleteNode(id, nodeCacheAllForms)
	queryDecoded, err := engine.getStoredBody(id)
	require.NoError(t, err)
	engine.cacheFillNode(engine.nodeCacheGen.current(), queryDecoded, nodeReadCacheQuery, engine.db.oracle.published.Load())
	queryCached, ok := engine.queryNodeCache[id]
	require.True(t, ok)
	require.False(t, queryDecoded == queryCached, "the cached query body is not the read's image")
	queryDecoded.Properties["v"] = int64(99)
	require.EqualValues(t, 1, engine.queryNodeCache[id].Properties["v"],
		"cached query body ignores a write to the read's image")

	// A query hit handed to a caller is that caller's own copy.
	hit, ok := engine.cacheLoadQueryNode(id)
	require.True(t, ok)
	require.False(t, queryCached == hit, "a hit returns the caller's own copy")
	hit.Properties["v"] = int64(50)
	hit.Properties["caller"] = true
	hit.Labels[0] = "Caller"
	require.Equal(t, map[string]any{"v": int64(1), "keep": "yes"}, engine.queryNodeCache[id].Properties,
		"a caller cannot write the cached query body")
	require.Equal(t, []string{"L"}, engine.queryNodeCache[id].Labels)

	// A full hit through GetNode is a copy too.
	first, err := engine.GetNode(id)
	require.NoError(t, err)
	first.Properties["v"] = int64(42)
	first.Properties["caller"] = true
	first.Labels[0] = "Caller"
	second, err := engine.GetNode(id)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"v": int64(1), "keep": "yes"}, second.Properties,
		"a caller cannot write the cached body")
	require.Equal(t, []string{"L"}, second.Labels)
}

// Deterministic concurrency regression: every round decodes the pre-write
// bodies from storage, commits a write, and only then lets readers fill the
// node and query caches under the pre-write generation at once. Every fill
// must be dropped and both caches must hold the written version afterwards.
// The driver is exact rather than probabilistic: the generation moved before
// any reader reaches the lock, so each of the readers would publish a stale
// body if the copy's placement broke the ordering.
func TestNodeCacheFillOutsideLockKeepsGenerationOrdering(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()

	const id NodeID = "fill:concurrent"
	const readers = 8
	const rounds = 50
	value := int64(0)
	_, err = engine.CreateNode(&Node{ID: id, Labels: []string{"L"}, Properties: map[string]any{"v": value}})
	require.NoError(t, err)
	for round := 0; round < rounds; round++ {
		// Each reader decodes the pre-write body from storage.
		gen := engine.nodeCacheGen.current()
		source := engine.db.oracle.published.Load()
		bodies := make([]*Node, readers)
		for r := 0; r < readers; r++ {
			bodies[r], err = engine.getStoredBody(id)
			require.NoError(t, err)
			require.EqualValues(t, value, bodies[r].Properties["v"])
		}

		// Then a write lands, before any fill.
		value++
		require.NoError(t, engine.UpdateNode(NodeUpdateRequest{After: &Node{ID: id, Labels: []string{"L"}, Properties: map[string]any{"v": value}}}))

		var start sync.WaitGroup
		var filled sync.WaitGroup
		start.Add(1)
		filled.Add(readers)
		for r := 0; r < readers; r++ {
			mode := nodeReadCacheFull
			if r%2 == 1 {
				mode = nodeReadCacheQuery
			}
			go func(body *Node, mode nodeReadCacheMode) {
				defer filled.Done()
				start.Wait()
				engine.cacheFillNode(gen, body, mode, source)
			}(bodies[r], mode)
		}
		start.Done()
		filled.Wait()

		got, err := engine.GetNode(id)
		require.NoError(t, err)
		require.EqualValues(t, value, got.Properties["v"], "round %d: a stale fill was published", round)
		if cached, ok := engine.queryNodeCache[id]; ok {
			require.EqualValues(t, value, cached.Properties["v"], "round %d: stale query fill published", round)
		}
	}
}

// Reads that decode from storage while another goroutine writes must never
// publish the body they decoded before the write.
func TestNodeCacheConcurrentFillNeverPublishesStaleBody(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	defer engine.Close()
	engine.nodeCacheMaxEntries = 0 // no eviction: a stale fill would stay visible

	const id NodeID = "fill:hammer"
	_, err = engine.CreateNode(&Node{ID: id, Labels: []string{"L"}, Properties: map[string]any{"v": int64(0)}})
	require.NoError(t, err)

	var stop atomic.Bool
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for i := 0; !stop.Load(); i++ {
				gen := engine.nodeCacheGen.current()
				source := engine.db.oracle.published.Load()
				body, decodeErr := engine.getStoredBody(id)
				if decodeErr != nil {
					continue
				}
				engine.cacheFillNode(gen, body, nodeReadCacheMode(r%4), source)
				_, _ = engine.cacheLoadQueryNode(id)
			}
		}(r)
	}
	var wrote int64
	for wrote < 200 {
		wrote++
		engine.UpdateNode(NodeUpdateRequest{After: &Node{ID: id, Labels: []string{"L"}, Properties: map[string]any{"v": wrote}}})
	}
	stop.Store(true)
	wg.Wait()

	got, err := engine.GetNode(id)
	require.NoError(t, err)
	require.EqualValues(t, wrote, got.Properties["v"])
	if cached, ok := engine.queryNodeCache[id]; ok {
		require.EqualValues(t, wrote, cached.Properties["v"], "query cache never published a stale body")
	}
}

// nodeCacheFillBenchmarkFixture shares untimed setup only. The two benchmark
// loops retain their distinct generation loads and cache-fill behavior.
func nodeCacheFillBenchmarkFixture(b *testing.B, distinct int, tags bool) (*BadgerEngine, []*Node) {
	b.Helper()
	engine, err := fixtureowner.OwnResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngine(b.TempDir()))
	require.NoError(b, err)
	bodies := make([]*Node, distinct)
	for i := range bodies {
		properties := map[string]any{"name": fmt.Sprintf("node-%d", i), "score": int64(i)}
		if tags {
			properties["tags"] = []any{"a", "b"}
		}
		bodies[i] = &Node{ID: NodeID(fmt.Sprintf("bench:fill:%d", i)), Labels: []string{"Person", "Bench"}, Properties: properties}
	}
	return engine, bodies
}

// BenchmarkNodeCacheFillParallel measures cacheFillNode from many goroutines
// filling distinct nodes, the shape a wide label scan has. The deep copy runs
// outside nodeCacheMu, so the lock covers the generation check and map writes
// only; -mutexprofile attributes the remainder of this hot path.
// The engine it opens is a real store fixture: it enters the lifetime owner and
// is closed by it, exactly like a test fixture.
func BenchmarkNodeCacheFillParallel(b *testing.B) {
	const distinct = 1024
	engine, bodies := nodeCacheFillBenchmarkFixture(b, distinct, true)
	var next atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := int(next.Add(1)-1) % distinct
			engine.cacheFillNode(engine.nodeCacheGen.current(), bodies[i], nodeReadCacheFull, engine.db.oracle.published.Load())
		}
	})
}

// BenchmarkNodeCacheFillParallelStale is BenchmarkNodeCacheFillParallel with a
// generation that has moved, so each fill copies its body, takes the lock,
// finds the generation stale and drops it: the worst case for the copy's
// placement relative to the lock.
func BenchmarkNodeCacheFillParallelStale(b *testing.B) {
	const distinct = 1024
	engine, bodies := nodeCacheFillBenchmarkFixture(b, distinct, false)
	stale := engine.nodeCacheGen.current() - 1
	var next atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := int(next.Add(1)-1) % distinct
			engine.cacheFillNode(stale, bodies[i], nodeReadCacheFull, engine.db.oracle.published.Load())
		}
	})
}
