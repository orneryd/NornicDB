package storage

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestProjectedReadsProjectedBodyCacheCoverageAndOwnership(t *testing.T) {
	b := createTestBadgerEngine(t)
	id := NodeID("test:partial-cache")
	body := strings.Repeat("unrequested document body ", 1024)
	_, err := b.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"key": "one", "body": body, "bytes": []byte{1, 2, 3}}})
	require.NoError(t, err)
	b.cacheDeleteNode(id, nodeCacheAllForms)
	fields := []string{"key", "bytes", "absent"}
	first, err := b.GetNodeProjected(t.Context(), id, fields)
	require.NoError(t, err)
	b.nodeBodyCacheMu.RLock()
	entry := b.nodeBodyCache[id]
	b.nodeBodyCacheMu.RUnlock()
	require.NotNil(t, entry, "projected reads must retain their requested fields")
	require.Same(t, b, entry.node.runtime.embeddingOwner)
	require.Equal(t, id, entry.node.runtime.embeddingOwnerID)
	require.Equal(t, entry.itemVersion, entry.node.runtime.embeddingRevision.physical)
	require.NotContains(t, entry.node.Properties, "body")
	first.Properties["key"] = "changed"
	first.Properties["bytes"].([]byte)[0] = 99
	fields[0] = "body" // the cache must own its coverage independently of the caller
	again, err := b.GetNodeProjected(t.Context(), id, []string{"key", "bytes", "absent"})
	require.NoError(t, err)
	require.Equal(t, "one", again.Properties["key"])
	require.Equal(t, []byte{1, 2, 3}, again.Properties["bytes"])
	require.NotContains(t, again.Properties, "absent")
	b.nodeBodyCacheMu.RLock()
	require.Same(t, entry, b.nodeBodyCache[id])
	b.nodeBodyCacheMu.RUnlock()
	_, hit := b.cacheLoadNodeBody(id, entry.itemVersion, nil, nodeBodyDetached)
	require.False(t, hit, "partial entries cannot serve complete bodies")
	_, hit = b.cacheLoadNodeBody(id, entry.itemVersion, []string{"body"}, nodeBodyDetached)
	require.False(t, hit, "unread fields are not known absent")
	probe := b.readNodeBodyCache(id, entry.itemVersion, []string{"key", "body", "absent"}, nodeBodyDetached, true, false)
	require.False(t, probe.complete)
	require.Equal(t, []string{"body"}, probe.missing, "lazy decoding must exclude both cached values and known-absent keys")
	wider, err := b.GetNodeProjected(t.Context(), id, []string{"body", "absent"})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"body": body}, wider.Properties, "a wider cache must not leak unrequested fields")
	b.nodeBodyCacheMu.RLock()
	widened := b.nodeBodyCache[id]
	require.Len(t, b.nodeBodyCache, 1, "projections share one entity/version entry")
	require.Equal(t, entry.itemVersion, widened.itemVersion)
	require.Equal(t, "one", widened.node.Properties["key"])
	require.Equal(t, []byte{1, 2, 3}, widened.node.Properties["bytes"])
	require.Equal(t, body, widened.node.Properties["body"])
	require.Contains(t, widened.coverage, "absent")
	b.nodeBodyCacheMu.RUnlock()
	oldSubset, err := b.GetNodeProjected(t.Context(), id, []string{"key", "bytes", "absent"})
	require.NoError(t, err)
	require.Equal(t, "one", oldSubset.Properties["key"])
	b.nodeBodyCacheMu.RLock()
	require.Same(t, widened, b.nodeBodyCache[id], "the previous projection remains cached after lazy fill")
	b.nodeBodyCacheMu.RUnlock()
	full, err := b.GetNodeProjected(t.Context(), id, nil)
	require.NoError(t, err)
	require.Equal(t, body, full.Properties["body"])
	require.Len(t, full.Properties, 3)
	// Full point images may supersede the body cache; directly exercise its complete-image admission.
	b.cacheStoreNodeBody(id, entry.itemVersion, full, ^uint64(0))
	b.nodeBodyCacheMu.RLock()
	complete := b.nodeBodyCache[id]
	b.nodeBodyCacheMu.RUnlock()
	b.cacheStoreNodeBody(id, complete.itemVersion, &Node{ID: id, Properties: map[string]any{"key": "one"}}, ^uint64(0), []string{"key"})
	b.nodeBodyCacheMu.RLock()
	require.Same(t, complete, b.nodeBodyCache[id], "a partial admission must retain an existing complete image")
	b.nodeBodyCacheMu.RUnlock()
}

func TestProjectedReadsPhysicalSnapshotPreservesEmbeddingMetadata(t *testing.T) {
	b := createTestBadgerEngine(t)
	id := NodeID("test:projection-physical-snapshot")
	_, err := b.CreateNode(t.Context(), &Node{ID: id, Properties: map[string]any{"key": "one"}})
	require.NoError(t, err)
	writeback := embeddingWriteback(t, b, id, [][]float32{{1, 2, 3}}, map[string]any{"has_embedding": true, "chunk_count": 1}, time.Time{})
	require.NoError(t, b.UpdateNodeEmbeddingSidecar(t.Context(), writeback))
	tx, err := b.BeginTransaction(t.Context(), OrdinaryBegin)
	require.NoError(t, err)
	defer tx.Rollback()
	// Exercise the pinned physical-snapshot route used when no MVCC cutoff is
	// available; it must hydrate metadata without returning vector payloads.
	tx.readTS = MVCCVersion{}
	node, err := tx.GetNodeProjected(t.Context(), id, []string{"key", "has_embedding"})
	require.NoError(t, err)
	require.Equal(t, "one", node.Properties["key"])
	require.Equal(t, true, node.EmbedMeta["has_embedding"])
	require.Empty(t, node.ChunkEmbeddings)
	// Also exercise the physical snapshot owner itself when the engine's
	// write-behind shortcut is enabled for ordinary transaction reads.
	node, err = tx.getNodeFromBadgerSnapshotProjectedLocked(id, []string{"key", "has_embedding"})
	require.NoError(t, err)
	require.Equal(t, true, node.EmbedMeta["has_embedding"])
	require.Empty(t, node.ChunkEmbeddings)
}

func TestProjectedReadsProjectedBodyCacheSnapshotAndInvalidation(t *testing.T) {
	b := createTestBadgerEngine(t)
	id := NodeID("test:projected-version")
	_, err := b.CreateNode(t.Context(), &Node{ID: id, Properties: map[string]any{"key": "old", "body": "old body"}})
	require.NoError(t, err)
	b.cacheDeleteNode(id, nodeCacheAllForms)
	_, err = b.GetNodeProjected(t.Context(), id, []string{"key", "absent"})
	require.NoError(t, err)
	tx, err := b.BeginTransaction(t.Context(), OrdinaryBegin)
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, b.UpdateNode(t.Context(), NodeUpdateRequest{After: &Node{ID: id, Properties: map[string]any{"key": "new", "body": "new body", "absent": "now present"}}}))
	old, err := tx.GetNodeProjected(t.Context(), id, []string{"key", "absent"})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"key": "old"}, old.Properties)
	current, err := b.GetNodeProjected(t.Context(), id, []string{"key", "absent"})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"key": "new", "absent": "now present"}, current.Properties)
	require.NoError(t, b.DeleteNode(t.Context(), id))
	_, err = b.GetNodeProjected(t.Context(), id, []string{"key"})
	require.ErrorIs(t, err, ErrNotFound)
}

func TestProjectedReadsProjectedBodyCacheBudget(t *testing.T) {
	b := createTestBadgerEngine(t)
	b.nodeCacheMaxEntries = 1
	b.nodeBodyCacheMaxBytes = 2048
	for _, id := range []NodeID{"test:partial-one", "test:partial-two"} {
		_, err := b.CreateNode(t.Context(), &Node{ID: id, Properties: map[string]any{"key": string(id), "body": strings.Repeat("large", 1024)}})
		require.NoError(t, err)
		b.cacheDeleteNode(id, nodeCacheAllForms)
		_, err = b.GetNodeProjected(t.Context(), id, []string{"key", "absent"})
		require.NoError(t, err)
	}
	b.nodeBodyCacheMu.RLock()
	defer b.nodeBodyCacheMu.RUnlock()
	require.Len(t, b.nodeBodyCache, 1)
	require.LessOrEqual(t, b.nodeBodyCacheBytes, b.nodeBodyCacheMaxBytes)
	for _, entry := range b.nodeBodyCache {
		require.Greater(t, entry.bytes, estimateNodeBodyCacheBytes(entry.node), "coverage must count against the existing cache budget")
	}
}

func TestProjectedReadsProjectedBodyCacheLatestAdmission(t *testing.T) {
	b := createTestBadgerEngine(t)
	id := NodeID("test:projection-current")
	_, err := b.CreateNode(t.Context(), &Node{ID: id, Properties: map[string]any{"key": "one", "body": "body"}})
	require.NoError(t, err)
	b.cacheDeleteNode(id, nodeCacheAllForms)
	tx, err := b.BeginTransaction(t.Context(), OrdinaryBegin)
	require.NoError(t, err)
	defer tx.Rollback()
	_, err = tx.GetNodeProjected(t.Context(), id, []string{"key"})
	require.NoError(t, err)
	read := b.readNodeBodyCache(id, 0, []string{"key"}, nodeBodyBorrowed, false, true)
	require.False(t, read.complete, "snapshot reads do not authorize a latest-value cache hit")
	b.cacheDeleteNode(id, nodeCacheAllForms)
	_, err = b.GetNodeProjected(t.Context(), id, []string{"key"})
	require.NoError(t, err)
	b.nodeBodyCacheMu.RLock()
	require.True(t, b.nodeBodyCache[id].current)
	b.nodeBodyCacheMu.RUnlock()
	hits := atomic.LoadInt64(&b.cacheHits)
	_, err = b.GetNodeProjected(t.Context(), id, []string{"key"})
	require.NoError(t, err)
	require.Greater(t, atomic.LoadInt64(&b.cacheHits), hits, "a validated latest projection must avoid another physical lookup")
	require.NoError(t, b.UpdateNode(t.Context(), NodeUpdateRequest{After: &Node{ID: id, Properties: map[string]any{"key": "two", "body": "changed"}}}))
	latest, err := b.GetNodeProjected(t.Context(), id, []string{"key"})
	require.NoError(t, err)
	require.Equal(t, "two", latest.Properties["key"])
	old, err := tx.GetNodeProjected(t.Context(), id, []string{"key"})
	require.NoError(t, err)
	require.Equal(t, "one", old.Properties["key"])
	latest, err = b.GetNodeProjected(t.Context(), id, []string{"key"})
	require.NoError(t, err)
	require.Equal(t, "two", latest.Properties["key"])
}

func TestProjectedReadsProjectedBodyCacheConcurrentWrites(t *testing.T) {
	b := createTestBadgerEngine(t)
	id := NodeID("test:projection-concurrent")
	_, err := b.CreateNode(t.Context(), &Node{ID: id, Properties: map[string]any{"sequence": int64(0), "body": "body"}})
	require.NoError(t, err)
	b.cacheDeleteNode(id, nodeCacheAllForms)
	failures := make(chan error, 4)
	var readers sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		readers.Go(func() {
			previous := int64(-1)
			for i := 0; i < 300; i++ {
				node, err := b.GetNodeProjected(t.Context(), id, []string{"sequence", "absent"})
				if err != nil {
					failures <- err
					return
				}
				current := node.Properties["sequence"].(int64)
				if current < previous {
					failures <- fmt.Errorf("latest projection went backwards: %d -> %d", previous, current)
					return
				}
				previous = current
			}
		})
	}
	for i := int64(1); i <= 50; i++ {
		err := b.UpdateNode(t.Context(), NodeUpdateRequest{After: &Node{ID: id, Properties: map[string]any{"sequence": i, "body": "body"}}})
		require.NoError(t, err)
		node, err := b.GetNodeProjected(t.Context(), id, []string{"sequence"})
		require.NoError(t, err)
		require.Equal(t, i, node.Properties["sequence"], "completed writes must be immediately visible")
	}
	readers.Wait()
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
}

func TestProjectedReadsProjectedBodyCacheConcurrentLazyFill(t *testing.T) {
	b := createTestBadgerEngine(t)
	id := NodeID("test:projection-union")
	_, err := b.CreateNode(t.Context(), &Node{ID: id, Properties: map[string]any{"a": int64(1), "b": int64(2), "c": int64(3)}})
	require.NoError(t, err)
	b.cacheDeleteNode(id, nodeCacheAllForms)
	_, err = b.GetNodeProjected(t.Context(), id, []string{"a"})
	require.NoError(t, err)
	start := make(chan struct{})
	failures := make(chan error, 2)
	var readers sync.WaitGroup
	for _, key := range []string{"b", "c"} {
		readers.Go(func() {
			<-start
			node, err := b.GetNodeProjected(t.Context(), id, []string{key})
			if err != nil {
				failures <- err
				return
			}
			if len(node.Properties) != 1 {
				failures <- fmt.Errorf("projection %s leaked other cached fields", key)
			}
		})
	}
	close(start)
	readers.Wait()
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
	b.nodeBodyCacheMu.RLock()
	defer b.nodeBodyCacheMu.RUnlock()
	require.Len(t, b.nodeBodyCache, 1)
	entry := b.nodeBodyCache[id]
	require.NotNil(t, entry)
	require.Equal(t, map[string]any{"a": int64(1), "b": int64(2), "c": int64(3)}, entry.node.Properties)
	require.Len(t, entry.coverage, 3, "concurrent wider projections must preserve each other's fills")
}
