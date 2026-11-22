package storage

// Review lane R2 of PR #1826 (projected reads on by default). Test-only file
// that uses only identifiers present on the PR head and on its base.

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pr1826R2EvictPointImages removes a node's whole-node and query images the way
// the shared byte budget does (putNodeLocked): without touching the body cache
// and without advancing the cache generation.
func pr1826R2EvictPointImages(b *BadgerEngine, id NodeID) {
	b.nodeCacheMu.Lock()
	b.deleteNodeReadCacheLocked(id, nodeReadCacheFull)
	b.deleteNodeReadCacheLocked(id, nodeReadCacheQuery)
	b.nodeCacheMu.Unlock()
}

func pr1826R2Projected(t *testing.T, b *BadgerEngine, id NodeID, fields ...string) map[string]any {
	t.Helper()
	var last map[string]any
	for attempt := 0; attempt < 3; attempt++ {
		node, err := b.GetNodeProjected(t.Context(), id, fields)
		if !assert.NoError(t, err, "projected read %v (read %d)", fields, attempt) || node == nil {
			return nil
		}
		if attempt > 0 {
			assert.Equal(t, last, node.Properties, "repeated projected read %v changed (read %d)", fields, attempt)
		}
		last = node.Properties
	}
	return last
}

// Every write of a node must be what the next projected read shows, also when
// the node's whole-node image has been evicted and an older transaction reads
// the previous version in between.
func TestPR1826R2ProjectedReadsAfterWritesAndEviction(t *testing.T) {
	b := createTestBadgerEngine(t)
	ctx := t.Context()
	id := NodeID("test:r2-current")
	_, err := b.CreateNode(ctx, &Node{ID: id, Labels: []string{"A"}, Properties: map[string]any{"key": "v1", "other": "o1"}})
	require.NoError(t, err)
	b.cacheDeleteNode(id, nodeCacheAllForms)

	old, err := b.BeginTransaction(ctx, OrdinaryBegin)
	require.NoError(t, err)
	defer old.Rollback()
	pinned, err := old.GetNodeProjected(ctx, id, []string{"key"})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"key": "v1"}, pinned.Properties)

	assert.Equal(t, map[string]any{"key": "v1"}, pr1826R2Projected(t, b, id, "key"))
	assert.Equal(t, map[string]any{}, pr1826R2Projected(t, b, id, "absent"))
	assert.Equal(t, map[string]any{"key": "v1", "other": "o1"}, pr1826R2Projected(t, b, id, "key", "other", "absent"))

	require.NoError(t, b.UpdateNode(ctx, NodeUpdateRequest{After: &Node{ID: id, Labels: []string{"A", "B"}, Properties: map[string]any{"key": "v2", "other": "o2", "absent": "present"}}}))
	for round := 0; round < 3; round++ {
		pr1826R2EvictPointImages(b, id)
		if round == 1 {
			// Coarsen the per-node fill floors, as the engine does once more
			// entities were written than its caches hold.
			b.durableCaches.mu.Lock()
			b.durableCaches.advanceFloorLocked()
			b.durableCaches.mu.Unlock()
		}
		seen, err := old.GetNodeProjected(ctx, id, []string{"key", "other", "absent"})
		if assert.NoError(t, err, "old transaction read (round %d)", round) {
			assert.Equal(t, map[string]any{"key": "v1", "other": "o1"}, seen.Properties, "the older transaction keeps its version (round %d)", round)
		}
		assert.Equal(t, map[string]any{"key": "v2"}, pr1826R2Projected(t, b, id, "key"), "round %d", round)
		assert.Equal(t, map[string]any{"key": "v2", "other": "o2", "absent": "present"}, pr1826R2Projected(t, b, id, "key", "other", "absent"), "round %d", round)
		node, err := b.GetNodeProjected(ctx, id, []string{})
		if assert.NoError(t, err) {
			assert.ElementsMatch(t, []string{"A", "B"}, node.Labels, "labels after the update (round %d)", round)
			assert.Empty(t, node.Properties)
		}
		whole, err := b.GetNode(ctx, id)
		if assert.NoError(t, err) {
			assert.Equal(t, map[string]any{"key": "v2", "other": "o2", "absent": "present"}, whole.Properties, "round %d", round)
		}
	}

	require.NoError(t, b.DeleteNode(ctx, id))
	pr1826R2EvictPointImages(b, id)
	_, err = b.GetNodeProjected(ctx, id, []string{"key"})
	assert.True(t, errors.Is(err, ErrNotFound), "deleted node: %v", err)

	_, err = b.CreateNode(ctx, &Node{ID: id, Labels: []string{"C"}, Properties: map[string]any{"key": "v3"}})
	require.NoError(t, err)
	pr1826R2EvictPointImages(b, id)
	assert.Equal(t, map[string]any{"key": "v3"}, pr1826R2Projected(t, b, id, "key", "other", "absent"))
}

// A partial-prefix delete removes node records in bulk.
func TestPR1826R2ProjectedReadAfterPrefixDelete(t *testing.T) {
	b := createTestBadgerEngine(t)
	ctx := t.Context()
	kept, gone := NodeID("test:r2keep-1"), NodeID("test:r2drop-1")
	for _, id := range []NodeID{kept, gone} {
		_, err := b.CreateNode(ctx, &Node{ID: id, Labels: []string{"P"}, Properties: map[string]any{"key": string(id), "body": "b"}})
		require.NoError(t, err)
		b.cacheDeleteNode(id, nodeCacheAllForms)
		assert.Equal(t, map[string]any{"key": string(id)}, pr1826R2Projected(t, b, id, "key"))
	}
	_, _, err := b.DeleteByPrefix("test:r2drop-")
	require.NoError(t, err)
	pr1826R2EvictPointImages(b, gone)
	pr1826R2EvictPointImages(b, kept)
	_, err = b.GetNodeProjected(ctx, gone, []string{"key"})
	assert.True(t, errors.Is(err, ErrNotFound), "node under the deleted prefix: %v", err)
	assert.Equal(t, map[string]any{"key": string(kept)}, pr1826R2Projected(t, b, kept, "key"))
}

// A caller that changes a value it was given must not change what the next
// caller, or a wider read, is given.
func TestPR1826R2ProjectedResultsAreCallerOwned(t *testing.T) {
	b := createTestBadgerEngine(t)
	ctx := t.Context()
	id := NodeID("test:r2-owned")
	_, err := b.CreateNode(ctx, &Node{ID: id, Properties: map[string]any{
		"list": []any{"a", "b"}, "nested": map[string]any{"k": []any{int64(1), int64(2)}}, "raw": []byte{1, 2, 3}, "other": "o",
	}})
	require.NoError(t, err)
	b.cacheDeleteNode(id, nodeCacheAllForms)
	want, err := b.GetNode(ctx, id)
	require.NoError(t, err)
	b.cacheDeleteNode(id, nodeCacheAllForms)
	mutate := func(node *Node) {
		for _, value := range node.Properties {
			switch typed := value.(type) {
			case []any:
				if len(typed) > 0 {
					typed[0] = "changed"
				}
			case []string:
				if len(typed) > 0 {
					typed[0] = "changed"
				}
			case []byte:
				if len(typed) > 0 {
					typed[0] = 99
				}
			case map[string]any:
				for key := range typed {
					typed[key] = "changed"
				}
			}
		}
		node.Properties["injected"] = true
		if len(node.Labels) > 0 {
			node.Labels[0] = "changed"
		}
	}
	for _, fields := range [][]string{{"list"}, {"list", "nested"}, {"list", "nested", "raw"}, {"list", "nested", "raw", "other"}} {
		for attempt := 0; attempt < 3; attempt++ {
			node, err := b.GetNodeProjected(ctx, id, fields)
			require.NoError(t, err)
			for _, field := range fields {
				assert.Equal(t, want.Properties[field], node.Properties[field], "field %s of projection %v (read %d)", field, fields, attempt)
			}
			assert.Len(t, node.Properties, len(fields), "projection %v (read %d)", fields, attempt)
			mutate(node)
		}
	}
	whole, err := b.GetNode(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, want.Properties, whole.Properties)
}
