package storage

import (
	"container/list"
	"sync"
	"sync/atomic"

	"github.com/orneryd/nornicdb/pkg/callerowned"
	"github.com/orneryd/nornicdb/pkg/util"
)

// =============================================================================
// BADGER ENGINE CACHE INVARIANTS + INVALIDATION
// =============================================================================
//
// This module centralizes cache writes/invalidations for BadgerEngine.
//
// Invariants:
//   - Node cache stores deep copies (see copyNode) and GetNode returns a deep
//     copy, so callers cannot mutate cached state.
//   - Edge type cache is an acceleration structure for GetEdgesByType and must
//     be invalidated whenever edges of a type are created/deleted OR when an
//     edge changes its Type.
//   - Cached node/edge counts are maintained on successful mutations to keep
//     Stats() O(1).
//
// Entry points:
//   - cacheOnNodeCreated / cacheOnNodeUpdated / cacheOnNodeDeleted
//   - cacheOnEdgeCreated / cacheOnEdgeUpdated / cacheOnEdgeDeleted
//
// These functions should be the only places that mutate/invalidate the caches
// in response to successful storage mutations.

type nodeBodyCacheEntry struct {
	itemVersion uint64
	node        *Node
	bytes       int64
	lruElement  *list.Element
	referenced  atomic.Bool
}

// cacheGeneration orders a read cache's fills against its writes (#1024).
// A read fills the cache with what it read after its read ends, so without
// it a read that started before a write could put back the version that
// write had just cached or removed, and later reads would return it. Every
// write to the cache (an entity written, deleted or invalidated) advances
// the generation while holding the cache's lock; a read captures it before
// it opens its view and, under the same lock, fills only if it hasn't
// moved.
type cacheGeneration struct{ n atomic.Uint64 }

func (g *cacheGeneration) current() uint64 { return g.n.Load() }

// advance records a write; the caller holds the cache's lock.
func (g *cacheGeneration) advance() { g.n.Add(1) }

// cacheStoreNode caches node as written (a write: see cacheGeneration).
func (b *BadgerEngine) cacheStoreNode(node *Node) {
	if node == nil {
		return
	}
	// Prepare immutable state before publication. Ordinary cache-hit copies
	// inherit this same owner record without allocating another one.
	cached := copyNode(node)
	state := cached.runtimeState()
	state.embeddingOwner, state.embeddingOwnerID = b, node.ID
	state.embeddingRevision, state.embeddingAttempt = EmbeddingBodyRevision{}, nil
	cached.setRuntimeState(state)
	b.nodeCacheGen.lockInvalidation(&b.nodeCacheMu)
	b.putNodeLocked(cached, nodeReadCacheFull)
	b.nodeCacheMu.Unlock()
	b.cacheDeleteNode(node.ID, nodeCacheBodyOnly)
}

// cacheFillNode caches node as read by a read that captured gen before it
// read; a write since then means node may be stale, and it isn't cached. The
// admission is decided under nodeCacheMu before the image is copied, so a fill
// that loses to a write, or a query image a full one supersedes, copies
// nothing (NOR-160).
func (b *BadgerEngine) cacheFillNode(gen uint64, node *Node, mode nodeReadCacheMode) {
	if node == nil {
		return
	}
	b.nodeCacheMu.Lock()
	if b.nodeCacheGen.current() != gen {
		b.nodeCacheMu.Unlock()
		return
	}
	if mode == nodeReadCacheQuery {
		if _, full := b.nodeCache[node.ID]; !full {
			// The cache keeps its own image; every exit copies it again.
			b.putNodeLocked(copyNode(node), nodeReadCacheQuery)
		}
		b.nodeCacheMu.Unlock()
		return
	}
	b.putNodeLocked(copyNode(node), nodeReadCacheFull)
	b.nodeCacheMu.Unlock()
	b.cacheDeleteNode(node.ID, nodeCacheBodyOnly)
}

// putNodeLocked stores an already-copied body in the selected cache while the
// caller holds nodeCacheMu. A full image supersedes its query-only image.
func (b *BadgerEngine) putNodeLocked(copied *Node, mode nodeReadCacheMode) {
	if mode == nodeReadCacheQuery {
		if b.queryNodeCache == nil || (b.nodeCacheMaxEntries > 0 && len(b.queryNodeCache) > b.nodeCacheMaxEntries) {
			b.queryNodeCache = make(map[NodeID]*Node)
		}
		b.queryNodeCache[copied.ID] = copied
		return
	}
	// Simple eviction: if cache is too large, clear it.
	// Keeps behavior consistent with existing code paths.
	if b.nodeCacheMaxEntries > 0 && len(b.nodeCache) > b.nodeCacheMaxEntries {
		b.nodeCache = make(map[NodeID]*Node, b.nodeCacheMaxEntries)
	}
	b.nodeCache[copied.ID] = copied
	delete(b.queryNodeCache, copied.ID)
}

// cacheLoadQueryNode returns a copy of id's GetQueryNode entry, sharing its
// in-memory vectors.
// Cached query images are immutable, so their copies can be made after the
// map lookup releases its read lock.
func (b *BadgerEngine) cacheLoadQueryNode(id NodeID) (*Node, bool) {
	b.nodeCacheMu.RLock()
	cached, ok := b.queryNodeCache[id]
	b.nodeCacheMu.RUnlock()
	if !ok {
		return nil, false
	}
	return copyNode(cached), true
}

// cacheLoadNodeBody materializes a cached body in the requested shape. keep,
// when set, decides on the borrowed body first: a body it rejects is a hit
// that returns nil without any copy (NOR-160).
func (b *BadgerEngine) cacheLoadNodeBody(id NodeID, itemVersion uint64, properties []string, keep ...func(*Node) bool) (*Node, bool) {
	if id == "" {
		return nil, false
	}
	b.nodeBodyCacheMu.RLock()
	entry, ok := b.nodeBodyCache[id]
	if !ok || entry.itemVersion != itemVersion || entry.node == nil {
		b.nodeBodyCacheMu.RUnlock()
		if ok {
			b.nodeBodyCacheMu.Lock()
			if current := b.nodeBodyCache[id]; current == entry {
				b.removeNodeBodyCacheEntryLocked(id, entry)
			}
			b.nodeBodyCacheMu.Unlock()
		}
		return nil, false
	}
	entry.referenced.Store(true)
	if len(keep) > 0 && keep[0] != nil && !keep[0](entry.node) {
		b.nodeBodyCacheMu.RUnlock()
		return nil, true
	}
	node := copyShaped(entry.node, properties, true)
	b.nodeBodyCacheMu.RUnlock()
	return node, true
}

func (b *BadgerEngine) cacheStoreNodeBody(id NodeID, itemVersion uint64, node *Node) {
	if id == "" || node == nil {
		return
	}
	// Inline vectors cannot be reconstructed from separate keys. Avoid caching
	// a lossy projection; large separately stored vectors are rehydrated on hit.
	if !node.RuntimeMetadata().EmbeddingsStoredSeparately && (len(node.ChunkEmbeddings) > 0 || len(node.NamedEmbeddings) > 0) {
		return
	}
	cached := copyNodeWithoutEmbeddings(node)
	retainedBytes := estimateNodeBodyCacheBytes(cached)
	if retainedBytes > b.nodeBodyCacheMaxBytes {
		return
	}
	b.nodeBodyCacheMu.Lock()
	if previous, ok := b.nodeBodyCache[id]; ok {
		b.removeNodeBodyCacheEntryLocked(id, previous)
	}
	entry := &nodeBodyCacheEntry{itemVersion: itemVersion, node: cached, bytes: retainedBytes}
	entry.referenced.Store(true)
	entry.lruElement = b.nodeBodyCacheLRU.PushFront(id)
	b.nodeBodyCache[id] = entry
	b.nodeBodyCacheBytes += retainedBytes
	for (b.nodeCacheMaxEntries > 0 && len(b.nodeBodyCache) > b.nodeCacheMaxEntries) ||
		(b.nodeBodyCacheMaxBytes > 0 && b.nodeBodyCacheBytes > b.nodeBodyCacheMaxBytes) {
		oldest := b.nodeBodyCacheLRU.Back()
		if oldest == nil {
			break
		}
		oldestID := oldest.Value.(NodeID)
		oldestEntry := b.nodeBodyCache[oldestID]
		if oldestEntry != nil && oldestEntry.referenced.Swap(false) && len(b.nodeBodyCache) > 1 {
			b.nodeBodyCacheLRU.MoveToFront(oldest)
			continue
		}
		b.removeNodeBodyCacheEntryLocked(oldestID, oldestEntry)
	}
	b.nodeBodyCacheMu.Unlock()
}

func (b *BadgerEngine) removeNodeBodyCacheEntryLocked(id NodeID, entry *nodeBodyCacheEntry) {
	delete(b.nodeBodyCache, id)
	if entry == nil {
		return
	}
	b.nodeBodyCacheBytes -= entry.bytes
	if b.nodeBodyCacheBytes < 0 {
		b.nodeBodyCacheBytes = 0
	}
	if entry.lruElement != nil {
		b.nodeBodyCacheLRU.Remove(entry.lruElement)
	}
}

func estimateNodeBodyCacheBytes(node *Node) int64 {
	if node == nil {
		return 0
	}
	bytes := int64(128 + len(node.ID))
	for _, label := range node.Labels {
		bytes += int64(16 + len(label))
	}
	bytes += estimateEntityPropertyBytes(node.Properties)
	for key, value := range node.EmbedMeta {
		bytes += int64(32+len(key)) + util.EstimateValueBytes(value)
	}
	return bytes
}

func estimateEntityPropertyBytes(properties map[string]interface{}) int64 {
	var bytes int64
	for key, value := range properties {
		bytes += int64(32+len(key)) + util.EstimateValueBytes(value)
	}
	return bytes
}

// edgeCacheRead describes what a reader asks of the edge body cache.
type edgeCacheRead struct {
	// version, when set, accepts only the body cached for that record
	// version, so a pinned read never sees a later body.
	version *uint64
	// bufferExcluded: the caller holds the write-behind buffer and has left
	// out every edge it buffers.
	bufferExcluded bool
	// match, when set, is the read's selection, decided on the retained
	// body before any copy; a hit it rejects returns nil and true.
	match func(*Edge) bool
}

// cacheLoadEdge is the edge body cache's exit for one relationship, and owns
// its one hit rule. The retained body never leaves the cache: the read's
// selection (match) is decided on it first, and only a selected hit is
// copied, once, for the read (NOR-160). Under write-behind the flusher can
// cache a body newer than a reader's committed view, so an unpinned read is
// answered only when it has excluded the buffered edges. Cached bodies are
// replaced wholesale and never changed in place, so match and the copy run
// after the lock is released.
func (b *BadgerEngine) cacheLoadEdge(id EdgeID, read edgeCacheRead) (edgeImage, bool) {
	if read.version == nil && !read.bufferExcluded && b.writeBehind != nil {
		return edgeImage{}, false
	}
	b.edgeCacheMu.RLock()
	cached, ok := b.edgeCache[id]
	if ok && read.version != nil && b.edgeCacheVersions[id] != *read.version {
		ok = false
	}
	b.edgeCacheMu.RUnlock()
	if !ok || cached == nil {
		return edgeImage{}, false
	}
	if read.match != nil && !read.match(cached) {
		return edgeImage{}, true
	}
	return decodedEdge(copyEdge(cached)), true
}

// edgeCacheList is one list a read asks of the edge body cache: the IDs in
// order, the ones the read has already answered elsewhere (skip), and the
// read's selection.
type edgeCacheList struct {
	ids  []EdgeID
	skip func(EdgeID) bool
	read edgeCacheRead
}

// cacheLoadEdgeLists answers several lists all or nothing: every listed body
// must be cached (and, for a pinned read, at its version) or the whole call
// misses before any copy. Only then is each list's selection decided on the
// retained bodies, and the selected ones copied once (NOR-160). A partial hit
// therefore never copies a body the fallback reads again.
func (b *BadgerEngine) cacheLoadEdgeLists(lists []edgeCacheList) ([][]*Edge, bool) {
	for _, list := range lists {
		if list.read.version == nil && !list.read.bufferExcluded && b.writeBehind != nil {
			return nil, false
		}
	}
	held := make([][]*Edge, len(lists))
	b.edgeCacheMu.RLock()
	for i, list := range lists {
		for _, id := range list.ids {
			if list.skip != nil && list.skip(id) {
				continue
			}
			cached, ok := b.edgeCache[id]
			if ok && list.read.version != nil && b.edgeCacheVersions[id] != *list.read.version {
				ok = false
			}
			if !ok || cached == nil {
				b.edgeCacheMu.RUnlock()
				return nil, false
			}
			held[i] = append(held[i], cached)
		}
	}
	b.edgeCacheMu.RUnlock()
	out := make([][]*Edge, len(lists))
	for i, list := range lists {
		selected := held[i][:0:0]
		for _, cached := range held[i] {
			if list.read.match == nil || list.read.match(cached) {
				selected = append(selected, cached)
			}
		}
		out[i] = callerowned.Each(selected, copyEdge)
	}
	return out, true
}

// cacheStoreEdge inserts edge into the per-engine edge body cache as written
// (a write: see cacheGeneration). Stores a deep copy so callers cannot
// mutate the cached state.
func (b *BadgerEngine) cacheStoreEdge(edge *Edge) {
	if edge == nil {
		return
	}
	b.edgeCacheGen.lockInvalidation(&b.edgeCacheMu)
	b.putEdgeLocked(edge, 0)
	b.edgeCacheMu.Unlock()
}

// cacheFillEdge caches edge as read by a read that captured gen before it
// read, unless the edge cache was written since.
func (b *BadgerEngine) cacheFillEdge(gen uint64, edge *Edge) { b.cacheFillEdgeAtVersion(gen, edge, 0) }

func (b *BadgerEngine) cacheFillEdgeAtVersion(gen uint64, edge *Edge, version uint64) {
	if edge == nil {
		return
	}
	b.edgeCacheMu.Lock()
	if b.edgeCacheGen.current() == gen {
		b.putEdgeLocked(edge, version)
	}
	b.edgeCacheMu.Unlock()
}

// putEdgeLocked stores a copy of edge, a body as storage returns it (see
// putNodeLocked); the caller holds edgeCacheMu.
func (b *BadgerEngine) putEdgeLocked(edge *Edge, version uint64) {
	if b.edgeCacheMaxItems > 0 && len(b.edgeCache) > b.edgeCacheMaxItems {
		b.edgeCache = make(map[EdgeID]*Edge, b.edgeCacheMaxItems)
		b.edgeCacheVersions = nil
	}
	cached := copyEdge(edge)
	b.edgeCache[edge.ID] = cached
	if version == 0 {
		delete(b.edgeCacheVersions, edge.ID)
	} else {
		if b.edgeCacheVersions == nil {
			b.edgeCacheVersions = make(map[EdgeID]uint64)
		}
		b.edgeCacheVersions[edge.ID] = version
	}
}

// lockInvalidation begins the mutation owned by this cache generation. The
// caller releases mu after its typed cache changes are complete.
func (generation *cacheGeneration) lockInvalidation(mu *sync.RWMutex) {
	mu.Lock()
	generation.advance()
}

// cacheDeleteEdge drops an edge from the body cache. Called from the edge
// mutation lifecycle hooks below.
func (b *BadgerEngine) cacheDeleteEdge(id EdgeID) {
	if id == "" {
		return
	}
	b.edgeCacheGen.lockInvalidation(&b.edgeCacheMu)
	delete(b.edgeCache, id)
	delete(b.edgeCacheVersions, id)
	b.edgeCacheMu.Unlock()
}

// cacheInvalidateEdges clears the entire edge body cache. Used by bulk
// delete and other callers that can't enumerate affected IDs cheaply.
func (b *BadgerEngine) cacheInvalidateEdges() {
	b.edgeCacheGen.lockInvalidation(&b.edgeCacheMu)
	b.edgeCache = make(map[EdgeID]*Edge, b.edgeCacheMaxItems)
	b.edgeCacheVersions = nil
	b.edgeCacheMu.Unlock()
}

// adjCacheLoadOutgoing returns the cached EdgeIDs incident to nodeID as
// outgoing, or (nil, false) on miss. Returned slice is shared with the
// cache; callers must treat it as read-only.
func (b *BadgerEngine) adjCacheLoadOutgoing(nodeID NodeID) ([]EdgeID, bool) {
	b.adjCacheMu.RLock()
	ids, ok := b.outgoingAdjCache[nodeID]
	b.adjCacheMu.RUnlock()
	return ids, ok
}

func (b *BadgerEngine) adjCacheLoadIncoming(nodeID NodeID) ([]EdgeID, bool) {
	b.adjCacheMu.RLock()
	ids, ok := b.incomingAdjCache[nodeID]
	b.adjCacheMu.RUnlock()
	return ids, ok
}

// adjCacheStoreOutgoing caches the outgoing IDs a read captured gen before
// reading, unless the adjacency cache was written since (cacheGeneration).
func (b *BadgerEngine) adjCacheStoreOutgoing(gen uint64, nodeID NodeID, ids []EdgeID) {
	b.adjCacheMu.Lock()
	defer b.adjCacheMu.Unlock()
	if b.adjCacheGen.current() != gen {
		return
	}
	if b.adjCacheMaxNodes > 0 && len(b.outgoingAdjCache) > b.adjCacheMaxNodes {
		b.outgoingAdjCache = make(map[NodeID][]EdgeID, b.adjCacheMaxNodes)
	}
	// Defensive copy: the iterator's growslice can re-allocate, but the
	// caller's slice is the canonical one. Store a fresh slice header.
	cached := make([]EdgeID, len(ids))
	copy(cached, ids)
	b.outgoingAdjCache[nodeID] = cached
}

// adjCacheStoreIncoming is adjCacheStoreOutgoing for incoming IDs.
func (b *BadgerEngine) adjCacheStoreIncoming(gen uint64, nodeID NodeID, ids []EdgeID) {
	b.adjCacheMu.Lock()
	defer b.adjCacheMu.Unlock()
	if b.adjCacheGen.current() != gen {
		return
	}
	if b.adjCacheMaxNodes > 0 && len(b.incomingAdjCache) > b.adjCacheMaxNodes {
		b.incomingAdjCache = make(map[NodeID][]EdgeID, b.adjCacheMaxNodes)
	}
	cached := make([]EdgeID, len(ids))
	copy(cached, ids)
	b.incomingAdjCache[nodeID] = cached
}

// adjCacheInvalidateForEdge drops the entries for both endpoints of edge.
// Called from edge create/update/delete lifecycle hooks. Cheap (two map
// deletes per edge mutation), so we don't bother batching.
func (b *BadgerEngine) adjCacheInvalidateForEdge(edge *Edge) {
	if edge == nil {
		return
	}
	b.adjCacheGen.lockInvalidation(&b.adjCacheMu)
	delete(b.outgoingAdjCache, edge.StartNode)
	delete(b.incomingAdjCache, edge.EndNode)
	b.adjCacheMu.Unlock()
}

// adjCacheInvalidate drops the selected nodes in both directions, or clears
// all adjacency when called with no IDs. Explicit empty IDs remain no-ops;
// a full invalidation of empty maps advances the generation without allocating.
func (b *BadgerEngine) adjCacheInvalidate(ids ...NodeID) {
	if len(ids) > 0 {
		nonempty := false
		for _, id := range ids {
			if id != "" {
				nonempty = true
				break
			}
		}
		if !nonempty {
			return
		}
	}
	b.adjCacheGen.lockInvalidation(&b.adjCacheMu)
	if len(ids) == 0 {
		if len(b.outgoingAdjCache) != 0 || len(b.incomingAdjCache) != 0 {
			b.outgoingAdjCache = make(map[NodeID][]EdgeID, b.adjCacheMaxNodes)
			b.incomingAdjCache = make(map[NodeID][]EdgeID, b.adjCacheMaxNodes)
		}
	} else {
		for _, id := range ids {
			if id != "" {
				delete(b.outgoingAdjCache, id)
				delete(b.incomingAdjCache, id)
			}
		}
	}
	b.adjCacheMu.Unlock()
}

func (b *BadgerEngine) labelCacheGetFirst(label string) (NodeID, bool) {
	if label == "" {
		return "", false
	}
	b.labelFirstNodeCacheMu.RLock()
	id, ok := b.labelFirstNodeCache[label]
	b.labelFirstNodeCacheMu.RUnlock()
	return id, ok
}

// labelCacheSetFirst caches the first node of label a read captured gen
// before reading, unless the label cache was written since (cacheGeneration).
func (b *BadgerEngine) labelCacheSetFirst(gen uint64, label string, id NodeID) {
	if label == "" || id == "" {
		return
	}
	b.labelFirstNodeCacheMu.Lock()
	defer b.labelFirstNodeCacheMu.Unlock()
	if b.labelFirstCacheGen.current() != gen {
		return
	}
	if b.labelFirstCacheMax > 0 && len(b.labelFirstNodeCache) > b.labelFirstCacheMax {
		b.labelFirstNodeCache = make(map[string]NodeID, b.labelFirstCacheMax)
	}
	b.labelFirstNodeCache[label] = id
}

// labelCacheDropStaleFirst drops label's cached first node id, which a read
// that captured gen found gone. It returns the generation the read keeps:
// still current when nothing else wrote the cache since gen, so the read
// may cache the first node it finds instead; otherwise stale.
func (b *BadgerEngine) labelCacheDropStaleFirst(gen uint64, label string, id NodeID) uint64 {
	b.labelFirstNodeCacheMu.Lock()
	defer b.labelFirstNodeCacheMu.Unlock()
	if cached, ok := b.labelFirstNodeCache[label]; ok && cached == id {
		delete(b.labelFirstNodeCache, label)
	}
	unchanged := b.labelFirstCacheGen.current() == gen
	b.labelFirstCacheGen.advance()
	if unchanged {
		return b.labelFirstCacheGen.current()
	}
	return gen
}

func (b *BadgerEngine) labelCacheInvalidateForNodeLabels(labels []string, nodeID NodeID) {
	if len(labels) == 0 || nodeID == "" {
		return
	}
	b.labelFirstNodeCacheMu.Lock()
	b.labelFirstCacheGen.advance()
	for _, label := range labels {
		if cached, ok := b.labelFirstNodeCache[label]; ok && cached == nodeID {
			delete(b.labelFirstNodeCache, label)
		}
	}
	b.labelFirstNodeCacheMu.Unlock()
}

func (b *BadgerEngine) labelCacheInvalidateForRemovedLabels(oldLabels, newLabels []string, nodeID NodeID) {
	if len(oldLabels) == 0 || nodeID == "" {
		return
	}
	if len(newLabels) == 0 {
		b.labelCacheInvalidateForNodeLabels(oldLabels, nodeID)
		return
	}

	newSet := make(map[string]struct{}, len(newLabels))
	for _, label := range newLabels {
		newSet[label] = struct{}{}
	}

	b.labelFirstNodeCacheMu.Lock()
	b.labelFirstCacheGen.advance()
	for _, label := range oldLabels {
		if _, ok := newSet[label]; ok {
			continue
		}
		if cached, ok := b.labelFirstNodeCache[label]; ok && cached == nodeID {
			delete(b.labelFirstNodeCache, label)
		}
	}
	b.labelFirstNodeCacheMu.Unlock()
}

type nodeCacheSelection uint8

const (
	nodeCacheBodyOnly nodeCacheSelection = iota
	nodeCacheAllForms
)

// cacheDeleteNode owns removal of one node's cached representations. The
// body-only selection preserves point-cache entries and their generation.
func (b *BadgerEngine) cacheDeleteNode(id NodeID, selection nodeCacheSelection) {
	if id == "" {
		return
	}
	if selection == nodeCacheAllForms {
		b.nodeCacheGen.lockInvalidation(&b.nodeCacheMu)
		delete(b.nodeCache, id)
		delete(b.queryNodeCache, id)
		b.nodeCacheMu.Unlock()
	}
	b.nodeBodyCacheMu.Lock()
	if entry, ok := b.nodeBodyCache[id]; ok {
		b.removeNodeBodyCacheEntryLocked(id, entry)
	}
	b.nodeBodyCacheMu.Unlock()
}

// cacheStoreWrittenNode caches node as a write just committed it, except a
// node still waiting for its managed embedding: the embed worker may store
// the embedding before this runs (a large transaction's commit fires its
// cache updates one by one after it is visible), and caching the written copy
// over it would hide the embedding from every read until the entry is
// evicted. Such a node's entry is dropped instead, and the next read loads
// it from storage.
func (b *BadgerEngine) cacheStoreWrittenNode(node *Node) {
	if node != nil && b.shouldIndexPendingEmbed(node) {
		b.cacheDeleteNode(node.ID, nodeCacheAllForms)
		return
	}
	b.cacheStoreNode(node)
}

func (b *BadgerEngine) cacheOnNodeCreated(node *Node) {
	b.cacheOnNodeCreatedWithCounts(node, true)
}

func (b *BadgerEngine) cacheOnNodeCreatedWithCounts(node *Node, publishCounts bool) {
	b.cacheStoreWrittenNode(node)
	if publishCounts {
		b.nodeCount.Add(1)
		b.addNamespaceNodeCount(node.ID, 1)
	}
	b.maintainPropertyIndexesOnNodeCreated(node)
}

func (b *BadgerEngine) cacheOnNodeUpdated(node *Node) {
	b.cacheStoreWrittenNode(node)
	// Updates are handled by cacheOnNodeUpdatedWithOldNode which has the
	// old-property context needed to remove stale index entries. The
	// `oldNode == nil` path is legitimately "we don't know the diff" —
	// fall back to treating it as a create so the new values at least
	// land in the indexes (no stale-removal possible without oldNode).
	b.maintainPropertyIndexesOnNodeCreated(node)
}

func (b *BadgerEngine) cacheOnNodeUpdatedWithOldNode(node *Node, oldNode *Node) {
	b.cacheStoreWrittenNode(node)
	if oldNode != nil {
		b.labelCacheInvalidateForRemovedLabels(oldNode.Labels, node.Labels, node.ID)
	}
	b.maintainPropertyIndexesOnNodeUpdated(node, oldNode)
}

// cacheOnNodeDeleted invalidates node cache and updates cached counts.
// edgesDeleted is the number of edges removed as part of deleting this node.
func (b *BadgerEngine) cacheOnNodeDeleted(id NodeID, edgesDeleted int64) {
	b.cacheOnNodeDeletedWithCounts(id, edgesDeleted, true)
}

func (b *BadgerEngine) cacheOnNodeDeletedWithCounts(id NodeID, edgesDeleted int64, publishCounts bool) {
	b.cacheDeleteNode(id, nodeCacheAllForms)

	// Decrement cached node count for O(1) stats.
	if publishCounts {
		b.nodeCount.Add(-1)
		b.addNamespaceNodeCount(id, -1)
	}

	// Decrement cached edge count for edges deleted with this node.
	if edgesDeleted > 0 {
		if publishCounts {
			b.edgeCount.Add(-edgesDeleted)
			b.addNamespaceEdgeCountFromNode(id, -edgesDeleted)
		}
		// We don't know which types were removed cheaply; invalidate whole type cache.
		b.InvalidateEdgeTypeCache()
		// Cascaded relationships have no separate delete operation. Clear
		// their cached bodies and adjacency before the generation retires.
		b.cacheInvalidateEdges()
		b.adjCacheInvalidate()
	}
}

func (b *BadgerEngine) cacheOnNodeDeletedWithNodeAndCounts(nodeID NodeID, deleted *Node, edgesDeleted int64, publishCounts bool) {
	b.cacheOnNodeDeletedWithCounts(nodeID, edgesDeleted, publishCounts)
	b.labelCacheInvalidateForNodeLabels(deleted.Labels, nodeID)

	// The pre-delete snapshot survives cache eviction and invalidation.
	b.maintainPropertyIndexesOnNodeDeletedWithNode(nodeID, deleted)
}

func (b *BadgerEngine) cacheOnEdgeCreated(edge *Edge) {
	b.cacheOnEdgeCreatedWithCounts(edge, true)
}

func (b *BadgerEngine) cacheOnEdgeCreatedWithCounts(edge *Edge, publishCounts bool) {
	if edge == nil {
		return
	}
	b.updateRelationshipUniqueIndexes(edge)
	b.InvalidateEdgeTypeCache(edge.Type)
	b.cacheStoreEdge(edge)
	b.adjCacheInvalidateForEdge(edge)
	if publishCounts {
		b.edgeCount.Add(1)
		b.addNamespaceEdgeCount(edge.ID, 1)
	}
}

// cacheOnEdgeUpdated invalidates relevant edge type cache entries when an edge changes.
func (b *BadgerEngine) cacheOnEdgeUpdated(oldType string, newEdge *Edge) {
	if newEdge == nil {
		return
	}
	b.updateRelationshipUniqueIndexes(newEdge)
	// If type changed, invalidate both old and new (old cache would still contain this edge).
	if oldType != "" && oldType != newEdge.Type {
		b.InvalidateEdgeTypeCache(oldType)
	}
	b.InvalidateEdgeTypeCache(newEdge.Type)
	b.cacheStoreEdge(newEdge)
	// Endpoints can change on update; the safe move is to drop the
	// adjacency entries for the new endpoints. (Old endpoints, if any
	// changed, are handled by the writer that supplied oldNode/oldEdge —
	// see the per-engine UpdateEdge path which calls this hook AFTER
	// already passing the stale pair through DeleteEdge index removal.)
	b.adjCacheInvalidateForEdge(newEdge)
}

func (b *BadgerEngine) cacheOnEdgeDeleted(id EdgeID, edgeType string) {
	b.cacheOnEdgeDeletedWithCounts(id, edgeType, true)
}

func (b *BadgerEngine) cacheOnEdgeDeletedWithCounts(id EdgeID, edgeType string, publishCounts bool) {
	b.deleteRelationshipUniqueIndexes(id)
	if edgeType != "" {
		b.InvalidateEdgeTypeCache(edgeType)
	} else {
		b.InvalidateEdgeTypeCache()
	}
	b.cacheDeleteEdge(id)
	// Without the edge body we can't target the endpoint nodes; clear
	// everything. Bulk delete paths use the same fallback. Adjacency
	// cache misses recover via a single index iteration on the next
	// read, so the worst case is one extra iteration per affected node.
	b.adjCacheInvalidate()
	if publishCounts {
		b.edgeCount.Add(-1)
		b.addNamespaceEdgeCount(id, -1)
	}
}

func (b *BadgerEngine) cacheOnEdgesDeleted(deletedIDs []EdgeID) {
	if len(deletedIDs) == 0 {
		return
	}
	b.deleteRelationshipUniqueIndexes(deletedIDs...)
	// Bulk delete may cover many types; invalidate once.
	b.InvalidateEdgeTypeCache()
	b.cacheInvalidateEdges()
	b.adjCacheInvalidate()
	b.edgeCount.Add(-int64(len(deletedIDs)))

	// Batch namespace updates under a single lock.
	deltas := make(map[string]int64)
	for _, id := range deletedIDs {
		prefix, ok := namespacePrefixFromID(string(id))
		if !ok {
			continue
		}
		deltas[prefix]--
	}
	if len(deltas) == 0 {
		return
	}
	b.namespaceCountsMu.Lock()
	for prefix, delta := range deltas {
		b.namespaceEdgeCounts[prefix] += delta
	}
	b.namespaceCountsMu.Unlock()
}

func (b *BadgerEngine) cacheOnNodesDeleted(deletedNodeIDs []NodeID, deletedNodeCount, totalEdgesDeleted int64) {
	if deletedNodeCount <= 0 {
		return
	}

	for _, nodeID := range deletedNodeIDs {
		b.cacheDeleteNode(nodeID, nodeCacheAllForms)
	}
	b.nodeCount.Add(-deletedNodeCount)

	// Update per-namespace node counts.
	namespaces := make(map[string]int64)
	for _, nodeID := range deletedNodeIDs {
		prefix, ok := namespacePrefixFromID(string(nodeID))
		if !ok {
			continue
		}
		namespaces[prefix]--
	}
	if len(namespaces) > 0 {
		b.namespaceCountsMu.Lock()
		for prefix, delta := range namespaces {
			b.namespaceNodeCounts[prefix] += delta
		}
		b.namespaceCountsMu.Unlock()
	}

	if totalEdgesDeleted > 0 {
		b.edgeCount.Add(-totalEdgesDeleted)

		// We only have an aggregate edge delete count. If the deleted nodes span
		// multiple namespaces, we can't attribute edges precisely.
		if len(namespaces) == 1 {
			for prefix := range namespaces {
				b.namespaceCountsMu.Lock()
				b.namespaceEdgeCounts[prefix] -= totalEdgesDeleted
				b.namespaceCountsMu.Unlock()
				break
			}
		}

		b.InvalidateEdgeTypeCache()
	}
}

func (b *BadgerEngine) addNamespaceNodeCount(id NodeID, delta int64) {
	prefix, ok := namespacePrefixFromID(string(id))
	if !ok {
		return
	}
	b.namespaceCountsMu.Lock()
	b.namespaceNodeCounts[prefix] += delta
	b.namespaceCountsMu.Unlock()
}

func (b *BadgerEngine) addNamespaceEdgeCount(id EdgeID, delta int64) {
	prefix, ok := namespacePrefixFromID(string(id))
	if !ok {
		return
	}
	b.namespaceCountsMu.Lock()
	b.namespaceEdgeCounts[prefix] += delta
	b.namespaceCountsMu.Unlock()
}

func (b *BadgerEngine) addNamespaceEdgeCountFromNode(nodeID NodeID, delta int64) {
	prefix, ok := namespacePrefixFromID(string(nodeID))
	if !ok {
		return
	}
	b.namespaceCountsMu.Lock()
	b.namespaceEdgeCounts[prefix] += delta
	b.namespaceCountsMu.Unlock()
}
