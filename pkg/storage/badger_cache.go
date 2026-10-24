package storage

import (
	"container/list"
	"sync/atomic"

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
// Written payloads enter once through commitWriter.finish and the shared
// publishCommitCacheImages owner. Operation hooks only maintain derived counts
// and indexes. Read fills carry their actual source view independently.

// Current-cache entries carry their source view's durable timestamp. An older
// payload can remain allocated after a commit, but can never answer a current
// read. Keep provenance inline in the map, without another allocation per node.
type nodeCacheEntry struct {
	*Node
	readTs uint64
}
type edgeCacheEntry struct {
	*Edge
	readTs    uint64
	coherence cacheKeyCoherence
}
type adjacencyCacheEntry struct {
	ids       []EdgeID
	readTs    uint64
	coherence cacheKeyCoherence
}
type edgeListCacheEntry struct {
	edges     []*Edge
	readTs    uint64
	coherence cacheKeyCoherence
}

// cacheVisibleAt is the single eligibility decision for current cache hits and
// fills. A pinned view may not consume a payload read from its future either.
// The source timestamp must come from the actual view, never fill/callback time.
func (b *BadgerEngine) cacheVisibleAt(sourceTs, viewTs uint64) bool {
	return cacheVisibleSince(sourceTs, viewTs, b.nodeCacheCommitTs.Load())
}

func cacheVisibleSince(sourceTs, viewTs, invalidationTs uint64) bool {
	return sourceTs >= invalidationTs && (viewTs == 0 || sourceTs <= viewTs)
}

// Known node writes remove their affected entries at durability. Unclassified
// writes additionally fence all older entries. A future entry cannot serve a
// pinned view. Caller holds nodeCacheMu; query-only entries cannot serve full reads.
func (b *BadgerEngine) cachedNodeLocked(id NodeID, query bool, viewTs uint64) (*Node, bool) {
	entry, ok := b.nodeCache[id]
	if ok && entry.Node != nil && cacheVisibleSince(entry.readTs, viewTs, b.nodeCacheInvalidationTs) {
		return entry.Node, true
	}
	if query {
		entry, ok = b.queryNodeCache[id]
		if ok && entry.Node != nil && cacheVisibleSince(entry.readTs, viewTs, b.nodeCacheInvalidationTs) {
			return entry.Node, true
		}
	}
	return nil, false
}

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

// cacheFillNode caches node as read by a read that captured gen before it
// read; a write since then means node may be stale, and it isn't cached.
func (b *BadgerEngine) cacheFillNode(gen uint64, node *Node, readTs uint64) {
	if node == nil {
		return
	}
	b.nodeCacheMu.Lock()
	if b.nodeCacheGen.current() != gen || !b.cacheVisibleAt(readTs, 0) {
		b.nodeCacheMu.Unlock()
		return
	}
	b.putNodeLocked(node, readTs)
	b.nodeCacheMu.Unlock()
	b.cacheDeleteNodeBody(node.ID)
}

// putNodeLocked stores a copy of node; the caller holds nodeCacheMu.
func (b *BadgerEngine) putNodeLocked(node *Node, readTs uint64) {
	// Simple eviction: if cache is too large, clear it.
	// Keeps behavior consistent with existing code paths.
	if b.nodeCacheMaxEntries > 0 && len(b.nodeCache) > b.nodeCacheMaxEntries {
		b.nodeCache = make(map[NodeID]nodeCacheEntry, b.nodeCacheMaxEntries)
	}
	cached := copyNode(node)
	b.nodeCache[node.ID] = nodeCacheEntry{Node: cached, readTs: readTs}
	delete(b.queryNodeCache, node.ID)
}

// cacheLoadQueryNode returns a copy of id's GetQueryNode entry, sharing its
// in-memory vectors.
func (b *BadgerEngine) cacheLoadQueryNode(id NodeID) (*Node, bool) {
	b.nodeCacheMu.RLock()
	defer b.nodeCacheMu.RUnlock()
	cached, ok := b.cachedNodeLocked(id, true, 0)
	if !ok {
		return nil, false
	}
	return copyNodeForCaller(cached), true
}

// cacheFillQueryNode caches node as GetQueryNode read it, under
// cacheFillNode's rule (no write since the read captured gen), unless
// nodeCache holds the full node.
func (b *BadgerEngine) cacheFillQueryNode(gen uint64, node *Node, readTs uint64) {
	if node == nil {
		return
	}
	b.nodeCacheMu.Lock()
	defer b.nodeCacheMu.Unlock()
	if b.nodeCacheGen.current() != gen || !b.cacheVisibleAt(readTs, 0) {
		return
	}
	if _, full := b.cachedNodeLocked(node.ID, false, 0); full {
		return
	}
	// Bounded like nodeCache: cleared when full.
	if b.queryNodeCache == nil || (b.nodeCacheMaxEntries > 0 && len(b.queryNodeCache) > b.nodeCacheMaxEntries) {
		b.queryNodeCache = make(map[NodeID]nodeCacheEntry)
	}
	cached := copyNodeForCaller(node)
	b.queryNodeCache[node.ID] = nodeCacheEntry{Node: cached, readTs: readTs}
}

func (b *BadgerEngine) cacheLoadNodeBody(id NodeID, itemVersion uint64) (*Node, bool) {
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
	node := copyNodeWithoutEmbeddings(entry.node)
	b.nodeBodyCacheMu.RUnlock()
	return node, true
}

func (b *BadgerEngine) cacheStoreNodeBody(id NodeID, itemVersion uint64, node *Node) {
	if id == "" || node == nil {
		return
	}
	// Inline vectors cannot be reconstructed from separate keys. Avoid caching
	// a lossy projection; large separately stored vectors are rehydrated on hit.
	if !node.EmbeddingsStoredSeparately && (len(node.ChunkEmbeddings) > 0 || len(node.NamedEmbeddings) > 0) {
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

func (b *BadgerEngine) cacheDeleteNodeBody(id NodeID) {
	if id == "" {
		return
	}
	b.nodeBodyCacheMu.Lock()
	if entry, ok := b.nodeBodyCache[id]; ok {
		b.removeNodeBodyCacheEntryLocked(id, entry)
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

// cacheCommitNodeWrites assigns eligibility in Badger commit order, before
// the commit becomes visible. A delayed older commit cannot regain eligibility.
// The returned generation authorizes one complete commit-image publication.
func (b *BadgerEngine) cacheCommitNodeWrites(ts uint64, scope nodeCacheWriteScope) uint64 {
	// Production terminals already prepared this scope before durability.
	// Direct test/maintenance issuance uses the same storage-free inventory.
	if !scope.relationshipPrepared && scope.known {
		var err error
		scope, err = b.prepareRelationshipCacheWrites(scope, nil, false)
		if err != nil {
			panic(err)
		}
	}
	b.nodeCacheMu.Lock()
	defer b.nodeCacheMu.Unlock()
	b.invalidateNodeCacheScopeLocked(scope, ts)
	b.invalidateRelationshipCacheScope(scope, ts)
	if ts <= b.nodeCacheCommitTs.Load() {
		return 0
	}
	b.nodeCacheCommitTs.Store(ts)
	b.nodeCacheGen.advance()
	return b.nodeCacheGen.current()
}

func (b *BadgerEngine) cacheDeleteNode(id NodeID) {
	if id == "" {
		return
	}
	b.nodeCacheMu.Lock()
	b.nodeCacheGen.advance()
	delete(b.nodeCache, id)
	delete(b.queryNodeCache, id)
	b.nodeCacheMu.Unlock()
	b.cacheDeleteNodeBody(id)
}

func (b *BadgerEngine) cacheOnNodeCreated(node *Node) {
	b.cacheOnNodeCreatedWithCounts(node, true)
}

func (b *BadgerEngine) cacheOnNodeCreatedWithCounts(node *Node, publishCounts bool) {
	if publishCounts {
		b.nodeCount.Add(1)
		b.addNamespaceNodeCount(node.ID, 1)
	}
	b.maintainPropertyIndexesOnNodeCreated(node)
}

func (b *BadgerEngine) cacheOnNodeUpdated(node *Node) {
	// Updates are handled by cacheOnNodeUpdatedWithOldNode which has the
	// old-property context needed to remove stale index entries. The
	// `oldNode == nil` path is legitimately "we don't know the diff" —
	// fall back to treating it as a create so the new values at least
	// land in the indexes (no stale-removal possible without oldNode).
	b.maintainPropertyIndexesOnNodeCreated(node)
}

func (b *BadgerEngine) cacheOnNodeUpdatedWithOldNode(node *Node, oldNode *Node) {
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
		// Cascaded bodies, endpoints and types were revoked by durable scope
		// issuance. The callback only publishes derived counts and indexes.
	}
}

func (b *BadgerEngine) cacheOnNodeDeletedWithLabels(nodeID NodeID, labels []string, edgesDeleted int64) {
	b.cacheOnNodeDeletedWithLabelsAndCounts(nodeID, labels, edgesDeleted, true)
}

func (b *BadgerEngine) cacheOnNodeDeletedWithLabelsAndCounts(nodeID NodeID, labels []string, edgesDeleted int64, publishCounts bool) {
	b.cacheOnNodeDeletedWithCounts(nodeID, edgesDeleted, publishCounts)
	b.labelCacheInvalidateForNodeLabels(labels, nodeID)

	// Remove stale property-index entries. Without labels + properties of
	// the deleted node we can't do targeted removal, so the narrower
	// cacheOnNodeDeleted (without labels) leaves the index potentially
	// dangling — callers MUST use this variant when property indexes exist.
	b.maintainPropertyIndexesOnNodeDeletedWithLabels(nodeID, labels)
}

func (b *BadgerEngine) cacheOnEdgeCreated(edge *Edge) {
	b.cacheOnEdgeCreatedWithCounts(edge, true)
}

func (b *BadgerEngine) cacheOnEdgeCreatedWithCounts(edge *Edge, publishCounts bool) {
	if edge == nil {
		return
	}
	b.updateRelationshipUniqueIndexes(edge)
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
}

func (b *BadgerEngine) cacheOnEdgeDeleted(id EdgeID, edgeType string) {
	b.cacheOnEdgeDeletedWithCounts(id, edgeType, true)
}

func (b *BadgerEngine) cacheOnEdgeDeletedWithCounts(id EdgeID, edgeType string, publishCounts bool) {
	b.deleteRelationshipUniqueIndexes(id)
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
