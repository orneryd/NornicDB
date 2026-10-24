package storage

// A key's revocation record outlives its payload. Only this owner may change
// it: invalidation raises the high-water mark; publication and discard never
// lower it. Each record stays inline with its cache entry (no second lookup).
type cacheKeyCoherence struct{ writeTs uint64 }

// issued is the greatest key revocation in this domain. A bounded reset moves
// it into floor before forgetting records, even if an oracle hole keeps the
// engine's published/commit watermark behind a just-issued revocation.
type cacheCoherenceDomain struct{ floor, issued uint64 }

func (d *cacheCoherenceDomain) revoke(key *cacheKeyCoherence, ts uint64) {
	d.issued = max(d.issued, ts)
	key.writeTs = max(key.writeTs, ts)
}

func (d *cacheCoherenceDomain) revokeAll(ts uint64) {
	d.issued = max(d.issued, ts)
	d.reset()
}

func (d *cacheCoherenceDomain) reset() { d.floor = max(d.floor, d.issued) }

func (key cacheKeyCoherence) admits(sourceTs, viewTs uint64, domain cacheCoherenceDomain) bool {
	return cacheVisibleSince(sourceTs, viewTs, max(key.writeTs, domain.floor))
}

func (b *BadgerEngine) revokeEdgeLocked(id EdgeID, ts uint64) {
	entry := b.edgeCache[id]
	b.edgeCoherence.revoke(&entry.coherence, ts)
	b.edgeCache[id] = entry
}

func (b *BadgerEngine) revokeAdjacencyLocked(id NodeID, outgoing bool, ts uint64) {
	cache := b.incomingAdjCache
	if outgoing {
		cache = b.outgoingAdjCache
	}
	entry := cache[id]
	b.adjCoherence.revoke(&entry.coherence, ts)
	cache[id] = entry
}

func (b *BadgerEngine) revokeEdgeTypeLocked(kind string, ts uint64) {
	entry := b.edgeTypeCache[kind]
	b.edgeTypeCoherence.revoke(&entry.coherence, ts)
	b.edgeTypeCache[kind] = entry
}

func (b *BadgerEngine) discardEdgePayloadLocked(id EdgeID) {
	if entry, exists := b.edgeCache[id]; exists {
		entry.Edge = nil
		b.edgeCache[id] = entry
	}
	delete(b.edgeCacheVersions, id)
}

func (b *BadgerEngine) resetEdgeCacheLocked() {
	b.edgeCoherence.reset()
	b.edgeCache = make(map[EdgeID]edgeCacheEntry)
	b.edgeCacheVersions = nil
}

func (b *BadgerEngine) resetAdjacencyCacheLocked() {
	b.adjCoherence.reset()
	b.outgoingAdjCache = make(map[NodeID]adjacencyCacheEntry)
	b.incomingAdjCache = make(map[NodeID]adjacencyCacheEntry)
}

func (b *BadgerEngine) resetEdgeTypeCacheLocked() {
	b.edgeTypeCoherence.reset()
	b.edgeTypeCache = make(map[string]edgeListCacheEntry)
}

func (b *BadgerEngine) revokeRelationshipCachesLocked(ts uint64) {
	b.edgeCoherence.revokeAll(ts)
	b.adjCoherence.revokeAll(ts)
	b.edgeTypeCoherence.revokeAll(ts)
}

// cacheLoadEdge returns the cached edge pointer if present.
//
// The pointer is shared with the cache and any other caller that took a
// hit since the last write. Callers MUST treat the returned edge as
// immutable; any mutation must clone via CopyEdge first. The cached body
// is replaced wholesale on UpdateEdge / CreateEdge and dropped on
// DeleteEdge, so a stable read keeps observing the value at the moment
// of the cache hit even if a concurrent writer races.
//
// This is the hot path for BFS traversals (shortestPath, variable-length
// MATCH). Profiling showed a per-hit copyEdge was ~33% of the warm
// shortestPath benchmark. None of the current Get*Edges callers mutate
// the returned edges, so sharing is correct.
func (b *BadgerEngine) cacheLoadEdge(id EdgeID) (*Edge, bool) {
	return b.cacheLoadEdgeInView(id, 0)
}

func (b *BadgerEngine) cacheLoadEdgeInView(id EdgeID, viewTs uint64) (*Edge, bool) {
	b.edgeCacheMu.RLock()
	defer b.edgeCacheMu.RUnlock()
	cached, ok := b.edgeCache[id]
	if !ok || cached.Edge == nil || !cached.coherence.admits(cached.readTs, viewTs, b.edgeCoherence) {
		return nil, false
	}
	return cached.Edge, true
}

// cacheFillEdge caches edge as read by a read that captured gen before it
// read, unless the edge cache was written since.
func (b *BadgerEngine) cacheFillEdge(gen uint64, edge *Edge, readTs uint64) {
	b.cacheFillEdgeAtVersion(gen, edge, 0, readTs)
}

func (b *BadgerEngine) cacheFillEdgeAtVersion(gen uint64, edge *Edge, version, readTs uint64) {
	if edge == nil {
		return
	}
	b.edgeCacheMu.Lock()
	if b.edgeCacheGen.current() == gen {
		b.putEdgeLocked(edge, version, readTs)
	}
	b.edgeCacheMu.Unlock()
}

// putEdgeLocked stores a copy of edge; the caller holds edgeCacheMu.
func (b *BadgerEngine) putEdgeLocked(edge *Edge, version, readTs uint64) {
	if b.edgeCacheMaxItems > 0 && len(b.edgeCache) > b.edgeCacheMaxItems {
		b.resetEdgeCacheLocked()
	}
	entry := b.edgeCache[edge.ID]
	if !entry.coherence.admits(readTs, 0, b.edgeCoherence) {
		return
	}
	cached := copyEdge(edge)
	entry.Edge, entry.readTs = cached, readTs
	b.edgeCache[edge.ID] = entry
	if version == 0 {
		delete(b.edgeCacheVersions, edge.ID)
	} else {
		if b.edgeCacheVersions == nil {
			b.edgeCacheVersions = make(map[EdgeID]uint64)
		}
		b.edgeCacheVersions[edge.ID] = version
	}
}

// cacheDeleteEdge drops an edge from the body cache. Called from the edge
// mutation lifecycle hooks below.
func (b *BadgerEngine) cacheDeleteEdge(id EdgeID) {
	if id == "" {
		return
	}
	b.edgeCacheMu.Lock()
	b.edgeCacheGen.advance()
	b.discardEdgePayloadLocked(id)
	b.edgeCacheMu.Unlock()
}

// cacheInvalidateEdges clears the entire edge body cache. Used by bulk
// delete and other callers that can't enumerate affected IDs cheaply.
func (b *BadgerEngine) cacheInvalidateEdges() {
	b.edgeCacheMu.Lock()
	b.edgeCacheGen.advance()
	b.resetEdgeCacheLocked()
	b.edgeCacheMu.Unlock()
}

// adjCacheLoadOutgoing returns the cached EdgeIDs incident to nodeID as
// outgoing, or (nil, false) on miss. Returned slice is shared with the
// cache; callers must treat it as read-only.
func (b *BadgerEngine) adjCacheLoadOutgoing(nodeID NodeID) ([]EdgeID, bool) {
	b.adjCacheMu.RLock()
	defer b.adjCacheMu.RUnlock()
	ids, ok := b.outgoingAdjCache[nodeID]
	return ids.ids, ok && ids.ids != nil && ids.coherence.admits(ids.readTs, 0, b.adjCoherence)
}

func (b *BadgerEngine) adjCacheLoadIncoming(nodeID NodeID) ([]EdgeID, bool) {
	b.adjCacheMu.RLock()
	defer b.adjCacheMu.RUnlock()
	ids, ok := b.incomingAdjCache[nodeID]
	return ids.ids, ok && ids.ids != nil && ids.coherence.admits(ids.readTs, 0, b.adjCoherence)
}

// adjCacheStoreOutgoing caches the outgoing IDs a read captured gen before
// reading, unless the adjacency cache was written since (cacheGeneration).
func (b *BadgerEngine) adjCacheStoreOutgoing(gen uint64, nodeID NodeID, ids []EdgeID, readTs uint64) {
	b.storeAdjacencyCache(gen, nodeID, ids, readTs, true)
}

// adjCacheStoreIncoming is adjCacheStoreOutgoing for incoming IDs.
func (b *BadgerEngine) adjCacheStoreIncoming(gen uint64, nodeID NodeID, ids []EdgeID, readTs uint64) {
	b.storeAdjacencyCache(gen, nodeID, ids, readTs, false)
}

// Both endpoint directions publish through the same version/fill owner. A
// paused old read must not hold the lock needed by pre-publication issuance.
func (b *BadgerEngine) storeAdjacencyCache(gen uint64, nodeID NodeID, ids []EdgeID, readTs uint64, outgoing bool) {
	if hook := b.adjCacheFillHook.Load(); hook != nil {
		(*hook)()
	}
	b.adjCacheMu.Lock()
	defer b.adjCacheMu.Unlock()
	cache := b.incomingAdjCache
	if outgoing {
		cache = b.outgoingAdjCache
	}
	if b.adjCacheGen.current() != gen {
		return
	}
	if b.adjCacheMaxNodes > 0 && len(cache) > b.adjCacheMaxNodes {
		b.resetAdjacencyCacheLocked()
		cache = b.incomingAdjCache
		if outgoing {
			cache = b.outgoingAdjCache
		}
	}
	entry := cache[nodeID]
	if !entry.coherence.admits(readTs, 0, b.adjCoherence) {
		return
	}
	cached := make([]EdgeID, len(ids))
	copy(cached, ids)
	entry.ids, entry.readTs = cached, readTs
	cache[nodeID] = entry
}

// adjCacheInvalidateForEdge drops the entries for both endpoints of edge.
// Called from edge create/update/delete lifecycle hooks. Cheap (two map
// deletes per edge mutation), so we don't bother batching.
func (b *BadgerEngine) adjCacheInvalidateForEdge(edge *Edge) {
	if edge == nil {
		return
	}
	b.adjCacheMu.Lock()
	b.adjCacheGen.advance()
	if out, exists := b.outgoingAdjCache[edge.StartNode]; exists {
		out.ids = nil
		b.outgoingAdjCache[edge.StartNode] = out
	}
	if in, exists := b.incomingAdjCache[edge.EndNode]; exists {
		in.ids = nil
		b.incomingAdjCache[edge.EndNode] = in
	}
	b.adjCacheMu.Unlock()
}

// adjCacheInvalidateAll clears the entire adjacency cache. Used by edge
// deletion paths that can't cheaply enumerate the affected node IDs. An
// empty cache is left as it is: a commit that deletes many edges calls this
// once per edge, and re-allocating two maps sized for the whole cache each
// time made deleting n edges cost n full-size allocations.
func (b *BadgerEngine) adjCacheInvalidateAll() {
	b.adjCacheMu.Lock()
	b.adjCacheGen.advance()
	if len(b.outgoingAdjCache) == 0 && len(b.incomingAdjCache) == 0 {
		b.adjCacheMu.Unlock()
		return
	}
	b.resetAdjacencyCacheLocked()
	b.adjCacheMu.Unlock()
}

// edgeTypeCacheFill caches the edges of edgeType a read captured gen before
// reading (simple LRU-style: clear if too many types), unless the type cache
// was written since (cacheGeneration).
func (b *BadgerEngine) edgeTypeCacheFill(gen uint64, edgeType string, edges []*Edge, readTs uint64) {
	b.edgeTypeCacheMu.Lock()
	defer b.edgeTypeCacheMu.Unlock()
	if b.edgeTypeCacheGen.current() != gen {
		return
	}
	if b.edgeTypeCacheMaxTypes > 0 && len(b.edgeTypeCache) > b.edgeTypeCacheMaxTypes {
		b.resetEdgeTypeCacheLocked()
	}
	entry := b.edgeTypeCache[edgeType]
	if !entry.coherence.admits(readTs, 0, b.edgeTypeCoherence) {
		return
	}
	if edges == nil {
		edges = []*Edge{}
	}
	entry.edges, entry.readTs = edges, readTs
	b.edgeTypeCache[edgeType] = entry
}

// InvalidateEdgeTypeCache clears the entire edge type cache.
// Called after bulk edge mutations to ensure cache consistency.
func (b *BadgerEngine) InvalidateEdgeTypeCache() {
	b.edgeTypeCacheMu.Lock()
	b.edgeTypeCacheGen.advance()
	b.resetEdgeTypeCacheLocked()
	b.edgeTypeCacheMu.Unlock()
}

// InvalidateEdgeTypeCacheForType removes only the specified edge type from cache.
// Much faster than full invalidation for single-edge operations.
func (b *BadgerEngine) InvalidateEdgeTypeCacheForType(edgeType string) {
	if edgeType == "" {
		return
	}
	b.edgeTypeCacheMu.Lock()
	b.edgeTypeCacheGen.advance()
	if entry, exists := b.edgeTypeCache[edgeType]; exists {
		entry.edges = nil
		b.edgeTypeCache[edgeType] = entry
	}
	b.edgeTypeCacheMu.Unlock()
}

// cacheLoadEdgeAtVersion is the version-checked form of the edge body's hot
// cache. A later body can never replace the record in a pinned transaction.
func (b *BadgerEngine) cacheLoadEdgeAtVersion(id EdgeID, version uint64) (*Edge, bool) {
	b.edgeCacheMu.RLock()
	defer b.edgeCacheMu.RUnlock()
	edge, found := b.edgeCache[id]
	return edge.Edge, found && edge.Edge != nil && edge.coherence.admits(edge.readTs, 0, b.edgeCoherence) && b.edgeCacheVersions[id] == version
}
