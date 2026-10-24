package storage

// commitCacheImages is the final per-key portion of commitStates. MVCC and
// cache publication retain the same coalesced images, not operation callbacks.
// The first maps are borrowed; replay merges later statements into them.
type commitCacheImages struct {
	finalNodes map[NodeID]*Node
	finalEdges map[EdgeID]*Edge
}

func (images *commitCacheImages) absorb(next commitCacheImages) {
	if images.finalNodes == nil {
		images.finalNodes = next.finalNodes
	} else {
		for id, node := range next.finalNodes {
			images.finalNodes[id] = node
		}
	}
	if images.finalEdges == nil {
		images.finalEdges = next.finalEdges
	} else {
		for id, edge := range next.finalEdges {
			images.finalEdges[id] = edge
		}
	}
}

// Publish once after the whole logical commit is visible, before any count,
// index or listener callback. The timestamp comes from this exact commit.
func (b *BadgerEngine) publishCommitCacheImages(scope nodeCacheWriteScope, images commitCacheImages, ts uint64, token []uint64) {
	var node *Node
	if scope.cacheNode != nil {
		node = *scope.cacheNode
	}
	if scope.cacheRelationships && scope.relationships != nil {
		if images.finalEdges == nil {
			images.finalEdges = make(map[EdgeID]*Edge)
		}
		scope.relationships(func(edge *Edge) { images.finalEdges[edge.ID] = edge })
	}
	if node == nil && scope.cacheEdge == nil && len(images.finalNodes) == 0 && len(images.finalEdges) == 0 {
		b.invalidateCommitNodeBodies(scope, images)
		return
	}
	b.nodeCacheMu.Lock()
	if len(token) != 1 || token[0] == 0 || token[0] != b.nodeCacheGen.current() {
		b.nodeCacheMu.Unlock()
		b.invalidateCommitNodeBodies(scope, images)
		return
	}
	b.nodeCacheGen.advance()
	token[0] = b.nodeCacheGen.current()
	if node != nil && !b.shouldIndexPendingEmbed(node) {
		b.putNodeLocked(node, ts)
	}
	for id, final := range images.finalNodes {
		if final != nil && !b.shouldIndexPendingEmbed(final) {
			b.putNodeLocked(final, ts)
		} else {
			delete(b.nodeCache, id)
			delete(b.queryNodeCache, id)
		}
	}
	if scope.cacheEdge != nil || len(images.finalEdges) != 0 {
		b.edgeCacheMu.Lock()
		b.edgeCacheGen.advance()
		if scope.cacheEdge != nil {
			b.putEdgeLocked(scope.cacheEdge, 0, ts)
		}
		for id, final := range images.finalEdges {
			if final != nil {
				b.putEdgeLocked(final, 0, ts)
			} else {
				b.discardEdgePayloadLocked(id)
			}
		}
		b.edgeCacheMu.Unlock()
	}
	b.nodeCacheMu.Unlock()
	b.invalidateCommitNodeBodies(scope, images)
}

// Body-cache entries belong to physical item versions. Keep their existing
// write/delete cleanup at this same commit boundary, outside payload locks.
func (b *BadgerEngine) invalidateCommitNodeBodies(scope nodeCacheWriteScope, images commitCacheImages) {
	if scope.nodeID != "" {
		b.cacheDeleteNodeBody(scope.nodeID)
	}
	for id := range images.finalNodes {
		if id != scope.nodeID {
			b.cacheDeleteNodeBody(id)
		}
	}
}
