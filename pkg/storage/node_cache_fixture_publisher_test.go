package storage

// Legacy unit fixtures invoke a committed single-key image, never an operation
// callback. Production exposes only the logical commit-level publisher.
func (b *BadgerEngine) publishCommittedNodeFixture(node *Node, token ...uint64) {
	if node == nil {
		return
	}
	if len(token) == 0 {
		b.cacheDeleteNode(node.ID)
		return
	}
	b.publishCommitCacheImages(nodeBodyCacheWrites(&node, node.ID), commitCacheImages{}, b.nodeCacheCommitTs.Load(), token)
}

// Test fixtures may install artificial payloads; production has no unscoped publisher.
// cacheStoreNode caches node as written (a write: see cacheGeneration).
func (b *BadgerEngine) cacheStoreNode(node *Node) {
	node = nodeForStorageWrite(node)
	if node == nil {
		return
	}
	b.nodeCacheMu.Lock()
	b.nodeCacheGen.advance()
	b.putNodeLocked(node, b.nodeCacheCommitTs.Load())
	// Only the cache copy is modified; buffered ACK bodies remain immutable.
	cached := b.nodeCache[node.ID]
	cached.embeddingRevision = EmbeddingBodyRevision{}
	cached.embeddingAttempt = nil
	b.nodeCacheMu.Unlock()
	b.cacheDeleteNodeBody(node.ID)
}
