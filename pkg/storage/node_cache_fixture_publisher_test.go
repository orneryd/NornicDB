package storage

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
	b.nodeCacheMu.Unlock()
	b.cacheDeleteNodeBody(node.ID)
}
