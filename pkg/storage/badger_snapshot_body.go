package storage

import "github.com/dgraph-io/badger/v4"

// readNodeItemInTxn reuses only a body with this record's physical version.
// Worker embedding state is hydrated from the same transaction, independently
// of the body cache: sidecar writes do not change the node record's version.
func (b *BadgerEngine) readNodeItemInTxn(txn *badger.Txn, item *badger.Item, id NodeID, hydrate func(*badger.Txn, *Node, NodeID) (*Node, error)) (*Node, error) {
	node, hit := b.cacheLoadNodeBody(id, item.Version())
	if !hit {
		err := item.Value(func(value []byte) error {
			var err error
			node, err = b.decodeNode(namespaceForNodeID(id), value)
			return err
		})
		if err != nil {
			return nil, err
		}
		if node == nil {
			return nil, ErrNotFound
		}
		b.cacheStoreNodeBody(id, item.Version(), node)
	}
	if hydrate != nil {
		return hydrate(txn, node, id)
	}
	return node, nil
}

// cacheLoadEdgeAtVersion is the version-checked form of the edge body's hot
// cache. A later body can never replace the record in a pinned transaction.
func (b *BadgerEngine) cacheLoadEdgeAtVersion(id EdgeID, version uint64) (*Edge, bool) {
	b.edgeCacheMu.RLock()
	defer b.edgeCacheMu.RUnlock()
	edge, found := b.edgeCache[id]
	return edge, found && edge != nil && b.edgeCacheVersions[id] == version
}

// readProjectedNodeItemInTxn shares the versioned body decision between point,
// label and MVCC projection readers. A nil projection retains full hydration.
func (b *BadgerEngine) readProjectedNodeItemInTxn(txn *badger.Txn, item *badger.Item, id NodeID, properties []string, include map[string]struct{}) (*Node, error) {
	if properties == nil {
		return b.readNodeItemInTxn(txn, item, id, b.loadNodeEmbeddings)
	}
	if cached, ok := b.cacheLoadNodeBody(id, item.Version()); ok {
		return projectCachedNodeForRead(cached, properties), nil
	}
	var node *Node
	err := item.Value(func(value []byte) error {
		var err error
		node, err = b.decodeNodeProjected(namespaceForNodeID(id), value, include)
		return err
	})
	return node, err
}
