package storage

import "github.com/dgraph-io/badger/v4"

// nodeBodyAccess keeps mutable query results detached. Commit validation may
// borrow an immutable version-matched body only for its immediate comparisons.
type nodeBodyAccess uint8

const (
	nodeBodyDetached nodeBodyAccess = iota
	nodeBodyBorrowed
)

type nodeItemHydration uint8

const (
	nodeItemBodyOnly nodeItemHydration = iota
	nodeItemQueryMetadata
	nodeItemFullEmbeddings
)

// readNodeItemInTxn reuses only a body with this record's physical version.
// Worker embedding state is hydrated from the same transaction, independently
// of the body cache: sidecar writes do not change the node record's version.
func (b *BadgerEngine) readNodeItemInTxn(txn *badger.Txn, item *badger.Item, id NodeID, hydrate nodeItemHydration) (*Node, error) {
	return b.readNodeItemInTxnWithEmbeddingAbsence(txn, item, id, hydrate, false)
}

// embeddingRecordsAbsent is established by the scan's own snapshot, never by
// a current-value cache. Legacy body flags still use normal hydration.
func (b *BadgerEngine) readNodeItemInTxnWithEmbeddingAbsence(txn *badger.Txn, item *badger.Item, id NodeID, hydrate nodeItemHydration, embeddingRecordsAbsent bool) (*Node, error) {
	node, hit := b.cacheLoadNodeBody(id, item.Version(), nil, nodeBodyDetached)
	if !hit {
		err := item.Value(func(value []byte) error {
			var err error
			node, err = b.decodeNodeProjected(txn, namespaceForNodeID(id), value, nil)
			return err
		})
		if err != nil {
			return nil, err
		}
		if node == nil {
			return nil, ErrNotFound
		}
		b.cacheStoreNodeBody(id, item.Version(), node, txn.ReadTs())
	}
	var err error
	switch hydrate {
	case nodeItemBodyOnly:
	case nodeItemQueryMetadata, nodeItemFullEmbeddings:
		if !embeddingRecordsAbsent || node.RuntimeMetadata().EmbeddingsStoredSeparately {
			node, err = b.loadNodeEmbeddingState(embeddingRecordReader{txn: txn}, node, id, hydrate == nodeItemFullEmbeddings)
			if err != nil {
				return nil, err
			}
		}
	default:
		return nil, ErrInvalidData
	}
	b.bindNodeReadSource(node, id, item.Version())
	return node, nil
}

// bindNodeReadSource gives a decoded image the identity of the pinned body
// it came from. Point and scan reads use the same record, independently of
// whether a whole-node cache happened to contain the node. Besides embedding
// writeback provenance, native handlers comparing node images must not see
// different ownership records solely because one read was a scan.
func (b *BadgerEngine) bindNodeReadSource(node *Node, id NodeID, version uint64) {
	if node == nil {
		return
	}
	revision := EmbeddingBodyRevision{physical: version}
	if node.runtime != nil && node.runtime.embeddingOwner == b && node.runtime.embeddingOwnerID == id && node.runtime.embeddingRevision == revision {
		return
	}
	state := node.runtimeState()
	state.embeddingOwner, state.embeddingOwnerID = b, id
	state.embeddingRevision = revision
	node.setRuntimeState(state)
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
func (b *BadgerEngine) readProjectedNodeItemInTxn(txn *badger.Txn, item *badger.Item, id NodeID, properties []string, include map[string]struct{}, access nodeBodyAccess) (*Node, error) {
	if properties == nil {
		return b.readNodeItemInTxn(txn, item, id, nodeItemFullEmbeddings)
	}
	if cached, ok := b.cacheLoadNodeBody(id, item.Version(), properties, access); ok {
		return cached, nil
	}
	var node *Node
	err := item.Value(func(value []byte) error {
		var err error
		node, err = b.decodeNodeProjected(txn, namespaceForNodeID(id), value, include)
		return err
	})
	return node, err
}
