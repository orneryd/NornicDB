package storage

import "github.com/dgraph-io/badger/v4"

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
	node, hit := b.cacheLoadNodeBody(id, item.Version(), nil)
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
	switch hydrate {
	case nodeItemBodyOnly:
		return node, nil
	case nodeItemQueryMetadata, nodeItemFullEmbeddings:
		return b.loadNodeEmbeddingState(embeddingRecordReader{txn: txn}, node, id, hydrate == nodeItemFullEmbeddings)
	default:
		return nil, ErrInvalidData
	}
}

// readProjectedNodeItemInTxn shares the versioned body decision between point,
// label and MVCC projection readers. A nil projection retains full hydration.
func (b *BadgerEngine) readProjectedNodeItemInTxn(txn *badger.Txn, item *badger.Item, id NodeID, properties []string, include map[string]struct{}) (*Node, error) {
	if properties == nil {
		return b.readNodeItemInTxn(txn, item, id, nodeItemFullEmbeddings)
	}
	if cached, ok := b.cacheLoadNodeBody(id, item.Version(), properties); ok {
		return cached, nil
	}
	var node *Node
	err := item.Value(func(value []byte) error {
		var err error
		node, err = b.decodeNodeProjected(namespaceForNodeID(id), value, include)
		return err
	})
	return node, err
}
