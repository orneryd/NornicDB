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
	return b.readKeptNodeItemInTxn(txn, item, id, hydrate, nil)
}

// readKeptNodeItemInTxn is readNodeItemInTxn for a scan that filters what it
// reads: keep decides on the body before it is materialized or hydrated, so a
// rejected cached body is never copied. A rejected node returns (nil, nil).
func (b *BadgerEngine) readKeptNodeItemInTxn(txn *badger.Txn, item *badger.Item, id NodeID, hydrate nodeItemHydration, keep func(*Node) bool) (*Node, error) {
	node, hit := b.cacheLoadNodeBody(id, item.Version(), nil, keep)
	if hit && node == nil {
		return nil, nil
	}
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
		b.cacheStoreNodeBody(id, item.Version(), node, txn.ReadTs())
		if keep != nil && !keep(node) {
			return nil, nil
		}
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
func (b *BadgerEngine) readProjectedNodeItemInTxn(txn *badger.Txn, item *badger.Item, id NodeID, properties []string, include map[string]struct{}, keep ...func(*Node) bool) (*Node, error) {
	var keepNode func(*Node) bool
	if len(keep) > 0 {
		keepNode = keep[0]
	}
	if properties == nil {
		return b.readKeptNodeItemInTxn(txn, item, id, nodeItemFullEmbeddings, keepNode)
	}
	if cached, ok := b.cacheLoadNodeBody(id, item.Version(), properties, keepNode); ok {
		return cached, nil
	}
	var node *Node
	err := item.Value(func(value []byte) error {
		var err error
		node, err = b.decodeNodeProjected(namespaceForNodeID(id), value, include)
		return err
	})
	if err == nil && node != nil && keepNode != nil && !keepNode(node) {
		return nil, nil
	}
	return node, err
}
