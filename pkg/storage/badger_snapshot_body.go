package storage

import "github.com/dgraph-io/badger/v4"

type nodeItemHydration uint8

const (
	nodeItemBodyOnly nodeItemHydration = iota
	nodeItemQueryMetadata
	nodeItemFullEmbeddings
)

// decayKeep is the decay selection of a read with visibility v, decided on a
// cached body before it is copied.
func (b *BadgerEngine) decayKeep(v readVisibility) func(*Node) bool {
	return b.decayKeepAt(v, DecayScoringTime())
}

// decayKeepAt is decayKeep for a scan that fixed its decay time once: the one
// decay selection every node read hands to the body cache and the node cache
// (NOR-160).
func (b *BadgerEngine) decayKeepAt(v readVisibility, now int64) func(*Node) bool {
	return func(node *Node) bool { return !b.filterEntityByDecay(v, node, now) }
}

// readKeptNodeItemInTxn reads one stored node record, reusing only a body
// cached for this record's physical version. Worker embedding state is
// hydrated from the same transaction, independently of the body cache:
// sidecar writes do not change the node record's version. keep is the read's
// own selection (every caller states it; nil only where nothing is rejected
// afterwards): it decides on the body before it is materialized or hydrated,
// so a rejected cached body is never copied. A rejected node returns
// (nil, nil) (NOR-160).
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
// keep is the read's own selection, as for readKeptNodeItemInTxn.
func (b *BadgerEngine) readProjectedNodeItemInTxn(txn *badger.Txn, item *badger.Item, id NodeID, properties []string, include map[string]struct{}, keepNode func(*Node) bool) (*Node, error) {
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
