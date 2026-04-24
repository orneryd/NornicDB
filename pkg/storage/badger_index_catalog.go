package storage

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
)

// IndexEntryCatalog tracks the exact secondary-index Badger keys written for
// an entity. The deindex cleanup job uses this to write tombstones without
// scanning the full index keyspace.
type IndexEntryCatalog struct {
	TargetID    string   `msgpack:"targetId"`
	TargetScope string   `msgpack:"targetScope"`
	IndexKeys   [][]byte `msgpack:"indexKeys"`
	Deindexed   bool     `msgpack:"deindexed,omitempty"`
}

func indexEntryCatalogKey(entityID string) []byte {
	return append([]byte{prefixIndexEntryCatalog}, []byte(entityID)...)
}

func (b *BadgerEngine) PutIndexEntryCatalog(entityID string, cat *IndexEntryCatalog) error {
	return b.withUpdate(func(txn *badger.Txn) error {
		return putIndexEntryCatalogInTxn(txn, entityID, cat)
	})
}

func putIndexEntryCatalogInTxn(txn *badger.Txn, entityID string, cat *IndexEntryCatalog) error {
	data, err := msgpack.Marshal(cat)
	if err != nil {
		return err
	}
	return txn.Set(indexEntryCatalogKey(entityID), data)
}

func (b *BadgerEngine) GetIndexEntryCatalog(entityID string) (*IndexEntryCatalog, error) {
	var cat IndexEntryCatalog
	found := false
	err := b.withView(func(txn *badger.Txn) error {
		item, err := txn.Get(indexEntryCatalogKey(entityID))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			found = true
			return msgpack.Unmarshal(val, &cat)
		})
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return &cat, nil
}

func (b *BadgerEngine) DeleteIndexEntryCatalog(entityID string) error {
	return b.withUpdate(func(txn *badger.Txn) error {
		return txn.Delete(indexEntryCatalogKey(entityID))
	})
}

func deleteIndexEntryCatalogInTxn(txn *badger.Txn, entityID string) error {
	return txn.Delete(indexEntryCatalogKey(entityID))
}

// collectNodeIndexKeys returns all secondary-index keys written for a node.
func collectNodeIndexKeys(nodeID NodeID, labels []string) [][]byte {
	keys := make([][]byte, 0, len(labels))
	for _, label := range labels {
		keys = append(keys, labelIndexKey(label, nodeID))
	}
	return keys
}

// collectEdgeIndexKeys returns all secondary-index keys written for an edge.
func collectEdgeIndexKeys(edgeID EdgeID, startNode NodeID, endNode NodeID, edgeType string) [][]byte {
	return [][]byte{
		outgoingIndexKey(startNode, edgeID),
		incomingIndexKey(endNode, edgeID),
		edgeTypeIndexKey(edgeType, edgeID),
		edgeBetweenIndexKey(startNode, endNode, edgeType, edgeID),
		edgeBetweenHeadKey(startNode, endNode, edgeType),
	}
}
