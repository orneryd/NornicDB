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
// Label keys use the compact numID format — callers with no numID yet
// get an empty slice (matching "no index entry exists" semantics).
func (b *BadgerEngine) collectNodeIndexKeys(nodeID NodeID, labels []string) [][]byte {
	nodeNum, ok := b.idDict.lookupNodeNumID(nodeID)
	if !ok {
		return nil
	}
	keys := make([][]byte, 0, len(labels))
	for _, label := range labels {
		keys = append(keys, labelIndexKey(label, nodeNum))
	}
	return keys
}

// collectEdgeIndexKeys returns all secondary-index keys written for an edge.
// All index keys use 8-byte numeric IDs from the engine's id dictionary.
// Missing numID entries are skipped — they indicate the edge never made
// it into the corresponding index.
func (b *BadgerEngine) collectEdgeIndexKeys(edgeID EdgeID, startNode NodeID, endNode NodeID, edgeType string) [][]byte {
	var keys [][]byte
	startNum, sOK := b.idDict.lookupNodeNumID(startNode)
	endNum, eOK := b.idDict.lookupNodeNumID(endNode)
	edgeNum, edgeOK := b.idDict.lookupEdgeNumID(edgeID)
	if sOK && edgeOK {
		keys = append(keys, outgoingIndexKey(startNum, edgeNum))
	}
	if eOK && edgeOK {
		keys = append(keys, incomingIndexKey(endNum, edgeNum))
	}
	if edgeOK {
		keys = append(keys, edgeTypeIndexKey(edgeType, edgeNum))
	}
	if sOK && eOK && edgeOK {
		keys = append(keys, edgeBetweenIndexKey(startNum, endNum, edgeType, edgeNum))
	}
	return keys
}
