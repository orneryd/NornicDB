package storage

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
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
	return accessMetaKey(entityID)
}

func (b *BadgerEngine) PutIndexEntryCatalog(entityID string, cat *IndexEntryCatalog) error {
	return b.withUpdate(func(txn *badger.Txn) error {
		return putIndexEntryCatalogInTxn(txn, entityID, cat)
	})
}

func putIndexEntryCatalogInTxn(txn *badger.Txn, entityID string, cat *IndexEntryCatalog) error {
	entry, err := getAccessMetaInTxn(txn, entityID)
	if err != nil {
		return err
	}
	if entry == nil {
		entry = &knowledgepolicy.AccessMetaEntry{TargetID: cat.TargetID, TargetScope: knowledgepolicy.ScopeType(cat.TargetScope)}
	}
	entry.IndexKeys = cat.IndexKeys
	entry.HasIndexKeys = true
	entry.Deindexed = cat.Deindexed
	return putAccessMetaInTxn(txn, entityID, entry)
}

func (b *BadgerEngine) GetIndexEntryCatalog(entityID string) (*IndexEntryCatalog, error) {
	var entry *knowledgepolicy.AccessMetaEntry
	err := b.withView(func(txn *badger.Txn) error {
		var err error
		entry, err = getAccessMetaInTxn(txn, entityID)
		return err
	})
	if err != nil {
		return nil, err
	}
	if entry == nil || (!entry.HasIndexKeys && entry.IndexKeys == nil) {
		return nil, nil
	}
	return &IndexEntryCatalog{TargetID: entry.TargetID, TargetScope: string(entry.TargetScope), IndexKeys: entry.IndexKeys, Deindexed: entry.Deindexed}, nil
}

func (b *BadgerEngine) DeleteIndexEntryCatalog(entityID string) error {
	return b.withUpdate(func(txn *badger.Txn) error {
		return deleteIndexEntryCatalogInTxn(txn, entityID)
	})
}

func deleteIndexEntryCatalogInTxn(txn *badger.Txn, entityID string) error {
	entry, err := getAccessMetaInTxn(txn, entityID)
	if err != nil || entry == nil {
		return err
	}
	entry.IndexKeys = nil
	entry.HasIndexKeys = false
	entry.Deindexed = false
	return putAccessMetaInTxn(txn, entityID, entry)
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
