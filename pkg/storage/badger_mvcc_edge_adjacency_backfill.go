package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

func (b *BadgerEngine) migrateV2ToV3() error {
	start := []byte{prefixEdge}
	for {
		edges, lastScanned, reachedEnd, err := b.collectEdgeBootstrapBatch(context.Background(), start, mvccRebuildScanBatchSize)
		if err != nil {
			return fmt.Errorf("scan edges: %w", err)
		}
		if err := b.withUpdate(func(txn *badger.Txn) error {
			for _, edge := range edges {
				head, err := b.loadEdgeMVCCHeadInTxn(txn, edge.ID)
				if err == ErrNotFound {
					continue
				}
				if err != nil {
					return err
				}
				if head.Tombstoned {
					continue
				}
				if err := b.ensureEdgeAdjacencyAtHeadInTxn(txn, edge, head.Version); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return fmt.Errorf("repair edge adjacency: %w", err)
		}
		if reachedEnd {
			break
		}
		start = nextScanStart(lastScanned)
	}
	if err := b.repairArchivedEdgeAdjacency(); err != nil {
		return fmt.Errorf("repair archived edge adjacency: %w", err)
	}
	if err := b.rebuildCaseSensitiveIndexes(); err != nil {
		return err
	}
	return b.writeSchemaVersion(storageVersionEdgeAdjacencyV3)
}

type archivedEdgeAdjacency struct {
	edge    *Edge
	version MVCCVersion
}

func (b *BadgerEngine) repairArchivedEdgeAdjacency() error {
	start := []byte{prefixMVCCEdge}
	for {
		batch := make([]archivedEdgeAdjacency, 0, mvccRebuildScanBatchSize)
		var lastKey []byte
		reachedEnd := true
		err := b.withView(func(txn *badger.Txn) error {
			options := badgerIteratorOptions()
			options.Prefix = []byte{prefixMVCCEdge}
			iterator := txn.NewIterator(options)
			defer iterator.Close()
			for iterator.Seek(start); iterator.ValidForPrefix(options.Prefix); iterator.Next() {
				key := iterator.Item().Key()
				// Legacy variable-length keys (string edgeID + 0x00 + version)
				// and fixed-width V3 keys coexist in stores that predate the
				// rewrite; the repair only needs the commit version.
				version, err := extractEdgeVersionFromVersionKey(key)
				if err != nil {
					return err
				}
				var record mvccEdgeRecord
				if err := iterator.Item().Value(func(value []byte) error {
					var decodeErr error
					record, decodeErr = decodeMVCCEdgeRecord(value)
					return decodeErr
				}); err != nil {
					return err
				}
				// An undo record's metadata carries the endpoints and type the
				// adjacency needs.
				edge := record.Edge
				if record.undo != nil {
					edge = record.undo.Meta
				}
				if !record.Tombstoned && edge != nil {
					batch = append(batch, archivedEdgeAdjacency{edge: edge, version: version})
				}
				lastKey = append(lastKey[:0], key...)
				if len(batch) >= mvccRebuildScanBatchSize {
					reachedEnd = false
					break
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		if len(batch) > 0 {
			if err := b.withUpdate(func(txn *badger.Txn) error {
				for _, record := range batch {
					if err := b.ensureEdgeAdjacencyAtHeadInTxn(txn, record.edge, record.version); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				return err
			}
		}
		if reachedEnd {
			return nil
		}
		start = nextScanStart(lastKey)
	}
}

func (b *BadgerEngine) ensureEdgeAdjacencyAtHeadInTxn(txn *badger.Txn, edge *Edge, version MVCCVersion) error {
	if edge == nil {
		return nil
	}
	outgoingKey, err := b.mvccOutgoingAdjacencyKeyString(txn, edge.StartNode, edge.ID, version)
	if err != nil {
		return err
	}
	incomingKey, err := b.mvccIncomingAdjacencyKeyString(txn, edge.EndNode, edge.ID, version)
	if err != nil {
		return err
	}
	for _, key := range [][]byte{outgoingKey, incomingKey} {
		if _, err := txn.Get(key); err == nil {
			continue
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		if err := txn.Set(key, encodeMVCCAdjacencyRecord(false)); err != nil {
			return err
		}
	}
	return nil
}
