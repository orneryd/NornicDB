package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/dgraph-io/badger/v4"
)

// dropDerivedPrefixes clears derived index keys before they are rebuilt.
func (b *BadgerEngine) dropDerivedPrefixes(prefixes ...byte) error {
	keys := make([][]byte, len(prefixes))
	for i, prefix := range prefixes {
		keys[i] = []byte{prefix}
	}
	return recoverBadgerClosedPanic(func() error {
		if err := b.ensureOpen(); err != nil {
			return err
		}
		return b.db.DropPrefix(keys...)
	})
}

// storedRecordScan describes one index rebuild's scan of stored records.
type storedRecordScan struct {
	prefix    byte // node or edge bodies
	keyPrefix []byte
	batchSize int          // writes per read-write transaction
	logEvery  int          // processed records between progress logs
	log       *slog.Logger // progress log (nil: none)
	message   string       // progress log message
	unit      string       // progress log attribute (nodes, edges)
}

// forEachStoredRecordInChunks visits every key under scan.prefix for an index
// rebuild. It runs read-write transactions that commit once visit has made
// about scan.batchSize writes, and resumes from the next key, so a rebuild's
// dictionary allocations and index writes commit together in bounded
// batches. visit returns how many writes it made and whether the record
// counts as processed; the scan returns the processed count and logs it
// every scan.logEvery records.
//
// The rebuild runs while the engine serves writes, and every write keeps its
// own index entries. A chunk's transaction tracks the records it reads: one
// that a write committed over in the meantime fails with a conflict, writes
// nothing, and is read again, halved so it finishes ahead of further writes.
func (b *BadgerEngine) forEachStoredRecordInChunks(ctx context.Context, scan storedRecordScan, visit func(txn *badger.Txn, key, value []byte) (writes int, processed bool, err error)) (int, error) {
	prefix := scan.keyPrefix
	if len(prefix) == 0 {
		prefix = []byte{scan.prefix}
	}
	processed := 0
	batchSize := scan.batchSize
	var cursor []byte
	for {
		if err := ctx.Err(); err != nil {
			return processed, err
		}
		counted := 0
		var next []byte
		err := b.withUpdate(func(txn *badger.Txn) error {
			it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
			defer it.Close()
			start := cursor
			if len(start) == 0 {
				start = prefix
			}
			writes := 0
			for it.Seek(start); it.ValidForPrefix(prefix) && writes < batchSize; it.Next() {
				item := it.Item()
				key := item.KeyCopy(nil)
				if err := item.Value(func(value []byte) error {
					n, ok, err := visit(txn, key, value)
					writes += n
					if ok {
						counted++
					}
					return err
				}); err != nil {
					return err
				}
			}
			if it.ValidForPrefix(prefix) {
				next = it.Item().KeyCopy(nil)
			}
			maintenancePause(pauseIndexBackfillChunk)
			return nil
		})
		if errors.Is(err, badger.ErrConflict) {
			batchSize = max(1, batchSize/2)
			continue
		}
		if err != nil {
			return processed, err
		}
		batchSize = scan.batchSize
		if scan.log != nil && scan.logEvery > 0 && (processed+counted)/scan.logEvery > processed/scan.logEvery {
			scan.log.Info(scan.message, scan.unit, processed+counted)
		}
		processed += counted
		if next == nil {
			return processed, nil
		}
		cursor = next
	}
}

// edgeTypeIndexRebuildBatchSize bounds the writes of one rebuild transaction.
const edgeTypeIndexRebuildBatchSize = 5000

// rebuildEdgeTypeIndex drops the relationship-type index (prefix 0x06) and
// writes it again from the stored edges. The V2-to-V3 upgrade uses it: older
// stores keyed the index by the lower-cased type (#862).
func (b *BadgerEngine) rebuildEdgeTypeIndex(ctx context.Context) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := b.dropDerivedPrefixes(prefixEdgeTypeIndex); err != nil {
		return 0, fmt.Errorf("clear edge-type index before rebuild: %w", err)
	}
	scan := storedRecordScan{prefix: prefixEdge, batchSize: edgeTypeIndexRebuildBatchSize}
	return b.forEachStoredRecordInChunks(ctx, scan, func(txn *badger.Txn, key, value []byte) (int, bool, error) {
		if len(key) <= 1 {
			return 0, false, nil
		}
		edgeID := EdgeID(key[1:])
		edge, err := b.decodeEdgeBodyByID(value, edgeID)
		if err != nil {
			return 0, false, fmt.Errorf("decode edge for edge-type index: %w", err)
		}
		if edge.Type == "" {
			return 0, true, nil
		}
		typeKey, err := b.edgeTypeIndexKeyString(txn, edge.Type, edgeID)
		if err == nil {
			err = txn.Set(typeKey, []byte{})
		}
		if err != nil {
			return 0, false, fmt.Errorf("write edge-type index for %q: %w", edgeID, err)
		}
		return 1, true, nil
	})
}
