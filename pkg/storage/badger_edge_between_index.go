// Package storage provides storage engine implementations for NornicDB.
package storage

import (
	"context"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

var edgeBetweenIndexReadyKey = []byte{prefixMVCCMeta, prefixMVCCMetaEdgeBetweenIndexReady}

const (
	edgeBetweenIndexRebuildBatchSize = 5_000
	edgeBetweenIndexRebuildLogEvery  = 100_000
)

// rebuildRelationshipIndexes makes label- and relationship-type-keyed data
// exact-case in the V2-to-V3 upgrade (#862). Older binaries lower-cased names in the label
// index, the relationship-type index, the relationship-between set and heads,
// the label and type counts and the temporal index, so :Person and :person
// shared entries and counts; Neo4j's names are case-sensitive. Node and edge
// bodies keep names as written, so every structure is rebuilt from them:
//
//   - the label index (0x03), the relationship-type index (0x06) and the
//     relationship-between set and heads (0x11 / 0x12) are rebuilt here;
//   - AccessMetaStore catalog state gets its entity's current index keys, and a
//     deindexed entity's markers move to those keys, so it stays
//     hidden;
//   - the clean-shutdown marker is removed, so the temporal index and MVCC
//     heads are rebuilt before the database serves traffic
//     (prepareDerivedIndexes);
//   - the label and type counts are rebuilt at open by ensureLabelCounts and
//     ensureEdgeTypeCounts, which compare them with counts taken from the
//     bodies.
//
// The caller writes version 3 only after this rebuild succeeds.
type relationshipIndexRebuild uint8

const (
	rebuildEdgeBetweenOnly relationshipIndexRebuild = iota
	rebuildCaseSensitiveUpgrade
)

// rebuildRelationshipIndexes owns relationship lookup rebuilds and the complete
// case-sensitive storage upgrade, including its ordered preparation and markers.
func (b *BadgerEngine) rebuildRelationshipIndexes(ctx context.Context, selection relationshipIndexRebuild) (processed int, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if selection == rebuildCaseSensitiveUpgrade {
		if err := b.migrateLegacyPolicyMetadata(ctx); err != nil {
			return 0, fmt.Errorf("move legacy policy metadata: %w", err)
		}
		if _, err := b.rebuildLabelIndex(ctx); err != nil {
			return 0, fmt.Errorf("rebuild label index: %w", err)
		}
		if _, err := b.rebuildEdgeTypeIndex(ctx); err != nil {
			return 0, fmt.Errorf("rebuild edge-type index: %w", err)
		}
	}
	edgePhase := true
	defer func() {
		if selection == rebuildCaseSensitiveUpgrade && edgePhase && err != nil {
			err = fmt.Errorf("rebuild edge-between index: %w", err)
		}
	}()
	if err := b.db.DropPrefix(ctx, prefixDropOptions{engine: b}, []byte{prefixEdgeBetweenIndex}, []byte{prefixEdgeBetweenHead}); err != nil {
		return 0, fmt.Errorf("clear edge-between set index before rebuild: %w", err)
	}
	// D-07 single-allocation: pre-bind subsystem attributes once for the
	// rebuild's progress emissions (every edgeBetweenIndexRebuildLogEvery
	// edges) so the steady-state path adds no .With(...) allocations.
	scan := storedRecordScan{
		prefix:    prefixEdge,
		batchSize: edgeBetweenIndexRebuildBatchSize,
		logEvery:  edgeBetweenIndexRebuildLogEvery,
		log:       b.log.With("subsystem", "index_rebuild", "index", "edge_between"),
		message:   "edge-between index backfill progress",
		unit:      "edges",
	}
	processed, err = b.forEachStoredRecordInChunks(ctx, scan, func(txn *badger.Txn, key, val []byte) (int, bool, error) {
		var edgeID EdgeID
		if len(key) > 1 {
			edgeID = EdgeID(key[1:])
		}
		edge, err := b.decodeEdgeBodyByID(val, edgeID)
		if err == nil {
			err = b.writeEdgeBetweenIndexesInTxn(txn, edge)
		}
		if err != nil {
			return 0, false, fmt.Errorf("decode edge for edge-between index: %w", err)
		}
		return 2, true, nil
	})
	if err != nil {
		return processed, err
	}
	if err := ctx.Err(); err != nil {
		return processed, err
	}
	err = b.withMetadataUpdateContext(ctx, func(txn *badger.Txn) error {
		return txn.Set(edgeBetweenIndexReadyKey, []byte{1})
	})
	if err != nil {
		return processed, err
	}
	edgePhase = false
	if selection == rebuildCaseSensitiveUpgrade {
		if err := b.db.DropPrefix(ctx, prefixDropOptions{engine: b}, []byte{0x18}, []byte{0x19}); err != nil {
			return processed, fmt.Errorf("clear displaced relationship indexes: %w", err)
		}
		if err := b.rewriteIndexEntryCatalogs(ctx); err != nil {
			return processed, fmt.Errorf("rewrite index entry catalogs: %w", err)
		}
		err = b.withMetadataUpdateContext(ctx, func(txn *badger.Txn) error { return txn.Delete(cleanShutdownMarkerKey) })
	}
	return processed, err
}
