package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/util"
)

// embeddingContentUpdatedAtKey is the internal sidecar-metadata key that
// records the body UpdatedAt the embedding was generated from. Reads compare
// it to the body's current UpdatedAt: a mismatch means the content changed
// after the embedding was computed, so the sidecar state is stale and must be
// ignored until the worker re-embeds the new content. The key is never
// returned to callers.
const embeddingContentUpdatedAtKey = "__nornic_content_updated_at"

// reservedEmbeddingMetaChunkIndex is the chunk index under which the worker's
// embedding metadata record is stored. It lives in the EXISTING embedding key
// space (prefixEmbedding, 0x08) so no new key prefix or migration arm is
// needed, and every existing cleanup of that prefix (business writes, legacy
// embedding updates, chunk replacement) removes it together with the chunk
// vectors. Real chunk counts never approach 2^31-1, so the index cannot
// collide with a stored vector.
const reservedEmbeddingMetaChunkIndex = int(0x7FFFFFFF)

// embeddingMetaKey creates the key for a node's sidecar embedding metadata
// record: an ordinary embedding-chunk key with the reserved chunk index.
func embeddingMetaKey(nodeID NodeID) []byte {
	return embeddingKey(nodeID, reservedEmbeddingMetaChunkIndex)
}

// embeddingMetaFromKey reports whether a raw embedding-prefix key is the
// metadata record for some node and, when so, the node ID it belongs to.
// Base chunk keys are [prefix][nodeID][0x00][4-byte index]; sharded part keys
// append [0x00][2-byte part], so a metadata record is exactly the base shape
// whose index bytes are the reserved index.
func embeddingMetaFromKey(key []byte) (NodeID, bool) {
	if len(key) < 6 || key[len(key)-5] != 0x00 {
		return "", false
	}
	if int(binary.BigEndian.Uint32(key[len(key)-4:])) != reservedEmbeddingMetaChunkIndex {
		return "", false
	}
	return NodeID(key[1 : len(key)-5]), true
}

// EmbeddingSidecarUpdater writes managed embedding state (chunk vectors and
// embedding metadata) for an existing node WITHOUT touching the node record:
// no MVCC version, no body rewrite, no UpdatedAt bump. A sidecar write can
// therefore never conflict with an incoming business write to the node.
// Returns ErrNotFound when the node does not exist.
type EmbeddingSidecarUpdater interface {
	UpdateNodeEmbeddingSidecar(node *Node) error
}

// EmbeddingFailureStreamer streams parked embedding failures from the
// embedding metadata key space without decoding node bodies.
type EmbeddingFailureStreamer interface {
	StreamParkedEmbeddingFailures(ctx context.Context, visit func(nodeID NodeID, meta map[string]any) error) (int, error)
}

// UpdateNodeEmbeddingSidecar persists only the embedding payload of an
// existing node in the dedicated embedding key space:
//
//   - chunk vectors under embeddingKey (old chunks replaced atomically, #703)
//   - EmbedMeta under embedMetaKey, stamped with the body UpdatedAt the
//     embedding was generated from
//   - the pending-embeddings index entry is removed
//
// The node record (nodeKey, MVCC head, version history) is never written, so
// this update cannot conflict with concurrent business writes and creates no
// MVCC version. The existence check is a plain read; a node deleted between
// the check and the write leaves only orphaned embedding keys, which the
// deletion path removes.
func (b *BadgerEngine) UpdateNodeEmbeddingSidecar(node *Node) error {
	start := time.Now()
	defer b.observeStorageOp(start, b.opDurPut)
	if node == nil {
		return ErrInvalidData
	}
	if node.ID == "" {
		return ErrInvalidID
	}
	if !strings.Contains(string(node.ID), ":") {
		return localizedError(localization.StorageClientNodeIDNamespaceUnprefixed(string(node.ID)), nil)
	}
	if err := b.ensureOpen(); err != nil {
		return err
	}
	if _, err := b.GetNode(node.ID); err != nil {
		return err // ErrNotFound: deleted, do not create orphans
	}

	meta := make(map[string]any, util.SafePreallocSum(len(node.EmbedMeta), 1))
	for key, value := range node.EmbedMeta {
		meta[key] = value
	}
	// No embedding state means the record is DELETED: the content stamp alone
	// is not metadata, and a record holding only the stamp would surface as an
	// empty EmbedMeta on reads.
	writeMeta := len(meta) > 0
	if writeMeta {
		if !node.UpdatedAt.IsZero() {
			meta[embeddingContentUpdatedAtKey] = node.UpdatedAt.UTC().Format(time.RFC3339Nano)
		}
	}
	metaBytes, err := encodeValue(meta)
	if err != nil {
		return fmt.Errorf("failed to encode embedding metadata for node %s: %w", node.ID, err)
	}

	// One atomic engine write (withUpdateUnits, #703): delete the previous
	// chunk vectors and metadata record, write the new chunk vectors, write or
	// delete the metadata record at its reserved chunk index, and remove the
	// pending-embeddings marker. Readers see the old embedding state or the
	// new one — never a mix.
	var units []func(txn *badger.Txn) error
	if err := b.withView(func(txn *badger.Txn) error {
		it := txn.NewIterator(badgerPrefixIteratorOptions(embeddingPrefix(node.ID)))
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(embeddingPrefix(node.ID)); it.Next() {
			key := it.Item().KeyCopy(nil)
			units = append(units, func(txn *badger.Txn) error { return txn.Delete(key) })
		}
		return nil
	}); err != nil {
		return localizedError(localization.StorageClientNodeEmbeddingChunksDeleteFailed(err), err)
	}
	for index, emb := range node.ChunkEmbeddings {
		kvs, err := buildEmbeddingChunkWriteKVs(node.ID, index, emb)
		if err != nil {
			return err
		}
		chunkIndex := index
		for _, kv := range kvs {
			kv := kv
			units = append(units, func(txn *badger.Txn) error {
				if err := txn.Set(kv.key, kv.val); err != nil {
					return localizedError(localization.StorageClientNodeEmbeddingChunkStoreFailed(chunkIndex, err), err)
				}
				return nil
			})
		}
	}
	metaKey := embeddingMetaKey(node.ID)
	if writeMeta {
		units = append(units, func(txn *badger.Txn) error { return txn.Set(metaKey, metaBytes) })
	} else {
		units = append(units, func(txn *badger.Txn) error { return txn.Delete(metaKey) })
	}
	units = append(units, func(txn *badger.Txn) error { return txn.Delete(pendingEmbedKey(node.ID)) })
	if len(units) > 0 {
		if err := b.withUpdateUnits(units); err != nil {
			return err
		}
	}

	// The full-node cache may hold a copy without the new embedding state;
	// drop it so the next read re-hydrates from the sidecar. Body cache and
	// counts are untouched: the node record did not change.
	b.cacheDeleteNode(node.ID)
	// Search indexes and listeners still need the updated node (mutation
	// notification to the embedding queue keeps its own loop guard).
	b.notifyNodeUpdated(node)
	return nil
}

// loadEmbeddingSidecar returns the sidecar metadata for nodeID, or
// (nil, false, nil) when no sidecar exists or the sidecar is stale (the body
// changed after the embedding was generated). Sidecar state takes precedence
// over body-inline embedding state when present and fresh.
func (b *BadgerEngine) loadEmbeddingSidecar(txn *badger.Txn, node *Node, nodeID NodeID) (map[string]any, bool, error) {
	item, err := txn.Get(embeddingMetaKey(nodeID))
	if err == badger.ErrKeyNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("failed to get embedding metadata for node %s: %w", nodeID, err)
	}
	var meta map[string]any
	if err := item.Value(func(val []byte) error {
		return decodeValue(val, &meta)
	}); err != nil {
		return nil, false, fmt.Errorf("failed to decode embedding metadata for node %s: %w", nodeID, err)
	}
	if meta == nil {
		return nil, false, nil
	}
	// msgpack may decode chunk_count as int8/uint16/... — canonicalize it to
	// int so callers observe the same type the legacy cached writeback left
	// in memory.
	if _, present := meta["chunk_count"]; present {
		meta["chunk_count"] = chunkCountFromMeta(meta)
	}
	if contentAt, ok := meta[embeddingContentUpdatedAtKey].(string); ok {
		if node.UpdatedAt.IsZero() || contentAt != node.UpdatedAt.UTC().Format(time.RFC3339Nano) {
			// The body moved on after the embedding was generated; the
			// sidecar describes stale content and must not be served.
			return nil, false, nil
		}
	}
	return meta, true, nil
}

// StreamParkedEmbeddingFailures iterates the embedding key space and visits
// each node whose reserved-chunk-index metadata record marks a permanent
// embedding failure, without decoding any node body.
func (b *BadgerEngine) StreamParkedEmbeddingFailures(ctx context.Context, visit func(nodeID NodeID, meta map[string]any) error) (int, error) {
	if err := b.ensureOpen(); err != nil {
		return 0, err
	}
	count := 0
	err := b.withView(func(txn *badger.Txn) error {
		it := txn.NewIterator(badgerPrefixIteratorOptions([]byte{prefixEmbedding}))
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix([]byte{prefixEmbedding}); it.Next() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			key := it.Item().Key()
			nodeID, ok := embeddingMetaFromKey(key)
			if !ok {
				continue // A chunk vector (or its shard part), not metadata.
			}
			var meta map[string]any
			if err := it.Item().Value(func(val []byte) error {
				return decodeValue(val, &meta)
			}); err != nil {
				continue
			}
			if failed, _ := meta["embedding_failed"].(bool); !failed {
				continue
			}
			count++
			if err := visit(nodeID, meta); err != nil {
				if errors.Is(err, ErrIterationStopped) {
					return ErrIterationStopped
				}
				return err
			}
		}
		return nil
	})
	if err == ErrIterationStopped {
		err = nil
	}
	return count, err
}
