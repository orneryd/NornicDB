package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
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
// no MVCC version, no body rewrite, no UpdatedAt bump. node carries the
// properties and labels the embedding was computed from; the write lands
// only while the stored node still has them. Returns ErrNotFound when the
// node does not exist and ErrEmbeddingSourceChanged when it changed since it
// was read (#889); either way nothing is written.
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
// The node record (nodeKey, MVCC head, version history) is never written and
// no MVCC version is created. The commit first reads the stored node in its
// first batch, whose reads Badger conflict-checks before anything is written
// (commitWriter): when the node is gone the result is ErrNotFound, and when
// its properties or labels differ from node's the embedding describes old
// content, so the result is ErrEmbeddingSourceChanged and the pending marker
// the business write set stays (#889). A business write committing between
// that read and this commit fails the commit with a conflict, reported as
// ErrEmbeddingSourceChanged too.
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

	// One atomic engine write (#703): check the stored node, then delete the
	// previous chunk vectors and metadata record, write the new chunk
	// vectors, write or delete the metadata record at its reserved chunk
	// index, and remove the pending-embeddings marker. Readers see the old
	// embedding state or the new one — never a mix.
	var units []func(txn *badger.Txn) error
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
	err = b.commitEngineWrite(func(cw *commitWriter) error {
		var previous [][]byte
		if err := cw.writeOnce(func(txn *badger.Txn) error {
			if err := b.checkEmbeddingSourceInTxn(txn, node); err != nil {
				return err
			}
			it := txn.NewIterator(badgerPrefixIteratorOptions(embeddingPrefix(node.ID)))
			defer it.Close()
			for it.Rewind(); it.ValidForPrefix(embeddingPrefix(node.ID)); it.Next() {
				previous = append(previous, it.Item().KeyCopy(nil))
			}
			if hook := embeddingSourceCheckedHook.Load(); hook != nil {
				(*hook)()
			}
			return nil
		}); err != nil {
			return err
		}
		for _, key := range previous {
			key := key
			if err := cw.write(func(txn *badger.Txn) error { return txn.Delete(key) }); err != nil {
				return localizedError(localization.StorageClientNodeEmbeddingChunksDeleteFailed(err), err)
			}
		}
		for _, unit := range units {
			if err := cw.write(unit); err != nil {
				return err
			}
		}
		return nil
	})
	if errors.Is(err, badger.ErrConflict) {
		return ErrEmbeddingSourceChanged
	}
	if err != nil {
		return err
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

// embeddingSourceCheckedHook, when set, runs in UpdateNodeEmbeddingSidecar
// after the stored node was checked and before the commit, so storage tests
// can land a business write in between (#889).
var embeddingSourceCheckedHook atomic.Pointer[func()]

// checkEmbeddingSourceInTxn reads the stored node in txn and reports
// ErrNotFound when it is gone and ErrEmbeddingSourceChanged when its
// properties or labels differ from embedded's, the copy the embedding was
// computed from (#889). The read is conflict-tracked by txn.
func (b *BadgerEngine) checkEmbeddingSourceInTxn(txn *badger.Txn, embedded *Node) error {
	item, err := txn.Get(nodeKey(embedded.ID))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	var stored *Node
	if err := item.Value(func(value []byte) error {
		var decodeErr error
		stored, decodeErr = b.decodeNode(namespaceForNodeID(embedded.ID), value)
		return decodeErr
	}); err != nil {
		return err
	}
	if !sameEmbeddingSource(stored, embedded) {
		return ErrEmbeddingSourceChanged
	}
	return nil
}

// sameEmbeddingSource reports whether two copies of a node have the same
// labels (in any order) and the same properties, comparing values as stored
// values rather than by Go type: a copy taken from a cache may hold an int or
// a []string where a decoded one holds an int64 or a []any.
func sameEmbeddingSource(a, b *Node) bool {
	if len(a.Labels) != len(b.Labels) || len(a.Properties) != len(b.Properties) {
		return false
	}
	labels := make(map[string]int, len(a.Labels))
	for _, label := range a.Labels {
		labels[label]++
	}
	for _, label := range b.Labels {
		if labels[label] == 0 {
			return false
		}
		labels[label]--
	}
	for name, value := range a.Properties {
		other, ok := b.Properties[name]
		if !ok || !sameStoredValue(value, other) {
			return false
		}
	}
	return true
}

// sameStoredValue compares two property values as stored values: numbers by
// value, lists and maps element by element, times as instants.
func sameStoredValue(a, b any) bool {
	if x, ok := numericConstraintValue(a); ok {
		y, ok := numericConstraintValue(b)
		return ok && x == y
	}
	if x, ok := a.(time.Time); ok {
		y, ok := b.(time.Time)
		return ok && x.Equal(y)
	}
	va, vb := reflect.ValueOf(a), reflect.ValueOf(b)
	if !va.IsValid() || !vb.IsValid() {
		return !va.IsValid() && !vb.IsValid()
	}
	list := func(v reflect.Value) bool { return v.Kind() == reflect.Slice || v.Kind() == reflect.Array }
	switch {
	case list(va) && list(vb):
		if va.Len() != vb.Len() {
			return false
		}
		for i := 0; i < va.Len(); i++ {
			if !sameStoredValue(va.Index(i).Interface(), vb.Index(i).Interface()) {
				return false
			}
		}
		return true
	case va.Kind() == reflect.Map && vb.Kind() == reflect.Map:
		if va.Len() != vb.Len() {
			return false
		}
		for _, key := range va.MapKeys() {
			other := vb.MapIndex(key)
			if !other.IsValid() || !sameStoredValue(va.MapIndex(key).Interface(), other.Interface()) {
				return false
			}
		}
		return true
	}
	return reflect.DeepEqual(a, b)
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
