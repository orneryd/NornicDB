package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/dgraph-io/badger/v4"
)

// replaceEmbeddingRange plans the final range before visiting its old keys.
// An overwrite already removes the old value atomically; staging a deletion of
// that same key only creates a redundant entry and retained key. The scan still
// observes every old key, including obsolete shard parts and metadata.
func (cw *commitWriter) replaceEmbeddingRange(id NodeID, embeddings [][]float32, metadata map[string]any, options ...embeddingKeyScanOptions) error {
	cw.publishKeys = append(cw.publishKeys, id)
	var small [4]embeddingWriteKV
	writes := small[:0]
	layout := binary.AppendUvarint([]byte{1}, uint64(len(embeddings)))
	keys := 1
	boundManifest := false
	for index, embedding := range embeddings {
		kvs, err := buildEmbeddingChunkWriteKVs(id, index, embedding)
		if err != nil {
			return err
		}
		writes = append(writes, kvs...)
		keys += len(kvs)
		layout = binary.AppendUvarint(layout, uint64(len(kvs)-1))
	}
	if metadata != nil {
		if keys <= maxEmbeddingManifestKeys {
			metadata[embeddingRangeManifestKey] = layout
			boundManifest = true
		} else {
			delete(metadata, embeddingRangeManifestKey)
		}
		data, err := encodeValue(metadata)
		if err == nil && len(data) > maxEmbeddingValueBytes {
			delete(metadata, embeddingRangeManifestKey)
			boundManifest = false
			data, err = encodeValue(metadata)
		}
		if err != nil {
			return fmt.Errorf("failed to encode embedding metadata for node %s: %w", id, err)
		}
		writes = append(writes, embeddingWriteKV{key: embeddingMetaKey(id), val: data})
	}
	// Chunk keys and their parts are produced in byte order; metadata follows
	// the real chunk indexes. Search avoids a scan per key on large ranges.
	var deletes [][]byte
	var retention embeddingRangeRetention
	if len(writes) > 0 {
		retention.contains = func(key []byte) bool {
			index := sort.Search(len(writes), func(i int) bool { return bytes.Compare(writes[i].key, key) >= 0 })
			return index < len(writes) && bytes.Equal(writes[index].key, key)
		}
	}
	if err := visitEmbeddingKeysExcept(cw.txn, id, retention, func(key []byte) error {
		deletes = append(deletes, key)
		return nil
	}, options...); err != nil {
		return err
	}
	// The scan must close before cw.write can rotate the Badger transaction.
	for _, key := range deletes {
		if err := cw.write(func(txn *badger.Txn) error { return txn.Delete(key) }); err != nil {
			return err
		}
	}
	for index, kv := range writes {
		entry := badger.NewEntry(kv.key, kv.val)
		// Metadata is last. Bind only the layout this owner generated in the
		// same atomic replacement; dropped manifests retain UserMeta zero.
		if boundManifest && index == len(writes)-1 {
			entry.WithMeta(embeddingRangeManifestBound)
		}
		if err := cw.write(func(txn *badger.Txn) error { return txn.SetEntry(entry) }); err != nil {
			return err
		}
	}
	return nil
}

// replaceEmbeddingSidecarKeys includes metadata in the same replacement plan;
// a nil metadata value removes the previous record with the obsolete keys.
func (cw *commitWriter) replaceEmbeddingSidecarKeys(id NodeID, embeddings [][]float32, metadata map[string]any) error {
	return cw.replaceEmbeddingRange(id, embeddings, metadata)
}
