package storage

import (
	"encoding/binary"
	"strings"

	"github.com/dgraph-io/badger/v4"
)

type embeddingReferenceValue struct {
	value   []byte
	deleted bool
}

// embeddingReferenceView resolves ownership in the final state of one
// publication while its physical keys are still being removed. Staged body
// and metadata entries are frozen before cleanup can change the write maps.
// Values already belong to the publication and are not mutated in place.
type embeddingReferenceView struct {
	txn                 *badger.Txn
	writer              *commitWriter
	staged              map[string]embeddingReferenceValue
	replaced            map[NodeID]struct{}
	compatibilityWrites map[string]struct{}
}

func newEmbeddingReferenceView(txn *badger.Txn, writes map[string][]byte, deletes map[string]bool, replacements []NodeID) embeddingReferenceView {
	view := embeddingReferenceView{txn: txn}
	stage := func(key string, value embeddingReferenceValue) {
		if !isEmbeddingReferenceKey(key) {
			return
		}
		if view.staged == nil {
			view.staged = make(map[string]embeddingReferenceValue)
		}
		view.staged[key] = value
	}
	// Replacing a range retires its old metadata even before the physical
	// scan has staged point deletes. Freeze that intent for every owner first;
	// explicit final writes and deletes below retain publication precedence.
	for _, id := range replacements {
		if view.replaced == nil {
			view.replaced = make(map[NodeID]struct{}, len(replacements))
		}
		view.replaced[id] = struct{}{}
		for _, key := range embeddingMetadataKeys(id) {
			stage(string(key), embeddingReferenceValue{deleted: true})
		}
	}
	for key, value := range writes {
		stage(key, embeddingReferenceValue{value: value})
		// Historical captured commits can explicitly republish compatibility
		// bytes. Freeze only their key membership, without copying values or
		// allocating this set for current-family writes.
		if len(view.replaced) != 0 && len(key) != 0 && (key[0] == prefixEmbedding || key[0] == prefixEmbeddingBinary) {
			if view.compatibilityWrites == nil {
				view.compatibilityWrites = make(map[string]struct{})
			}
			view.compatibilityWrites[key] = struct{}{}
		}
	}
	for key, deleted := range deletes {
		if deleted {
			stage(key, embeddingReferenceValue{deleted: true})
			delete(view.compatibilityWrites, key)
		}
	}
	return view
}

func isEmbeddingReferenceKey(key string) bool {
	if len(key) == 0 {
		return false
	}
	if key[0] == prefixNode {
		return true
	}
	// Only the reserved base record can contain sidecar metadata. Avoid
	// decoding ordinary chunk/part keys while freezing the staged view.
	if key[0] == prefixEmbeddingCurrent {
		return strings.HasSuffix(key, "7fffffff")
	}
	if key[0] == prefixEmbedding || key[0] == prefixEmbeddingBinary {
		return strings.HasSuffix(key, "\x7f\xff\xff\xff")
	}
	return false
}

func (view embeddingReferenceView) value(key []byte, visit func([]byte) error) error {
	if pending, ok := view.staged[string(key)]; ok {
		if pending.deleted {
			return badger.ErrKeyNotFound
		}
		return visit(pending.value)
	}
	txn := view.txn
	if view.writer != nil {
		// A large commit can rotate batches between owners. Never retain a
		// closed batch as the reference reader for the next owner.
		txn = view.writer.txn
	}
	item, err := txn.Get(key)
	if err != nil {
		return err
	}
	return item.Value(visit)
}

// embeddingMetadataKeys uses the same compatibility addresses for implicit
// range retirement and reference checks. All keys come from the store codec.
func embeddingMetadataKeys(id NodeID) [][]byte {
	keys := [][]byte{embeddingMetaKey(id)}
	if strings.IndexByte(string(id), 0) >= 0 {
		keys = append(keys, binary.BigEndian.AppendUint32(binaryEmbeddingPrefix(id), uint32(reservedEmbeddingMetaChunkIndex)))
	}
	return append(keys, legacyEmbeddingKey(id, reservedEmbeddingMetaChunkIndex))
}

func (view embeddingReferenceView) ownerReferenced(id NodeID, candidate ...[]byte) (bool, error) {
	var owner *Node
	err := view.value(nodeKey(id), func(value []byte) error {
		var err error
		owner, _, err = decodeNodePayload(value)
		return err
	})
	if err == badger.ErrKeyNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	// A replacement's final body describes its NEW range, not the obsolete
	// compatibility bytes being visited. Only an explicit final point write
	// can reintroduce that exact key. Decode the body first so corrupt owners
	// still fail conservatively. Unqualified owner queries retain the body/
	// metadata contract below, as do untouched owners and current-family keys.
	if _, replaced := view.replaced[id]; replaced && len(candidate) != 0 && len(candidate[0]) != 0 && (candidate[0][0] == prefixEmbedding || candidate[0][0] == prefixEmbeddingBinary) {
		_, written := view.compatibilityWrites[string(candidate[0])]
		return written, nil
	}
	if owner.EmbeddingsStoredSeparately {
		return true, nil
	}
	for _, key := range embeddingMetadataKeys(id) {
		err := view.value(key, func([]byte) error { return nil })
		if err != badger.ErrKeyNotFound {
			return err == nil, err
		}
	}
	return false, nil
}

type embeddingKeyScanOptions struct {
	after      []byte
	references *embeddingReferenceView
}

type embeddingKeyCleanupOptions struct {
	cursor     *[]byte
	references *embeddingReferenceView
}
