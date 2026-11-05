package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
)

// The replacement owner writes this index with the metadata in the same atomic
// commit as its keys. It records counts, never vectors or caller-supplied keys.
// All keys are reconstructed through the active codec; compatibility ranges
// remain scanned separately. Missing/invalid indexes take the bounded scan.
const embeddingRangeManifestKey = "__nornic_embedding_range_v1"

// This proof belongs to the Badger entry, not its encoded metadata map.
// Older writers copy unknown map fields, but Txn.Set creates a new entry with
// UserMeta zero. A replacement by such a writer therefore invalidates trust
// even when it carries a syntactically valid, now stale manifest forward.
// Only replaceEmbeddingRange sets the proof, with its freshly planned layout.
// Ordinary metadata rewrites (including retained freshness and buffered replay)
// conservatively clear it and take the bounded scan on the next replacement.
const embeddingRangeManifestBound byte = 1

// Larger ranges retain identical storage semantics through the scan. This
// admission bound also limits work from a malformed/restored metadata record.
const maxEmbeddingManifestKeys = 4096

type embeddingRangeManifestRecord struct {
	Layout []byte `msgpack:"__nornic_embedding_range_v1"`
}

// Reuse parsing scratch without caching a decoded node or transaction.
// A borrowed state stays exclusive until its manifest has been consumed.
type embeddingRangeManifestDecoder struct {
	reader   bytes.Reader
	decoder  *msgpack.Decoder
	record   embeddingRangeManifestRecord
	reusable bool
}

var embeddingRangeManifestDecoders = sync.Pool{New: func() any {
	state := new(embeddingRangeManifestDecoder)
	state.decoder = msgpack.NewDecoder(&state.reader)
	return state
}}

func (state *embeddingRangeManifestDecoder) decode(data []byte) error {
	state.record.Layout = state.record.Layout[:0]
	state.reusable = false
	serializer, payload, ok, err := splitSerializationHeader(data)
	if err != nil {
		return err
	}
	if !ok || serializer != serializerIDMsgpack {
		return decodeValue(data, &state.record)
	}
	state.reader.Reset(payload)
	state.decoder.Reset(&state.reader)
	err = state.decoder.Decode(&state.record)
	state.reader.Reset(nil)
	state.reusable = err == nil && len(data) <= 1024
	return err
}

func (state *embeddingRangeManifestDecoder) release() {
	state.reader.Reset(nil)
	// Large/malformed records still decode normally, but cannot grow the pool.
	if state.reusable && cap(state.record.Layout) <= 1024 {
		clear(state.record.Layout)
		state.record.Layout = state.record.Layout[:0]
		embeddingRangeManifestDecoders.Put(state)
	}
}

func validEmbeddingRangeManifest(layout []byte) bool {
	if len(layout) < 2 || layout[0] != 1 {
		return false
	}
	count, size := binary.Uvarint(layout[1:])
	if size <= 0 || count >= maxEmbeddingManifestKeys {
		return false
	}
	remaining := layout[1+size:]
	keys := count + 1 // The metadata key is implicit.
	for range count {
		parts, size := binary.Uvarint(remaining)
		if size <= 0 || parts > maxEmbeddingManifestKeys-keys {
			return false
		}
		keys += parts
		remaining = remaining[size:]
	}
	return len(remaining) == 0
}

// visitEmbeddingManifestKeys uses the transaction's metadata read as the
// conflict guard. Its entry-level proof binds the layout to a replacement by
// the shared range owner; an unknown/old writer's metadata is never trusted.
// Callbacks own generated keys. Old snapshots read their own manifest and
// codecs, just as they read their own vectors. No payload point probes run.
func visitEmbeddingManifestKeys(txn *badger.Txn, id NodeID, retained embeddingRangeRetention, visit func([]byte) error, after []byte) (bool, error) {
	if retained.contains == nil {
		return false, nil
	}
	metaKey := embeddingMetaKey(id)
	item, err := txn.Get(metaKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if item.UserMeta() != embeddingRangeManifestBound {
		return false, nil
	}
	state := embeddingRangeManifestDecoders.Get().(*embeddingRangeManifestDecoder)
	defer state.release()
	if err := item.Value(state.decode); err != nil || !validEmbeddingRangeManifest(state.record.Layout) {
		return false, nil
	}
	count, size := binary.Uvarint(state.record.Layout[1:])
	remaining := state.record.Layout[1+size:]
	accept := func(key []byte) error {
		if bytes.Compare(key, after) >= 0 && !retained.contains(key) {
			return visit(key)
		}
		return nil
	}
	for index := 0; index < int(count); index++ {
		parts, size := binary.Uvarint(remaining)
		remaining = remaining[size:]
		if err := accept(embeddingKey(id, index)); err != nil {
			return true, err
		}
		for part := 0; part < int(parts); part++ {
			if err := accept(embeddingChunkPartKey(id, index, part)); err != nil {
				return true, err
			}
		}
	}
	return true, accept(metaKey)
}
