package storage

import (
	"bytes"
	"hash/maphash"
	"slices"
	"sync"
	"sync/atomic"
)

// What one transaction's embeddingChunkDecodes keeps is bounded whatever the
// node IDs: at most maxChunkDecodes entries, each one stored value of at most
// maxChunkDecodeValueBytes bytes and its decoded vector of at most
// maxChunkDecodeValueBytes bytes (maxChunkDecodeValueBytes/4 values). An entry
// is found by an 8-byte hash of its chunk key; the key itself, which contains
// the node ID, is not kept. A decode that succeeds replaces the entry its key
// hash had with the new value, unless the value or its vector is beyond its
// size bound or the memo holds maxChunkDecodes other entries: then that entry
// is dropped and the value is not kept, so each later read of it decodes it
// again. The memo holds at most 384 MiB of stored values and vectors, plus
// about 1 MiB for the map itself, and about 72 MiB for 8,192 chunks of 1,024
// values (about 5 KiB stored and 4 KiB decoded each).
const (
	maxChunkDecodes          = 8192
	maxChunkDecodeValueBytes = 24 << 10
)

// chunkDecodeSeed hashes chunk keys for every memo of this process.
var chunkDecodeSeed = maphash.MakeSeed()

// embeddingChunkDecodes remembers the vectors one transaction decoded from
// stored chunk values (NOR-412, bug 35). An update's commit can read a
// node's separately stored chunks several times: the predecessor read of its
// write target, then the before and after images of the stored-entity
// captures, at preparation (always for a prepared commit, and for a buffered
// one when its namespace has a stored-entity observer) and at publication
// (when it has one). For a chunk the commit leaves as it is, each of those
// reads returns the same stored bytes.
//
// decode answers with a copy of an earlier vector only for a value
// byte-identical to the one that vector was decoded from. decodeEmbedding is
// a pure function of those bytes, so the copy is exactly the vector a new
// decode returns; every image is still complete and owns its vectors. Two
// chunk keys with the same hash share one entry, so one can displace the
// other; that costs a read at most one decode, never a different answer,
// because an entry answers only for its own bytes.
// A nil *embeddingChunkDecodes decodes every value, as every read did before.
// The zero value is ready to use and allocates nothing until it keeps a
// value, so a commit that reads no chunk allocates nothing for it.
type embeddingChunkDecodes struct {
	mu        sync.Mutex
	byKeyHash map[uint64]decodedEmbeddingChunk
}

// decodeEveryChunkValue, set only by tests, makes every memo decode each
// value as every read did before it, so a test can compare what a commit
// produces with and without it.
var decodeEveryChunkValue atomic.Bool

// chunkDecodeObserved, set only by tests, is told of every value a memo
// decodes (reused false) and of every decode it saves (reused true).
var chunkDecodeObserved atomic.Pointer[func(reused bool)]

func observeChunkDecode(reused bool) {
	if observe := chunkDecodeObserved.Load(); observe != nil {
		(*observe)(reused)
	}
}

type decodedEmbeddingChunk struct {
	value  []byte
	vector []float32
}

// decode returns decodeEmbedding(value) for the chunk value stored under key.
// value must stay unmodified after the call: the memo keeps it.
func (d *embeddingChunkDecodes) decode(key, value []byte) ([]float32, error) {
	if d == nil || decodeEveryChunkValue.Load() {
		return decodeEmbedding(value)
	}
	keyHash := maphash.Bytes(chunkDecodeSeed, key)
	d.mu.Lock()
	entry, found := d.byKeyHash[keyHash]
	d.mu.Unlock()
	if found && bytes.Equal(entry.value, value) {
		observeChunkDecode(true)
		return slices.Clone(entry.vector), nil
	}
	vector, err := decodeEmbedding(value)
	if err != nil {
		return nil, err
	}
	observeChunkDecode(false)
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.byKeyHash, keyHash)
	if len(value) <= maxChunkDecodeValueBytes && len(vector) <= maxChunkDecodeValueBytes/4 && len(d.byKeyHash) < maxChunkDecodes {
		if d.byKeyHash == nil {
			d.byKeyHash = make(map[uint64]decodedEmbeddingChunk)
		}
		d.byKeyHash[keyHash] = decodedEmbeddingChunk{value: value, vector: slices.Clone(vector)}
	}
	return vector, nil
}

// release drops every remembered value. The memo stays usable.
func (d *embeddingChunkDecodes) release() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.byKeyHash = nil
}
