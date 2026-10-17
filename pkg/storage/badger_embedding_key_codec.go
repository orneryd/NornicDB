package storage

import (
	"bytes"
	"encoding/binary"
	"strconv"
	"strings"

	"github.com/dgraph-io/badger/v4"
)

// All new writes use one disjoint family. Length/index fields are ASCII hex,
// so valid UTF-8 IDs remain valid UTF-8 physical keys in JSON WAL records.
// Layout: [0x28][8 hex ID-byte-length][raw ID][8 hex chunk index], with
// [0x00][4 hex part index] appended for shards. The length disambiguates
// every ID byte, including NUL, from the fixed suffix. This previously unused
// family does not reassign released 0x08 or unreleased R4 0x27; both remain
// read/cleanup compatibility formats and receive no new embedding writes.
const prefixEmbeddingBinary = byte(0x27)
const prefixEmbeddingCurrent = byte(0x28)

var embeddingKeyFamilies = [...]byte{prefixEmbedding, prefixEmbeddingBinary, prefixEmbeddingCurrent}

func legacyEmbeddingPrefix(id NodeID) []byte {
	key := make([]byte, 1, len(id)+6)
	key[0] = prefixEmbedding
	return append(append(key, []byte(id)...), 0)
}

func legacyEmbeddingKey(id NodeID, index int) []byte {
	return binary.BigEndian.AppendUint32(legacyEmbeddingPrefix(id), uint32(index))
}

type embeddingKeyOwner struct {
	id    NodeID
	index uint32
	part  bool
}

func embeddingKeyOwners(key []byte) []embeddingKeyOwner {
	if len(key) == 0 {
		return nil
	}
	if key[0] == prefixEmbeddingCurrent {
		if len(key) < 17 {
			return nil
		}
		n, err := strconv.ParseUint(string(key[1:9]), 16, 32)
		if err != nil || n > uint64(len(key)-17) {
			return nil
		}
		end := 9 + int(n)
		part := len(key)-end == 13 && key[end+8] == 0
		if len(key)-end != 8 && !part {
			return nil
		}
		index, err := strconv.ParseUint(string(key[end:end+8]), 16, 32)
		if err != nil {
			return nil
		}
		if part {
			if _, err = strconv.ParseUint(string(key[end+9:]), 16, 16); err != nil {
				return nil
			}
		}
		return []embeddingKeyOwner{{NodeID(key[9:end]), uint32(index), part}}
	}
	if key[0] == prefixEmbeddingBinary {
		if len(key) < 9 {
			return nil
		}
		n := uint64(binary.BigEndian.Uint32(key[1:5]))
		if n > uint64(len(key)-9) {
			return nil
		}
		end := 5 + int(n)
		if len(key)-end != 4 && (len(key)-end != 7 || key[end+4] != 0) {
			return nil
		}
		return []embeddingKeyOwner{{NodeID(key[5:end]), binary.BigEndian.Uint32(key[end:]), len(key)-end == 7}}
	}
	if key[0] != prefixEmbedding {
		return nil
	}
	var owners []embeddingKeyOwner
	// Decode from the fixed-width suffix, including all bytes of the ID.
	if len(key) >= 6 && key[len(key)-5] == 0 {
		owners = append(owners, embeddingKeyOwner{NodeID(key[1 : len(key)-5]), binary.BigEndian.Uint32(key[len(key)-4:]), false})
	}
	if len(key) >= 9 && key[len(key)-8] == 0 && key[len(key)-3] == 0 {
		owners = append(owners, embeddingKeyOwner{NodeID(key[1 : len(key)-8]), binary.BigEndian.Uint32(key[len(key)-7:]), true})
	}
	return owners
}

// embeddingKeyBelongsTo prevents a legacy prefix from swallowing another
// accepted ID. Only aliases referenced by another owner are preserved.
func embeddingKeyBelongsTo(txn *badger.Txn, key []byte, id NodeID, references *embeddingReferenceView) (bool, error) {
	// A base key for an ordinary ID has one owner. Keep its cleanup free of
	// candidate allocations and additional owner reads; only legacy binary
	// IDs and shard-part aliases need the compatibility checks below.
	if strings.IndexByte(string(id), 0) < 0 && len(key) == len(id)+6 && key[0] == prefixEmbedding && key[len(key)-5] == 0 && string(key[1:len(key)-5]) == string(id) {
		return true, nil
	}
	owners := embeddingKeyOwners(key)
	match := false
	for _, owner := range owners {
		if owner.id == id {
			match = true
			continue
		}
		view := embeddingReferenceView{txn: txn}
		if references != nil {
			view = *references
		}
		referenced, err := view.ownerReferenced(owner.id, key)
		if err != nil {
			return false, err
		}
		if referenced {
			return false, nil
		}
	}
	return match, nil
}

func embeddingPrefixes(id NodeID) [][]byte {
	prefix := legacyEmbeddingPrefix(id)
	if strings.IndexByte(string(id), 0) < 0 {
		return [][]byte{prefix, embeddingPrefix(id)}
	}
	return [][]byte{prefix, binaryEmbeddingPrefix(id), embeddingPrefix(id)}
}

// A V4 store reads each record with one point read. Compatibility probes run
// only for a store that may hold compatibility keys, when the current key is
// missing; the returned base selects matching parts.
func getEmbeddingItem(txn *badger.Txn, id NodeID, index int, compatibility bool) (*badger.Item, []byte, error) {
	return readEmbeddingRecord(id, index, compatibility, txn.Get)
}

// compatibilityEmbeddingKeys reports whether this store may still hold
// embedding records in a compatibility family. The persisted storage version
// decides it: the V4 migration moves every record to the current family, and
// a fresh store starts at V4.
func (b *BadgerEngine) compatibilityEmbeddingKeys() bool {
	return b.storageVersion < storageVersionEmbeddingKeysV4
}

// readEmbeddingRecord owns compatibility selection for both persisted point
// readers and recovered commit overlays. The returned base also selects shards.
func readEmbeddingRecord[T any](id NodeID, index int, compatibility bool, read func([]byte) (T, error)) (T, []byte, error) {
	key := embeddingKey(id, index)
	item, err := read(key)
	if !compatibility {
		return item, key, err
	}
	if err == badger.ErrKeyNotFound && strings.IndexByte(string(id), 0) >= 0 {
		key = binary.BigEndian.AppendUint32(binaryEmbeddingPrefix(id), uint32(index))
		item, err = read(key)
	}
	if err == badger.ErrKeyNotFound {
		key = legacyEmbeddingKey(id, index)
		item, err = read(key)
	}
	return item, key, err
}

func embeddingOwnerPrefix(id NodeID) []byte {
	key := make([]byte, 1, len(id)+17)
	key[0] = prefixEmbeddingCurrent
	key = appendEmbeddingHex(key, uint32(len(id)), 8)
	return append(key, []byte(id)...)
}

func binaryEmbeddingPrefix(id NodeID) []byte {
	key := binary.BigEndian.AppendUint32([]byte{prefixEmbeddingBinary}, uint32(len(id)))
	return append(key, []byte(id)...)
}

func appendEmbeddingHex(key []byte, value uint32, width int) []byte {
	const digits = "0123456789abcdef"
	for shift := (width - 1) * 4; shift >= 0; shift -= 4 {
		key = append(key, digits[(value>>uint(shift))&15])
	}
	return key
}

// The base key may come from either compatibility reader during hydration.
func embeddingPartKey(base []byte, part int) []byte {
	key := make([]byte, len(base), len(base)+5)
	copy(key, base)
	key = append(key, 0)
	if base[0] == prefixEmbeddingCurrent {
		return appendEmbeddingHex(key, uint32(part), 4)
	}
	return append(key, byte(part>>8), byte(part))
}

func embeddingNamespaceOwnsKey(key, prefix []byte) bool {
	owners := embeddingKeyOwners(key)
	return len(owners) == 1 && bytes.HasPrefix([]byte(owners[0].id), prefix)
}
