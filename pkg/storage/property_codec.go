package storage

import (
	"bytes"
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
)

// Storage version constants. The current binary writes only V2 bodies
// once the data directory is upgraded; the upgrade is gated behind
// --upgrade-storage and runs the eager rewrite migration. Old V1 stores
// are refused at engine-open time unless the upgrade flag is passed.
const (
	storageVersionV0            = 0
	storageVersionV1            = 1
	storageVersionPropKeyDictV2 = 2

	storageVersionCurrent = storageVersionPropKeyDictV2
)

// Format-byte tokens for tokenized bodies. Both are reserved so they
// never collide with the leading byte of any pre-existing legacy body.
//
// Nodes had no leading format byte before V2; V2 nodes start with
// nodeFormatTokenizedV1 (0x10) followed by the tokenized properties
// payload, then the standard Node msgpack body (with Properties cleared).
//
// Edges: edgeFormatCompactV1 (0x02) is the legacy compact codec; V2
// adds edgeFormatCompactV2 (0x03) with a map[uint64]any properties
// payload in place of the V1 map[string]any.
const (
	nodeFormatTokenizedV1 byte = 0x10
	edgeFormatCompactV2   byte = 0x03
)

// Tokenized property payload layout:
//
//	varint(count)
//	repeated count times:
//	  varint(id)        -- dictionary ID for the key name
//	  msgpack(value)    -- the property value, preserving its msgpack type
//
// Hand-rolled rather than `map[uint64]any` because msgpack-go encodes
// uint64 map keys as full 9-byte forms regardless of value, wiping
// out the savings from tokenization. UseCompactInts would fix the
// keys but also narrow integer VALUES (e.g. int64(30) → int8(30)),
// changing user-observable types. The varint+msgpack(value) layout
// gives compact keys without touching value encoding.

// encodeTokenizedProperties serializes a property map under the layout
// above, allocating dictionary IDs inside the caller's txn so the
// dict writes commit atomically with the body that references them.
func (b *BadgerEngine) encodeTokenizedProperties(txn *badger.Txn, namespace string, props map[string]any) ([]byte, error) {
	var buf bytes.Buffer
	scratch := make([]byte, 10)
	n := putUvarint(scratch, uint64(len(props)))
	buf.Write(scratch[:n])
	for name, val := range props {
		id, err := b.propKeyDict.resolveOrAllocateInTxn(txn, namespace, name)
		if err != nil {
			return nil, fmt.Errorf("allocating property key id for %q: %w", name, err)
		}
		n := putUvarint(scratch, id)
		buf.Write(scratch[:n])
		valBytes, err := msgpack.Marshal(val)
		if err != nil {
			return nil, fmt.Errorf("marshaling property %q value: %w", name, err)
		}
		buf.Write(valBytes)
	}
	return buf.Bytes(), nil
}

// decodeTokenizedProperties reverses encodeTokenizedProperties using
// the dictionary's in-memory reverse map. Returns an empty map (not
// nil) when the payload is absent so callers can range over the result
// without nil-checking.
//
// The decoder pairs a homegrown varint reader for the IDs with a
// streaming msgpack decoder for the values. We keep the byte-level
// position tracking explicit because we need to alternate between the
// two encodings without re-creating the decoder per pair.
func (b *BadgerEngine) decodeTokenizedProperties(namespace string, data []byte) (map[string]any, error) {
	if len(data) == 0 {
		return map[string]any{}, nil
	}
	count, consumed, err := readUvarint(data)
	if err != nil {
		return nil, fmt.Errorf("decoding tokenized properties: count varint: %w", err)
	}
	rest := data[consumed:]

	reader := bytes.NewReader(rest)
	dec := msgpack.NewDecoder(reader)
	out := make(map[string]any, count)
	for i := uint64(0); i < count; i++ {
		// reader.Len() tells us how many bytes the msgpack decoder has
		// not yet consumed. We slice rest from the head by the same
		// number to reach the next byte of unread input.
		consumedSoFar := len(rest) - reader.Len()
		if consumedSoFar >= len(rest) {
			return nil, fmt.Errorf("decoding tokenized properties: ran out of bytes after %d/%d entries", i, count)
		}
		id, n, err := readUvarint(rest[consumedSoFar:])
		if err != nil {
			return nil, fmt.Errorf("decoding tokenized properties: key %d id varint: %w", i, err)
		}
		// Advance the reader past the varint. bytes.Reader.Read is
		// infallible once we know n bytes are available (verified by
		// readUvarint succeeding), so we ignore the err return.
		_, _ = reader.Read(make([]byte, n))
		name, ok := b.propKeyDict.lookup(namespace, id)
		if !ok {
			return nil, fmt.Errorf("property key id %d not in dictionary for namespace %q", id, namespace)
		}
		var val any
		if err := dec.Decode(&val); err != nil {
			return nil, fmt.Errorf("decoding tokenized properties: key %q value: %w", name, err)
		}
		out[name] = val
	}
	if reader.Len() != 0 {
		return nil, fmt.Errorf("decoding tokenized properties: %d trailing bytes", reader.Len())
	}
	return out, nil
}

// putUvarint and readUvarint mirror encoding/binary's helpers but
// kept package-local to avoid an extra import on hot paths.
func putUvarint(buf []byte, v uint64) int {
	i := 0
	for v >= 0x80 {
		buf[i] = byte(v) | 0x80
		v >>= 7
		i++
	}
	buf[i] = byte(v)
	return i + 1
}

// readUvarint parses a uvarint-encoded uint64 from buf. Returns the
// value, the number of bytes consumed, and an error.
//
// A uint64 is at most 10 bytes uvarint-encoded (9 × 7 = 63 bits in the
// low seven bits of the first nine bytes, plus 1 bit in the 10th).
// We classify failure modes deterministically:
//
//   - "varint truncated"  — fewer than 10 bytes seen and every byte
//     had its continuation bit set.
//   - "varint overflow"   — 10th byte still has its continuation bit
//     set, or its value > 1 (would shift bits past the uint64 boundary).
func readUvarint(buf []byte) (uint64, int, error) {
	var v uint64
	var s uint
	for i, b := range buf {
		if i == 9 {
			if b >= 0x80 || b > 1 {
				return 0, 0, fmt.Errorf("varint overflow")
			}
			return v | uint64(b)<<s, i + 1, nil
		}
		if b < 0x80 {
			return v | uint64(b)<<s, i + 1, nil
		}
		v |= uint64(b&0x7f) << s
		s += 7
	}
	return 0, 0, fmt.Errorf("varint truncated")
}
