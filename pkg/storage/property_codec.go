package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

// Storage version constants. V3 retains V2 body encoding and repairs
// versioned adjacency for edges written by older bulk writers.
const (
	storageVersionV0              = 0
	storageVersionV1              = 1
	storageVersionPropKeyDictV2   = 2
	storageVersionEdgeAdjacencyV3 = 3
	storageVersionCurrent         = storageVersionEdgeAdjacencyV3
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
//	  msgpack(value)    -- the canonical property value
//
// Hand-rolled rather than `map[uint64]any` because msgpack-go encodes
// uint64 map keys as full 9-byte forms regardless of value, wiping
// out the savings from tokenization. The varint+msgpack(value) layout
// gives compact keys while retaining typed arrays and temporal values.
// Scalar integer widths are canonicalized by the shared property owner.

// encodeTokenizedProperties serializes a property map under the layout
// above, allocating dictionary IDs inside the caller's txn so the
// dict writes commit atomically with the body that references them.
func (b *BadgerEngine) encodeTokenizedProperties(txn *badger.Txn, namespace string, props map[string]any) ([]byte, error) {
	var buf bytes.Buffer
	scratch := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(scratch, uint64(len(props)))
	buf.Write(scratch[:n])
	for name, val := range props {
		id, err := b.propKeyDict.resolveOrAllocateInTxn(txn, namespace, name)
		if err != nil {
			return nil, fmt.Errorf("allocating property key id for %q: %w", name, err)
		}
		n := binary.PutUvarint(scratch, id)
		buf.Write(scratch[:n])
		valBytes, err := msgpack.Marshal(encodeStoredTemporalValue(val))
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
	return b.decodeTokenizedPropertiesProjected(namespace, data, nil)
}

func propertyProjectionSet(properties []string) map[string]struct{} {
	if properties == nil {
		return nil
	}
	out := make(map[string]struct{}, len(properties))
	for _, property := range properties {
		out[property] = struct{}{}
	}
	return out
}

func (b *BadgerEngine) decodeTokenizedPropertiesProjected(namespace string, data []byte, include map[string]struct{}) (map[string]any, error) {
	if len(data) == 0 {
		return map[string]any{}, nil
	}
	count, consumed, err := readUvarint(data)
	if err != nil {
		return nil, fmt.Errorf("decoding tokenized properties: count varint: %w", err)
	}
	rest := data[consumed:]

	reader := bytes.NewReader(rest)
	// Pooled: a scan decodes this once per node, and a new decoder per node
	// was a large part of the per-node cost (#824).
	dec := msgpack.GetDecoder()
	dec.Reset(reader)
	defer msgpack.PutDecoder(dec)
	outSize := int(count)
	if include != nil && len(include) < outSize {
		outSize = len(include)
	}
	out := make(map[string]any, outSize)
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
		// Advance the reader past the varint without an allocation.
		// readUvarint already verified n bytes are available, so the
		// seek can't move past EOF.
		if _, err := reader.Seek(int64(n), io.SeekCurrent); err != nil {
			return nil, fmt.Errorf("decoding tokenized properties: advance past key %d: %w", i, err)
		}
		name, ok := b.propKeyDict.lookup(namespace, id)
		if !ok {
			return nil, fmt.Errorf("property key id %d not in dictionary for namespace %q", id, namespace)
		}
		if include != nil {
			if _, wanted := include[name]; !wanted {
				if err := dec.Skip(); err != nil {
					return nil, fmt.Errorf("decoding tokenized properties: skip key %q value: %w", name, err)
				}
				continue
			}
		}
		val, err := decodeStrictTypedValue(dec)
		if err != nil {
			return nil, fmt.Errorf("decoding tokenized properties: key %q value: %w", name, err)
		}
		out[name] = restoreStoredTemporalValue(val)
	}
	if reader.Len() != 0 {
		return nil, fmt.Errorf("decoding tokenized properties: %d trailing bytes", reader.Len())
	}
	return out, nil
}

func encodeStoredTemporalValue(value any) any {
	switch value := value.(type) {
	case time.Time:
		return storedTime{value: value}
	case []interface{}:
		result := make([]interface{}, len(value))
		for index, item := range value {
			result[index] = encodeStoredTemporalValue(item)
		}
		return result
	case map[string]interface{}:
		result := make(map[string]interface{}, len(value))
		for key, item := range value {
			result[key] = encodeStoredTemporalValue(item)
		}
		return result
	default:
		return value
	}
}

func restoreStoredTemporalValue(value any) any {
	switch value := value.(type) {
	case storedTime:
		return value.value
	case []interface{}:
		for index, item := range value {
			value[index] = restoreStoredTemporalValue(item)
		}
		return value
	case map[string]interface{}:
		for key, item := range value {
			value[key] = restoreStoredTemporalValue(item)
		}
		return value
	default:
		return value
	}
}

// decodeStrictTypedValue parses the wire, then applies the same representation
// owner as write entry. The parser deliberately makes no list-type decisions.
func decodeStrictTypedValue(dec *msgpack.Decoder) (any, error) {
	value, err := decodePropertyWireValue(dec)
	if err != nil {
		return nil, err
	}
	return canonicalPropertyValue(value), nil
}

func decodePropertyWireValue(dec *msgpack.Decoder) (any, error) {
	code, err := dec.PeekCode()
	if err != nil {
		return nil, err
	}
	if msgpcode.IsFixedArray(code) || code == msgpcode.Array16 || code == msgpcode.Array32 {
		return decodePropertyWireArray(dec)
	}
	if msgpcode.IsFixedMap(code) || code == msgpcode.Map16 || code == msgpcode.Map32 {
		n, err := dec.DecodeMapLen()
		if err != nil {
			return nil, err
		}
		out := make(map[string]any, n)
		for i := 0; i < n; i++ {
			key, err := dec.DecodeString()
			if err != nil {
				return nil, err
			}
			out[key], err = decodePropertyWireValue(dec)
			if err != nil {
				return nil, err
			}
		}
		return out, nil
	}
	return dec.DecodeInterface()
}

func decodePropertyWireArray(dec *msgpack.Decoder) (any, error) {
	n, err := dec.DecodeArrayLen()
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, nil
	}
	out := make([]any, n)
	for i := range out {
		out[i], err = decodePropertyWireValue(dec)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// Kept as a thin decoder entry for callers that have already selected an array.
func decodeStrictTypedArray(dec *msgpack.Decoder) (any, error) {
	value, err := decodePropertyWireArray(dec)
	if err != nil {
		return nil, err
	}
	return canonicalPropertyValue(value), nil
}

// readUvarint wraps binary.Uvarint with diagnostic error messages.
// We keep the wrapper (rather than calling binary.Uvarint directly at
// each call site) so the truncation/overflow cases can return distinct
// errors that property-codec callers can wrap with context — the
// stdlib helper signals both via int return values (0 / negative) and
// produces no message text, which makes the surrounding error chain
// less actionable when bodies on disk are corrupt.
func readUvarint(buf []byte) (uint64, int, error) {
	v, n := binary.Uvarint(buf)
	switch {
	case n > 0:
		return v, n, nil
	case n == 0:
		return 0, 0, fmt.Errorf("varint truncated")
	default:
		return 0, 0, fmt.Errorf("varint overflow")
	}
}
