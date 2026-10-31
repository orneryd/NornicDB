package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
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
	storageVersionCatalogV4       = 4
	storageVersionCurrent         = storageVersionCatalogV4
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
	scratch := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(scratch, uint64(len(props)))
	buf.Write(scratch[:n])
	if len(props) == 0 {
		return buf.Bytes(), nil
	}
	encoder := msgpack.GetEncoder()
	defer msgpack.PutEncoder(encoder)
	var valueBuf bytes.Buffer
	for name, val := range props {
		id, err := b.propKeyDict.resolveOrAllocateInTxn(txn, namespace, name)
		if err != nil {
			return nil, fmt.Errorf("allocating property key id for %q: %w", name, err)
		}
		n := binary.PutUvarint(scratch, id)
		buf.Write(scratch[:n])
		// Each value gets its own empty writer, as with Marshal: a custom
		// typed-property encoder can use or reset that writer without
		// changing the surrounding tokenized body. Reuse its capacity.
		valueBuf.Reset()
		encoder.Reset(&valueBuf)
		converted := encodeStoredTemporalValue(val)
		var encodeErr error
		if isSimplePropertyEncoding(val) {
			encodeErr = encoder.Encode(converted)
		} else {
			switch val.(type) {
			case []any, map[string]any:
				// A custom value nested in a collection must not share the
				// collection's writer or encoder state with its siblings.
				needsIsolation, err := inspectPropertyEncoding(val, 0, -1)
				if err != nil {
					return nil, fmt.Errorf("marshaling property %q value: %w", name, err)
				}
				if needsIsolation {
					encodeErr = encodeIsolatedPropertyValue(encoder, converted, 0, -1)
				} else {
					encodeErr = encoder.Encode(converted)
				}
			default:
				encodeErr = encoder.Encode(converted)
				if encodeErr == nil {
					if err := validateMessagePackValue(valueBuf.Bytes()); err != nil {
						encodeErr = fmt.Errorf("invalid MessagePack value: %w: %w", ErrInvalidData, err)
					}
				}
			}
		}
		if encodeErr != nil {
			return nil, fmt.Errorf("marshaling property %q value: %w", name, encodeErr)
		}
		buf.Write(valueBuf.Bytes())
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
	// Each entry requires at least a one-byte key token and one value byte.
	// Cap preallocation by the remaining payload, then parse normally so a
	// malformed key or truncated value retains its precise diagnostic.
	outSize := len(rest) / 2
	if count < uint64(outSize) {
		outSize = int(count)
	}

	reader := bytes.NewReader(rest)
	// Pooled: a scan decodes this once per node, and a new decoder per node
	// was a large part of the per-node cost (#824).
	dec := msgpack.GetDecoder()
	dec.Reset(reader)
	defer msgpack.PutDecoder(dec)
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

// encodeStoredTemporalValue wraps every time.Time reachable through
// []interface{} and map[string]interface{} values as storedTime.
func encodeStoredTemporalValue(value any) any {
	converted, _ := storedTemporalValue(value)
	return converted
}

// storedTemporalValue is encodeStoredTemporalValue that also reports whether
// anything was wrapped. A list or map is copied only when something inside it
// changes, so a value without temporal content is returned as it is and the
// input is never mutated. A nil list or map counts as changed and becomes an
// empty one: the encoded form has always been an empty collection, not nil.
func storedTemporalValue(original any) (any, bool) {
	switch value := original.(type) {
	case time.Time:
		return storedTime{value: value}, true
	case []interface{}:
		if value == nil {
			return []interface{}{}, true
		}
		var result []interface{}
		for index, item := range value {
			converted, changed := storedTemporalValue(item)
			if changed && result == nil {
				result = make([]interface{}, len(value))
				copy(result, value[:index])
			}
			if result != nil {
				result[index] = converted
			}
		}
		if result == nil {
			return original, false
		}
		return result, true
	case map[string]interface{}:
		if value == nil {
			return map[string]interface{}{}, true
		}
		var result map[string]interface{}
		for key, item := range value {
			converted, changed := storedTemporalValue(item)
			if !changed {
				continue
			}
			if result == nil {
				result = make(map[string]interface{}, len(value))
				for k, v := range value {
					result[k] = v
				}
			}
			result[key] = converted
		}
		if result == nil {
			return original, false
		}
		return result, true
	default:
		return original, false
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

// decodeStrictTypedValue decodes one msgpack-encoded property value
// while preserving the on-disk type tags. The default `dec.Decode(&any)`
// path widens every msgpack array into []interface{} regardless of
// element homogeneity — even though the values were written as
// []float64 (or []int64 / []string) and the on-disk msgpack tags
// reflect that — which forces every read site downstream to type-coerce
// and silently changes the shape callers wrote.
//
// We instead inspect the msgpack header byte for arrays and decode each
// element into a strict-typed slice (homogeneous arrays only). Mixed
// arrays still fall back to []interface{} so we don't lose information.
//
// Maps inside properties are handled the same way: every value gets
// recursed through this function so a deeply-nested []float64 stays
// []float64.
func decodeStrictTypedValue(dec *msgpack.Decoder) (any, error) {
	code, err := dec.PeekCode()
	if err != nil {
		return nil, err
	}
	if msgpcode.IsFixedArray(code) || code == msgpcode.Array16 || code == msgpcode.Array32 {
		return decodeStrictTypedArray(dec)
	}
	if msgpcode.IsFixedMap(code) || code == msgpcode.Map16 || code == msgpcode.Map32 {
		return decodeStrictTypedMap(dec)
	}
	// Every integer reads as int64, as Cypher integers are, whatever code
	// msgpack stored it in (a Go int takes the narrowest that holds it, and
	// Uint64 above 2^32-1); only a value above MaxInt64 stays uint64.
	if isIntegerCode(code) {
		var signed int64
		overflow, err := decodePropertyInteger(dec, code, &signed)
		if err != nil {
			return nil, err
		}
		if overflow != nil {
			return overflow, nil
		}
		return signed, nil
	}
	// DecodeInterface is the msgpack package's hand-written scalar path.
	// Decode(&any) routes through reflection and dominates large property
	// arrays such as Graphiti embedding vectors.
	return dec.DecodeInterface()
}

// decodeStrictTypedArray decodes a msgpack array. If every element is
// the same kind (float, int, string, bool) we return a strictly-typed
// slice. Mixed-type arrays come back as []interface{}.
func decodeStrictTypedArray(dec *msgpack.Decoder) (any, error) {
	n, err := dec.DecodeArrayLen()
	if err != nil {
		return nil, err
	}
	if err := validateMessagePackCollection(dec, n, 1); err != nil {
		return nil, err
	}
	if n <= 0 {
		return []interface{}{}, nil
	}
	firstCode, err := dec.PeekCode()
	if err != nil {
		return nil, err
	}
	switch {
	case firstCode == msgpcode.Double:
		return decodeFloat64Array(dec, n)
	case firstCode == msgpcode.Float:
		out := make([]float32, n)
		i, err := walkTypedArray(dec, out, func(dec *msgpack.Decoder, slot *float32) error {
			code, err := dec.PeekCode()
			if err != nil {
				return err
			}
			if code != msgpcode.Float {
				return errTypedArrayMismatch
			}
			value, err := dec.DecodeFloat32()
			*slot = value
			return err
		})
		if i == len(out) {
			return out, nil
		}
		return finishMixedArray(dec, out[:i], nil, len(out), err)
	case isIntegerCode(firstCode):
		out := make([]int64, n)
		var alternate any
		i, err := walkTypedArray(dec, out, func(dec *msgpack.Decoder, slot *int64) error {
			code, err := dec.PeekCode()
			if err != nil {
				return err
			}
			if !isIntegerCode(code) {
				return errTypedArrayMismatch
			}
			overflow, err := decodePropertyInteger(dec, code, slot)
			alternate = overflow
			if err != nil {
				return err
			}
			if overflow != nil {
				return errTypedArrayMismatch
			}
			return nil
		})
		if i == len(out) {
			return out, nil
		}
		return finishMixedArray(dec, out[:i], alternate, len(out), err)
	case msgpcode.IsString(firstCode):
		out := make([]string, n)
		i, err := walkTypedArray(dec, out, func(dec *msgpack.Decoder, slot *string) error {
			code, err := dec.PeekCode()
			if err != nil {
				return err
			}
			if !msgpcode.IsString(code) {
				return errTypedArrayMismatch
			}
			value, err := dec.DecodeString()
			*slot = value
			return err
		})
		if i == len(out) {
			return out, nil
		}
		return finishMixedArray(dec, out[:i], nil, len(out), err)
	case firstCode == msgpcode.False || firstCode == msgpcode.True:
		out := make([]bool, n)
		i, err := walkTypedArray(dec, out, func(dec *msgpack.Decoder, slot *bool) error {
			code, err := dec.PeekCode()
			if err != nil {
				return err
			}
			if code-msgpcode.False > msgpcode.True-msgpcode.False {
				return errTypedArrayMismatch
			}
			value, err := dec.DecodeBool()
			*slot = value
			return err
		})
		if i == len(out) {
			return out, nil
		}
		return finishMixedArray(dec, out[:i], nil, len(out), err)
	}

	// Wire tags above select efficient primitive readers. The representation
	// of complex and mixed lists is shared with owned ingress values.
	return canonicalPropertyList(n, func(int) (any, error) {
		return decodeStrictTypedValue(dec)
	})
}

func decodeFloat64Array(dec *msgpack.Decoder, n int) (any, error) {
	out := make([]float64, n)
	i, err := walkTypedArray(dec, out, func(dec *msgpack.Decoder, slot *float64) error {
		code, err := dec.PeekCode()
		if err != nil {
			return err
		}
		if code != msgpcode.Double {
			return errTypedArrayMismatch
		}
		value, err := dec.DecodeFloat64()
		*slot = value
		return err
	})
	if i == len(out) {
		return out, nil
	}
	return finishMixedArray(dec, out[:i], nil, len(out), err)
}

// errTypedArrayMismatch stops the typed walk at a different element kind or
// an already-decoded wide integer. The stop owner widens either form without
// exposing this internal marker to callers.
var errTypedArrayMismatch = fmt.Errorf("typed array element kind changed")

// walkTypedArray is small enough to inline each literal wire operation, so
// all primitive families share one walk without per-element dynamic dispatch.
func walkTypedArray[T float64 | float32 | int64 | string | bool](dec *msgpack.Decoder, out []T, decode func(*msgpack.Decoder, *T) error) (int, error) {
	for i := range out {
		if err := decode(dec, &out[i]); err != nil {
			return i, err
		}
	}
	return len(out), nil
}

func isSignedIntegerCode(code byte) bool {
	return code == msgpcode.Int8 || code == msgpcode.Int16 || code == msgpcode.Int32 || code == msgpcode.Int64
}

// isIntegerCode reports whether code holds an integer: a fixnum or any
// signed or unsigned integer code.
func isIntegerCode(code byte) bool {
	return isSignedIntegerCode(code) || isUnsignedIntegerCode(code) || msgpcode.IsFixedNum(code)
}

// An uninspected code uses MessagePack's reserved, never-encoded byte.
// Stored-value callers already inspected the code and avoid a second peek.
const uninspectedIntegerCode byte = 0xc1

// decodePropertyInteger is the shared integer reader for stored properties and
// transport values. Fitting values are written to signed without boxing; only
// a Uint64 above MaxInt64 returns an alternate value.
func decodePropertyInteger(dec *msgpack.Decoder, code byte, signed *int64) (any, error) {
	if code == uninspectedIntegerCode {
		var err error
		code, err = dec.PeekCode()
		if err != nil {
			return nil, err
		}
		if !isIntegerCode(code) {
			return nil, fmt.Errorf("transport integer has invalid MessagePack code %x", code)
		}
	}
	if code != msgpcode.Uint64 {
		value, err := dec.DecodeInt64()
		*signed = value
		return nil, err
	}
	n, err := dec.DecodeUint64()
	if err != nil {
		return nil, err
	}
	if n > math.MaxInt64 {
		return n, nil
	}
	*signed = int64(n)
	return nil, nil
}

func isUnsignedIntegerCode(code byte) bool {
	return code == msgpcode.Uint8 || code == msgpcode.Uint16 || code == msgpcode.Uint32 || code == msgpcode.Uint64
}

// decodeStrictTypedMap decodes a msgpack map keyed by strings, recursing
// through decodeStrictTypedValue for each value so nested arrays keep
// their strict type.
func decodeStrictTypedMap(dec *msgpack.Decoder) (any, error) {
	n, err := dec.DecodeMapLen()
	if err != nil {
		return nil, err
	}
	if err := validateMessagePackCollection(dec, n, 2); err != nil {
		return nil, err
	}
	out := make(map[string]any, n)
	for i := 0; i < n; i++ {
		k, err := dec.DecodeString()
		if err != nil {
			return nil, err
		}
		v, err := decodeStrictTypedValue(dec)
		if err != nil {
			return nil, err
		}
		out[k] = v
	}
	return out, nil
}

// finishMixedArray owns widening a typed prefix and decoding its remaining values.
// A supplied stop reports an interrupted homogeneous walk. Only its private
// mismatch marker enters widening; real decode errors return without consuming
// another value. A non-nil current value already consumed a wide integer.
func finishMixedArray[T any](dec *msgpack.Decoder, prefix []T, current any, totalLen int, stop ...error) (any, error) {
	if len(stop) > 0 {
		if stop[0] != errTypedArrayMismatch {
			return nil, stop[0]
		}
		if current == nil {
			var err error
			current, err = decodeStrictTypedValue(dec)
			if err != nil {
				return nil, err
			}
		}
	}
	out := make([]any, 0, totalLen)
	for _, value := range prefix {
		out = append(out, value)
	}
	out = append(out, current)
	for len(out) < totalLen {
		next, err := decodeStrictTypedValue(dec)
		if err != nil {
			return nil, err
		}
		out = append(out, next)
	}
	return out, nil
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
