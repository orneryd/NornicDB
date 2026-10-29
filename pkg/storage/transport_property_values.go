package storage

import (
	"fmt"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

// transportPropertyMap is used only by replication wire images. It keeps Go
// property types which a plain map[string]any MessagePack decode cannot infer
// (for example []int64 versus []any), without changing Badger's stored bodies.
type transportPropertyMap map[string]any

// Replication values are subsequently visited recursively by storage. Limit
// transport nesting to bound stack growth on incoming and outgoing images.
const maxTransportPropertyDepth = 1024

const (
	transportNil uint8 = iota
	transportBool
	transportString
	transportInt
	transportInt8
	transportInt16
	transportInt32
	transportInt64
	transportUint
	transportUint8
	transportUint16
	transportUint32
	transportUint64
	transportFloat32
	transportFloat64
	transportTimestamp
	transportBytes
	transportStrings
	transportInts
	transportInt32s
	transportInt64s
	transportFloat32s
	transportFloat64s
	transportBools
	transportList
	transportMap
	transportTyped
)

func (m transportPropertyMap) EncodeMsgpack(enc *msgpack.Encoder) error {
	return encodeTransportPropertyMap(enc, m, 0)
}

func encodeTransportPropertyMap(enc *msgpack.Encoder, m transportPropertyMap, depth int) error {
	if m == nil {
		return enc.EncodeNil()
	}
	if err := enc.EncodeMapLen(len(m)); err != nil {
		return err
	}
	for key, value := range m {
		if err := enc.EncodeString(key); err != nil {
			return err
		}
		if err := encodeTransportProperty(enc, value, depth); err != nil {
			return fmt.Errorf("property %q: %w", key, err)
		}
	}
	return nil
}

func encodeTransportProperty(enc *msgpack.Encoder, value any, depth int) error {
	if depth > maxTransportPropertyDepth {
		return fmt.Errorf("replication property nesting depth exceeds %d", maxTransportPropertyDepth)
	}
	var tag uint8
	switch value.(type) {
	case nil:
		tag = transportNil
	case bool:
		tag = transportBool
	case string:
		tag = transportString
	case int:
		tag = transportInt
	case int8:
		tag = transportInt8
	case int16:
		tag = transportInt16
	case int32:
		tag = transportInt32
	case int64:
		tag = transportInt64
	case uint:
		tag = transportUint
	case uint8:
		tag = transportUint8
	case uint16:
		tag = transportUint16
	case uint32:
		tag = transportUint32
	case uint64:
		tag = transportUint64
	case float32:
		tag = transportFloat32
	case float64:
		tag = transportFloat64
	case time.Time:
		tag = transportTimestamp
	case []byte:
		tag = transportBytes
	case []string:
		tag = transportStrings
	case []int:
		tag = transportInts
	case []int32:
		tag = transportInt32s
	case []int64:
		tag = transportInt64s
	case []float32:
		tag = transportFloat32s
	case []float64:
		tag = transportFloat64s
	case []bool:
		tag = transportBools
	case []any:
		tag = transportList
	case map[string]any:
		tag = transportMap
	case TypedPropertyValue:
		tag = transportTyped
	default:
		return fmt.Errorf("unsupported transport property type %T", value)
	}
	if err := enc.EncodeArrayLen(2); err != nil {
		return err
	}
	if err := enc.EncodeUint8(tag); err != nil {
		return err
	}
	switch value := value.(type) {
	case time.Time:
		data, err := value.MarshalBinary()
		if err != nil {
			return err
		}
		return enc.EncodeBytes(data)
	case []any:
		if value == nil {
			return enc.EncodeNil()
		}
		if err := enc.EncodeArrayLen(len(value)); err != nil {
			return err
		}
		for _, item := range value {
			if err := encodeTransportProperty(enc, item, depth+1); err != nil {
				return err
			}
		}
		return nil
	case map[string]any:
		return encodeTransportPropertyMap(enc, transportPropertyMap(value), depth+1)
	case TypedPropertyValue:
		// Typed Cypher scalars already have registered MessagePack extension
		// codecs. Refuse an unregistered type rather than silently turning it
		// into a generic map on the receiving side.
		data, err := msgpack.Marshal(value)
		if err != nil {
			return err
		}
		if len(data) == 0 || !(data[0] >= 0xd4 && data[0] <= 0xd8 || data[0] >= 0xc7 && data[0] <= 0xc9) {
			return fmt.Errorf("unregistered transport property type %T", value)
		}
		_, err = enc.Writer().Write(data)
		return err
	default:
		return enc.Encode(value)
	}
}

func (m *transportPropertyMap) DecodeMsgpack(dec *msgpack.Decoder) error {
	return decodeTransportPropertyMap(dec, m, 0)
}

func decodeTransportPropertyMap(dec *msgpack.Decoder, m *transportPropertyMap, depth int) error {
	count, err := dec.DecodeMapLen()
	if err != nil {
		return err
	}
	if count == -1 {
		*m = nil
		return nil
	}
	if err := validateMessagePackCollection(dec, count, 2); err != nil {
		return err
	}
	out := make(transportPropertyMap, count)
	for i := 0; i < count; i++ {
		key, err := dec.DecodeString()
		if err != nil {
			return err
		}
		value, err := decodeTransportProperty(dec, depth)
		if err != nil {
			return fmt.Errorf("property %q: %w", key, err)
		}
		out[key] = value
	}
	*m = out
	return nil
}

func decodeTransportAs[T any](dec *msgpack.Decoder) (any, error) {
	var value T
	err := dec.Decode(&value)
	return value, err
}

type transportSigned interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64
}
type transportUnsigned interface {
	~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64
}

func readTransportInteger(dec *msgpack.Decoder) (int64, any, error) {
	code, err := dec.PeekCode()
	if err != nil {
		return 0, nil, err
	}
	if !isIntegerCode(code) {
		return 0, nil, fmt.Errorf("transport integer has invalid MessagePack code %x", code)
	}
	var signed int64
	wide, err := decodeStoredInteger(dec, code, &signed)
	return signed, wide, err
}

func decodeTransportSigned[T transportSigned](dec *msgpack.Decoder) (T, error) {
	signed, wide, err := readTransportInteger(dec)
	if err != nil {
		return 0, err
	}
	value := T(signed)
	if wide != nil || int64(value) != signed {
		return 0, fmt.Errorf("transport signed integer is out of range")
	}
	return value, nil
}

func decodeTransportUnsigned[T transportUnsigned](dec *msgpack.Decoder) (T, error) {
	signed, wide, err := readTransportInteger(dec)
	if err != nil {
		return 0, err
	}
	var unsigned uint64
	if wide != nil {
		unsigned = wide.(uint64)
	} else if signed < 0 {
		return 0, fmt.Errorf("transport unsigned integer is negative")
	} else {
		unsigned = uint64(signed)
	}
	value := T(unsigned)
	if uint64(value) != unsigned {
		return 0, fmt.Errorf("transport unsigned integer is out of range")
	}
	return value, nil
}

func decodeTransportSignedSlice[T transportSigned](dec *msgpack.Decoder) (any, error) {
	count, err := dec.DecodeArrayLen()
	if err != nil {
		return nil, err
	}
	if count == -1 {
		return []T(nil), nil
	}
	if err := validateMessagePackCollection(dec, count, 1); err != nil {
		return nil, err
	}
	values := make([]T, count)
	for i := range values {
		values[i], err = decodeTransportSigned[T](dec)
		if err != nil {
			return nil, err
		}
	}
	return values, nil
}

func decodeTransportProperty(dec *msgpack.Decoder, depth int) (any, error) {
	if depth > maxTransportPropertyDepth {
		return nil, fmt.Errorf("replication property nesting depth exceeds %d", maxTransportPropertyDepth)
	}
	n, err := dec.DecodeArrayLen()
	if err != nil {
		return nil, err
	}
	if n != 2 {
		return nil, fmt.Errorf("invalid transport property tuple length %d", n)
	}
	tag, err := decodeTransportUnsigned[uint8](dec)
	if err != nil {
		return nil, err
	}
	switch tag {
	case transportNil:
		return nil, dec.DecodeNil()
	case transportBool:
		return decodeTransportAs[bool](dec)
	case transportString:
		return decodeTransportAs[string](dec)
	case transportInt:
		return decodeTransportSigned[int](dec)
	case transportInt8:
		return decodeTransportSigned[int8](dec)
	case transportInt16:
		return decodeTransportSigned[int16](dec)
	case transportInt32:
		return decodeTransportSigned[int32](dec)
	case transportInt64:
		return decodeTransportSigned[int64](dec)
	case transportUint:
		return decodeTransportUnsigned[uint](dec)
	case transportUint8:
		return decodeTransportUnsigned[uint8](dec)
	case transportUint16:
		return decodeTransportUnsigned[uint16](dec)
	case transportUint32:
		return decodeTransportUnsigned[uint32](dec)
	case transportUint64:
		return decodeTransportUnsigned[uint64](dec)
	case transportFloat32:
		return decodeTransportAs[float32](dec)
	case transportFloat64:
		return decodeTransportAs[float64](dec)
	case transportTimestamp:
		data, err := dec.DecodeBytes()
		if err != nil {
			return nil, err
		}
		var value time.Time
		err = value.UnmarshalBinary(data)
		return value, err
	case transportBytes:
		return decodeTransportAs[[]byte](dec)
	case transportStrings:
		return decodeTransportAs[[]string](dec)
	case transportInts:
		return decodeTransportSignedSlice[int](dec)
	case transportInt32s:
		return decodeTransportSignedSlice[int32](dec)
	case transportInt64s:
		return decodeTransportSignedSlice[int64](dec)
	case transportFloat32s:
		return decodeTransportAs[[]float32](dec)
	case transportFloat64s:
		return decodeTransportAs[[]float64](dec)
	case transportBools:
		return decodeTransportAs[[]bool](dec)
	case transportList:
		count, err := dec.DecodeArrayLen()
		if err != nil {
			return nil, err
		}
		if count == -1 {
			return []any(nil), nil
		}
		if err := validateMessagePackCollection(dec, count, 1); err != nil {
			return nil, err
		}
		items := make([]any, count)
		for i := range items {
			items[i], err = decodeTransportProperty(dec, depth+1)
			if err != nil {
				return nil, err
			}
		}
		return items, nil
	case transportMap:
		var value transportPropertyMap
		if err := decodeTransportPropertyMap(dec, &value, depth+1); err != nil {
			return nil, err
		}
		return map[string]any(value), nil
	case transportTyped:
		value, err := dec.DecodeInterface()
		if err != nil {
			return nil, err
		}
		if _, ok := value.(TypedPropertyValue); !ok {
			return nil, fmt.Errorf("invalid typed transport property %T", value)
		}
		return value, nil
	default:
		return nil, fmt.Errorf("unknown transport property tag %d", tag)
	}
}
