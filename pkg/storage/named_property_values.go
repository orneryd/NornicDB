package storage

import (
	"bytes"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

// EncodeNamedProperties encodes semantic property names and values without
// dictionary IDs, using the same temporal and numeric owner as stored bodies.
func EncodeNamedProperties(properties map[string]any) ([]byte, error) {
	return msgpack.Marshal(encodeStoredTemporalValue(properties))
}

// DecodeNamedProperties restores authoritative stored property representations
// from a dictionary-independent semantic map, including named-zone instants.
func DecodeNamedProperties(data []byte) (map[string]any, error) {
	return decodeNamedValues(data, decodeStrictTypedValue)
}

// DecodeNamedMetadata preserves the body metadata codec's scalar/array Go
// representations, rather than applying property integer/list normalization.
func DecodeNamedMetadata(data []byte) (map[string]any, error) {
	return decodeNamedValues(data, func(dec *msgpack.Decoder) (any, error) { return dec.DecodeInterface() })
}

func decodeNamedValues(data []byte, decode func(*msgpack.Decoder) (any, error)) (map[string]any, error) {
	if err := validateMessagePackValue(data); err != nil {
		return nil, fmt.Errorf("invalid named property values: %w", err)
	}
	reader := bytes.NewReader(data)
	decoder := msgpack.NewDecoder(reader)
	value, err := decode(decoder)
	if err != nil {
		return nil, err
	}
	if reader.Len() != 0 {
		return nil, fmt.Errorf("trailing named property values")
	}
	if value == nil {
		return nil, nil
	}
	properties, ok := restoreStoredTemporalValue(value).(map[string]any)
	if !ok {
		return nil, fmt.Errorf("named property values must be a map")
	}
	return properties, nil
}
