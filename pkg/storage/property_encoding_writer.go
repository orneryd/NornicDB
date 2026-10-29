package storage

import (
	"bytes"
	"fmt"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

// inspectPropertyEncoding validates an optional nesting limit and detects
// opaque values whose MessagePack encoders need their own writer.
func inspectPropertyEncoding(value any, depth, limit int) (bool, error) {
	if limit >= 0 && depth > limit {
		return false, fmt.Errorf("replication property nesting depth exceeds %d", limit)
	}
	switch value := value.(type) {
	case []any:
		isolated := false
		for _, item := range value {
			needsIsolation, err := inspectPropertyEncoding(item, depth+1, limit)
			if err != nil {
				return false, err
			}
			isolated = isolated || needsIsolation
		}
		return isolated, nil
	case map[string]any:
		isolated := false
		for _, item := range value {
			needsIsolation, err := inspectPropertyEncoding(item, depth+1, limit)
			if err != nil {
				return false, err
			}
			isolated = isolated || needsIsolation
		}
		return isolated, nil
	default:
		return !isSimplePropertyEncoding(value), nil
	}
}

// These supported scalar and homogeneous list types cannot supply a custom
// MessagePack encoder. Other values are isolated, including registered typed
// Cypher extensions whose callbacks receive the encoder's writer.
func isSimplePropertyEncoding(value any) bool {
	switch value.(type) {
	case nil, bool, string, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64, float32, float64, time.Time,
		[]byte, []string, []int, []int32, []int64, []float32, []float64, []bool:
		return true
	default:
		return false
	}
}

// encodeIsolatedPropertyValue walks supported property containers so an
// opaque value cannot modify its parent or its siblings through Writer().
// Callers use MessagePack's direct encoding for values without custom leaves.
func encodeIsolatedPropertyValue(enc *msgpack.Encoder, value any, depth, limit int) error {
	switch value := value.(type) {
	case []any:
		if err := enc.EncodeArrayLen(len(value)); err != nil {
			return err
		}
		for _, item := range value {
			if err := encodeIsolatedPropertyValue(enc, item, depth+1, limit); err != nil {
				return err
			}
		}
		return nil
	case map[string]any:
		if err := enc.EncodeMapLen(len(value)); err != nil {
			return err
		}
		for key, item := range value {
			if err := enc.EncodeString(key); err != nil {
				return err
			}
			if err := encodeIsolatedPropertyValue(enc, item, depth+1, limit); err != nil {
				return err
			}
		}
		return nil
	default:
		if isSimplePropertyEncoding(value) {
			return enc.Encode(value)
		}
		var buffer bytes.Buffer
		if err := msgpack.NewEncoder(&buffer).Encode(value); err != nil {
			return err
		}
		var err error
		if limit < 0 {
			err = validateMessagePackValue(buffer.Bytes())
		} else {
			err = validateMessagePackValue(buffer.Bytes(), limit-depth+1)
		}
		if err != nil {
			return fmt.Errorf("invalid MessagePack property %T: %w: %w", value, ErrInvalidData, err)
		}
		_, err = enc.Writer().Write(buffer.Bytes())
		return err
	}
}
