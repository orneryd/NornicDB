package storage

import (
	"bytes"
	"fmt"
	"reflect"

	forkcodec "github.com/dgraph-io/badger/v4/experimental/nornic"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

// Extension 60 is reserved for experimental physical property encoding. Logical
// snapshots and transport values use ordinary typed MessagePack, not this frame.
const experimentalPropertyExtension int8 = 60

type storedExperimentalProperty struct{ value any }

func init() {
	msgpack.RegisterExtDecoder(experimentalPropertyExtension, storedExperimentalProperty{}, func(dec *msgpack.Decoder, target reflect.Value, length int) error {
		if length > forkcodec.MaxPayloadBytes+64 {
			return fmt.Errorf("experimental property: frame exceeds codec bound")
		}
		if length < 1 {
			return fmt.Errorf("experimental property: empty frame")
		}
		payload := make([]byte, length)
		if err := dec.ReadFull(payload); err != nil {
			return err
		}
		var value any
		switch payload[0] {
		case 1:
			raw, err := forkcodec.DecodePayload(payload)
			if err != nil {
				return fmt.Errorf("experimental property: %w", err)
			}
			reader := bytes.NewReader(raw)
			nested := msgpack.GetDecoder()
			nested.Reset(reader)
			defer msgpack.PutDecoder(nested)
			value, err = decodeStrictTypedValue(nested)
			if err != nil {
				return err
			}
			if reader.Len() != 0 {
				return fmt.Errorf("experimental property: trailing value bytes")
			}
		case 2:
			column, err := forkcodec.OpenInt64(payload[1:])
			if err != nil {
				return err
			}
			values := make([]int64, column.Len())
			if err = column.DecodeInto(values); err != nil {
				return err
			}
			value = values
		default:
			return fmt.Errorf("experimental property: unsupported codec %d", payload[0])
		}
		target.Set(reflect.ValueOf(storedExperimentalProperty{value: value}))
		return nil
	})
}

// encodeExperimentalProperty preserves the caller's typed value and only
// changes physical bytes when the complete extension saves at least 12.5%.
func encodeExperimentalProperty(value any, raw []byte) ([]byte, error) {
	var payload []byte
	if values, ok := value.([]int64); ok && len(values) >= 32 && len(values) <= forkcodec.MaxColumnValues {
		encoded, err := forkcodec.EncodeInt64(values)
		if err != nil {
			return nil, err
		}
		if len(encoded)+7 < len(raw)-len(raw)/8 {
			payload = append([]byte{2}, encoded...)
		}
	}
	compressed, err := forkcodec.EncodePayload(raw, 6)
	if err != nil {
		return nil, err
	}
	if compressed != nil && (payload == nil || len(compressed) < len(payload)) {
		payload = compressed
	}
	if payload == nil {
		return raw, nil
	}
	var out bytes.Buffer
	encoder := msgpack.GetEncoder()
	defer msgpack.PutEncoder(encoder)
	encoder.Reset(&out)
	if err := encoder.EncodeExtHeader(experimentalPropertyExtension, len(payload)); err != nil {
		return nil, err
	}
	out.Write(payload)
	return out.Bytes(), nil
}

// isExperimentalProperty examines only the extension header. A projection can
// skip a compressed value without allocating or decompressing its payload.
func isExperimentalProperty(raw []byte) bool {
	if len(raw) < 3 {
		return false
	}
	var offset int
	switch raw[0] {
	case msgpcode.Ext8:
		offset = 2
	case msgpcode.Ext16:
		offset = 3
	case msgpcode.Ext32:
		offset = 5
	default:
		return false
	}
	return len(raw) > offset && int8(raw[offset]) == experimentalPropertyExtension
}
