package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"sync"

	forkcodec "github.com/dgraph-io/badger/v4/experimental/nornic"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

// Extension 60 is reserved for experimental physical property encoding. Logical
// snapshots and transport values use ordinary typed MessagePack, not this frame.
const experimentalPropertyExtension int8 = 60

type storedExperimentalProperty struct{ value any }

const maxExperimentalPropertyScratchBytes = 128 << 10

type experimentalPropertyScratch struct{ bytes []byte }

var experimentalPropertyScratchPool = sync.Pool{New: func() any { return new(experimentalPropertyScratch) }}

func experimentalPropertyLength(length int) error {
	if length > forkcodec.MaxPayloadBytes+64 {
		return fmt.Errorf("experimental property: frame exceeds codec bound")
	}
	if length < 1 {
		return fmt.Errorf("experimental property: empty frame")
	}
	return nil
}

func init() {
	msgpack.RegisterExtDecoder(experimentalPropertyExtension, storedExperimentalProperty{}, func(dec *msgpack.Decoder, target reflect.Value, length int) error {
		if err := experimentalPropertyLength(length); err != nil {
			return err
		}
		payload := make([]byte, length)
		if err := dec.ReadFull(payload); err != nil {
			return err
		}
		value, err := decodeExperimentalProperty(payload)
		if err != nil {
			return err
		}
		target.Set(reflect.ValueOf(storedExperimentalProperty{value: value}))
		return nil
	})
}

// decodeExperimentalProperty owns the typed payload semantics for both generic
// MessagePack callers and borrowed record spans. Every returned value is owned.
func decodeExperimentalProperty(payload []byte) (any, error) {
	if err := experimentalPropertyLength(len(payload)); err != nil {
		return nil, err
	}
	switch payload[0] {
	case 1, 3:
		// Typed decoding below detaches strings, bytes and containers. Keep the
		// decompression buffer only until that owned value has been produced.
		var raw []byte
		var err error
		experimental := config.IsExperimentalPropertyStorageEnabled()
		if experimental {
			scratch := experimentalPropertyScratchPool.Get().(*experimentalPropertyScratch)
			defer experimentalPropertyScratchPool.Put(scratch)
			raw, err = forkcodec.DecodePayloadInto(scratch.bytes, payload)
			if err == nil && cap(raw) > cap(scratch.bytes) && cap(raw) <= maxExperimentalPropertyScratchBytes {
				scratch.bytes = raw[:0]
			}
		} else {
			raw, err = forkcodec.DecodePayload(payload)
		}
		if err != nil {
			return nil, fmt.Errorf("experimental property: %w", err)
		}
		if experimental {
			if text, consumed, ok := msgpackStringPayload(raw); ok && consumed == len(raw) {
				return string(text), nil
			}
		}
		reader := bytes.NewReader(raw)
		nested := msgpack.GetDecoder()
		nested.Reset(reader)
		defer msgpack.PutDecoder(nested)
		value, err := decodeStrictTypedValue(nested)
		if err != nil {
			return nil, err
		}
		if reader.Len() != 0 {
			return nil, fmt.Errorf("experimental property: trailing value bytes")
		}
		return value, nil
	case 2:
		column, err := forkcodec.OpenInt64(payload[1:])
		if err != nil {
			return nil, err
		}
		values := make([]int64, column.Len())
		if err = column.DecodeInto(values); err != nil {
			return nil, err
		}
		return values, nil
	default:
		return nil, fmt.Errorf("experimental property: unsupported codec %d", payload[0])
	}
}

// decodeStoredPropertyValue borrows an experimental extension only for the
// duration of decoding. bytes.Reader is unbuffered; advancing it leaves the
// canonical decoder positioned at the next token without a payload copy.
func decodeStoredPropertyValue(dec *msgpack.Decoder, reader *bytes.Reader, raw []byte) (any, error) {
	if !isExperimentalProperty(raw) || !config.IsExperimentalPropertyStorageEnabled() {
		return decodeStrictTypedValue(dec)
	}
	var length uint64
	var header int
	switch raw[0] {
	case msgpcode.Ext8:
		header = 2
		length = uint64(raw[1])
	case msgpcode.Ext16:
		header = 3
		length = uint64(binary.BigEndian.Uint16(raw[1:3]))
	case msgpcode.Ext32:
		header = 5
		length = uint64(binary.BigEndian.Uint32(raw[1:5]))
	}
	if length > uint64(forkcodec.MaxPayloadBytes+64) {
		return nil, fmt.Errorf("experimental property: frame exceeds codec bound")
	}
	if err := experimentalPropertyLength(int(length)); err != nil {
		return nil, err
	}
	end := uint64(header+1) + length
	if end > uint64(len(raw)) {
		if len(raw) == header+1 {
			return nil, io.EOF
		}
		return nil, io.ErrUnexpectedEOF
	}
	value, err := decodeExperimentalProperty(raw[header+1 : int(end)])
	if err != nil {
		return nil, err
	}
	if _, err = reader.Seek(int64(end), io.SeekCurrent); err != nil {
		return nil, err
	}
	return value, nil
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
	// A native integer column already saving 75% does not need a second
	// compression contest. Keep the typed packed representation and avoid
	// recompressing the much larger MessagePack array on every write.
	if payload == nil || len(payload)+6 > len(raw)/4 {
		compressed, err := forkcodec.EncodePayload(raw, 6)
		if err != nil {
			return nil, err
		}
		if compressed != nil && (payload == nil || len(compressed) < len(payload)) {
			payload = compressed
		}
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

// skipUnselectedProperty advances over contiguous MessagePack payloads without
// copying their bytes into the decoder's scratch buffer. bytes.Reader implements
// io.ByteScanner, so the decoder has no buffered input to invalidate. Containers
// retain the canonical decoder walk; selected values retain strict decoding.
func skipUnselectedProperty(dec *msgpack.Decoder, reader *bytes.Reader, raw []byte) error {
	if !config.IsExperimentalPropertyStorageEnabled() || len(raw) == 0 {
		return dec.Skip()
	}
	header, extra := 1, uint64(0)
	var width int
	switch raw[0] {
	case msgpcode.Str8, msgpcode.Bin8:
		width = 1
	case msgpcode.Str16, msgpcode.Bin16:
		width = 2
	case msgpcode.Str32, msgpcode.Bin32:
		width = 4
	case msgpcode.Ext8:
		width = 1
		extra = 1
	case msgpcode.Ext16:
		width = 2
		extra = 1
	case msgpcode.Ext32:
		width = 4
		extra = 1
	default:
		if raw[0] < 0xa0 || raw[0] > 0xbf {
			return dec.Skip()
		}
	}
	header += width
	if len(raw) < header {
		return io.ErrUnexpectedEOF
	}
	var length uint64
	switch width {
	case 0:
		length = uint64(raw[0] & 31)
	case 1:
		length = uint64(raw[1])
	case 2:
		length = uint64(binary.BigEndian.Uint16(raw[1:3]))
	case 4:
		length = uint64(binary.BigEndian.Uint32(raw[1:5]))
	}
	size := uint64(header) + extra + length
	if size > uint64(len(raw)) {
		return io.ErrUnexpectedEOF
	}
	_, err := reader.Seek(int64(size), io.SeekCurrent)
	return err
}
