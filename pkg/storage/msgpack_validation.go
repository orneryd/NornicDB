package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/vmihailenco/msgpack/v5"
)

// validateMessagePackCollection bounds a decoded container against its
// remaining encoded bytes before allocating its elements. Stored property
// decoders use a sized reader so both malformed and valid sizes can be checked
// without putting an arbitrary limit on legitimate property values.
func validateMessagePackCollection(dec *msgpack.Decoder, count, valuesPerEntry int) error {
	reader, ok := dec.Buffered().(interface{ Len() int })
	if !ok {
		return errors.New("MessagePack property decoder requires a sized reader")
	}
	if count < 0 || count > reader.Len()/valuesPerEntry {
		return io.ErrUnexpectedEOF
	}
	return nil
}

// validateMessagePackValue verifies the framing of one complete MessagePack
// value before a decoder uses encoded collection lengths for allocations.
// Every pending value requires at least one byte, so malformed container
// headers are rejected without allocating in proportion to a forged length.
// Values are visited iteratively; nesting depth does not consume stack space.
func validateMessagePackValue(data []byte, maxNesting ...int) error {
	var pending uint64 = 1
	offset := 0
	// Most callers only need structural validation. Replication additionally
	// bounds depth because its decoded property values are recursively walked
	// by storage. Keep shallow replication messages on the stack.
	var localFrames [64]uint64
	frames := localFrames[:0]
	limited := len(maxNesting) != 0
	if limited {
		frames = append(frames, 1)
	}
	for pending > 0 {
		if limited {
			for len(frames) != 0 && frames[len(frames)-1] == 0 {
				frames = frames[:len(frames)-1]
			}
			frames[len(frames)-1]--
		}
		if offset == len(data) {
			return io.ErrUnexpectedEOF
		}
		code := data[offset]
		offset++
		pending--
		var size, children uint64
		switch {
		case code <= 0x7f || code >= 0xe0:
		case code >= 0x80 && code <= 0x8f:
			children = uint64(code&0x0f) * 2
		case code >= 0x90 && code <= 0x9f:
			children = uint64(code & 0x0f)
		case code >= 0xa0 && code <= 0xbf:
			size = uint64(code & 0x1f)
		case code == 0xc0 || code == 0xc2 || code == 0xc3:
		case code == 0xc1:
			return errors.New("invalid MessagePack code")
		case code == 0xc4 || code == 0xc7 || code == 0xd9:
			if len(data)-offset < 1 {
				return io.ErrUnexpectedEOF
			}
			size = uint64(data[offset])
			offset++
			if code == 0xc7 {
				size++ // extension type byte
			}
		case code == 0xc5 || code == 0xc8 || code == 0xda || code == 0xdc || code == 0xde:
			if len(data)-offset < 2 {
				return io.ErrUnexpectedEOF
			}
			n := uint64(binary.BigEndian.Uint16(data[offset:]))
			offset += 2
			switch code {
			case 0xdc:
				children = n
			case 0xde:
				children = n * 2
			default:
				size = n
				if code == 0xc8 {
					size++
				}
			}
		case code == 0xc6 || code == 0xc9 || code == 0xdb || code == 0xdd || code == 0xdf:
			if len(data)-offset < 4 {
				return io.ErrUnexpectedEOF
			}
			n := uint64(binary.BigEndian.Uint32(data[offset:]))
			offset += 4
			switch code {
			case 0xdd:
				children = n
			case 0xdf:
				children = n * 2
			default:
				size = n
				if code == 0xc9 {
					size++
				}
			}
		case code == 0xca || code == 0xce || code == 0xd2:
			size = 4
		case code == 0xcb || code == 0xcf || code == 0xd3:
			size = 8
		case code == 0xcc || code == 0xd0:
			size = 1
		case code == 0xcd || code == 0xd1:
			size = 2
		case code >= 0xd4 && code <= 0xd8:
			size = (1 << (code - 0xd4)) + 1 // extension type byte
		default:
			return errors.New("invalid MessagePack code")
		}
		remaining := uint64(len(data) - offset)
		if size > remaining {
			return io.ErrUnexpectedEOF
		}
		offset += int(size)
		pending += children
		if pending > uint64(len(data)-offset) {
			return io.ErrUnexpectedEOF
		}
		if limited && children != 0 {
			if len(frames) >= maxNesting[0] {
				return fmt.Errorf("MessagePack nesting depth exceeds %d", maxNesting[0])
			}
			frames = append(frames, children)
		}
	}
	if offset != len(data) {
		return errors.New("trailing MessagePack value")
	}
	return nil
}
