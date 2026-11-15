package storage

import (
	"bytes"
	"encoding/binary"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
	"io"
)

// skipUnselectedProperty advances over contiguous MessagePack payloads without
// copying their bytes into the decoder's scratch buffer. bytes.Reader implements
// io.ByteScanner, so the decoder has no buffered input to invalidate. Containers
// retain the canonical decoder walk; selected values retain strict decoding.
func skipUnselectedProperty(dec *msgpack.Decoder, reader *bytes.Reader, raw []byte) error {
	if len(raw) == 0 {
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
