package storage

import (
	"encoding/binary"
	"io"
)

// buildAtomicRecordV2 builds a single WAL record in the v2 atomic format:
//
//	[magic:4][version:1][length:4][payload:N][crc:4][trailer:8][padding:0-7]
//
// The record is 8-byte aligned to prevent torn headers and to make padding skips deterministic.
// The returned int64 is the aligned record length (for accounting/stats).
func buildAtomicRecordV2(payload []byte) ([]byte, int64) {
	entryCRC := crc32Checksum(payload)

	headerSize := int64(4 + 1 + 4)          // magic + version + length
	bodySize := int64(len(payload) + 4 + 8) // payload + crc + trailer
	rawRecordLen := headerSize + bodySize
	alignedRecordLen := alignUp(rawRecordLen)

	record := make([]byte, alignedRecordLen)
	offset := 0

	binary.LittleEndian.PutUint32(record[offset:], walMagic)
	offset += 4
	record[offset] = walFormatVersion
	offset++
	binary.LittleEndian.PutUint32(record[offset:], uint32(len(payload)))
	offset += 4
	copy(record[offset:], payload)
	offset += len(payload)
	binary.LittleEndian.PutUint32(record[offset:], entryCRC)
	offset += 4
	binary.LittleEndian.PutUint64(record[offset:], walTrailer)
	// padding is already zeroed by make(...)

	return record, alignedRecordLen
}

// writeAtomicRecordV2 writes a single WAL record in the v2 atomic format directly to w:
//
//	[magic:4][version:1][length:4][payload:N][crc:4][trailer:8][padding:0-7]
//
// This avoids allocating a full record buffer on the hot append path.
// The returned int64 is the aligned record length (for accounting/stats).
func writeAtomicRecordV2(w io.Writer, payload []byte) (int64, error) {
	entryCRC := crc32Checksum(payload)

	headerSize := int64(4 + 1 + 4)          // magic + version + length
	bodySize := int64(len(payload) + 4 + 8) // payload + crc + trailer
	rawRecordLen := headerSize + bodySize
	alignedRecordLen := alignUp(rawRecordLen)

	var header [9]byte
	binary.LittleEndian.PutUint32(header[0:], walMagic)
	header[4] = walFormatVersion
	binary.LittleEndian.PutUint32(header[5:], uint32(len(payload)))

	if _, err := w.Write(header[:]); err != nil {
		return 0, err
	}
	if _, err := w.Write(payload); err != nil {
		return 0, err
	}

	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], entryCRC)
	if _, err := w.Write(crcBuf[:]); err != nil {
		return 0, err
	}

	var trailerBuf [8]byte
	binary.LittleEndian.PutUint64(trailerBuf[:], walTrailer)
	if _, err := w.Write(trailerBuf[:]); err != nil {
		return 0, err
	}

	padding := int(alignedRecordLen - rawRecordLen) // 0-7
	if padding > 0 {
		var zeros [8]byte
		if _, err := w.Write(zeros[:padding]); err != nil {
			return 0, err
		}
	}

	return alignedRecordLen, nil
}
