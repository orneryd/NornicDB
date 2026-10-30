package storage

import (
	"bufio"
	"encoding/binary"
	"io"
)

// alignUp rounds n up to the nearest multiple of walAlignment (8 bytes).
// This ensures WAL records start at aligned offsets, preventing torn headers.
func alignUp(n int64) int64 {
	return (n + walAlignment - 1) &^ (walAlignment - 1)
}

// buildAtomicRecord builds a single WAL record in the atomic format:
//
//	[magic:4][version:1][length:4][payload:N][crc:4][trailer:8][padding:0-7]
//
// The record is 8-byte aligned to prevent torn headers and to make padding skips deterministic.
// The returned int64 is the aligned record length (for accounting/stats).
func buildAtomicRecord(payload []byte, version uint8) ([]byte, int64) {
	entryCRC := crc32Checksum(payload)

	headerSize := int64(4 + 1 + 4)          // magic + version + length
	bodySize := int64(len(payload) + 4 + 8) // payload + crc + trailer
	rawRecordLen := headerSize + bodySize
	alignedRecordLen := alignUp(rawRecordLen)

	record := make([]byte, alignedRecordLen)
	offset := 0

	binary.LittleEndian.PutUint32(record[offset:], walMagic)
	offset += 4
	record[offset] = version
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

// writeAtomicRecord writes a single WAL record in the atomic format directly to w:
//
//	[magic:4][version:1][length:4][payload:N][crc:4][trailer:8][padding:0-7]
//
// This avoids allocating a full record buffer on the hot append path.
// The returned int64 is the aligned record length (for accounting/stats).
func writeAtomicRecord(w io.Writer, payload []byte, version uint8) (int64, error) {
	entryCRC := crc32Checksum(payload)

	headerSize := int64(4 + 1 + 4)          // magic + version + length
	bodySize := int64(len(payload) + 4 + 8) // payload + crc + trailer
	rawRecordLen := headerSize + bodySize
	alignedRecordLen := alignUp(rawRecordLen)

	var header [9]byte
	binary.LittleEndian.PutUint32(header[0:], walMagic)
	header[4] = version
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

// writeAtomicRecordBufio writes a single WAL record to w, using a single Write
// when w is a *bufio.Writer so that a record is never split across buffer flushes.
// This prevents recovery from seeing a partial last record after a clean close.
func writeAtomicRecordBufio(w io.Writer, payload []byte, version uint8) (int64, error) {
	record, alignedRecordLen := buildAtomicRecord(payload, version)
	if bw, ok := w.(*bufio.Writer); ok {
		if bw.Available() < len(record) {
			if err := bw.Flush(); err != nil {
				return 0, err
			}
		}
	}
	n, err := w.Write(record)
	if err != nil {
		return 0, err
	}
	if n != len(record) {
		return 0, io.ErrShortWrite
	}
	return alignedRecordLen, nil
}
