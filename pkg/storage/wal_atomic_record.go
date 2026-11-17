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
	alignedRecordLen := alignUp(int64(len(payload)) + 21)
	record := make([]byte, alignedRecordLen)
	fillAtomicRecord(record, payload, version)
	return record, alignedRecordLen
}

// fillAtomicRecord is the framing owner for both owned records and a bufio
// writer's reserved storage. Padding must be cleared when storage is reused.
func fillAtomicRecord(record, payload []byte, version uint8) {
	binary.LittleEndian.PutUint32(record, walMagic)
	record[4] = version
	binary.LittleEndian.PutUint32(record[5:], uint32(len(payload)))
	end := 9 + len(payload)
	copy(record[9:end], payload)
	binary.LittleEndian.PutUint32(record[end:], crc32Checksum(payload))
	binary.LittleEndian.PutUint64(record[end+4:], walTrailer)
	clear(record[end+12:])
}

// writeAtomicRecordBufio writes a single WAL record to w, using a single Write
// when w is a *bufio.Writer so that a record is never split across buffer flushes.
// This prevents recovery from seeing a partial last record after a clean close.
func writeAtomicRecordBufio(w io.Writer, payload []byte, version uint8) (int64, error) {
	alignedRecordLen := alignUp(int64(len(payload)) + 21)
	if bw, ok := w.(*bufio.Writer); ok && alignedRecordLen <= int64(bw.Size()) {
		if int64(bw.Available()) < alignedRecordLen {
			if err := bw.Flush(); err != nil {
				return 0, err
			}
		}
		// Reserve the complete frame in the writer's own backing storage.
		// Write commits this same slice before any subsequent buffer operation.
		record := bw.AvailableBuffer()[:int(alignedRecordLen)]
		fillAtomicRecord(record, payload, version)
		n, err := bw.Write(record)
		if err != nil {
			return 0, err
		}
		if n != len(record) {
			return 0, io.ErrShortWrite
		}
		return alignedRecordLen, nil
	}
	// Oversized frames and generic writers retain one contiguous write.
	record, alignedRecordLen := buildAtomicRecord(payload, version)
	if bw, ok := w.(*bufio.Writer); ok && bw.Available() < len(record) {
		if err := bw.Flush(); err != nil {
			return 0, err
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
