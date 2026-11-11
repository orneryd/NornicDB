package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/orneryd/nornicdb/internal/jsontext"
)

// A historical audit has no replay image to preserve. Keep its authenticated
// envelope and an explicit omission, rather than inventing a node identity or
// materializing its vectors. Compaction/upgrade can write this bounded record.
const walOversizedAuditOmission = `{"audit_omitted":"oversized historical embedding"}`

// readOversizedEmbeddingAudit owns the exception for complete oversized audit
// frames emitted by the b6 v3 writer or carried in current raw-Data framing.
// reader starts after the nine-byte frame header. No other operation/version
// gets this exception. The ordinary 16 MiB allocation/write bound is unchanged.
func readOversizedEmbeddingAudit(reader io.Reader, size uint32, version byte) (WALEntry, int64, error) {
	return readOversizedFrame(reader, size, version, false)
}

// readOversizedFrame authenticates one frame over the 16 MiB bound: exact
// lengths, the data and frame checksums, trailer and padding. A complete
// embedding audit is bounded to an explicit omission. With carryHistorical,
// the V4 upgrade of a store journal, a complete version-3 frame of any other
// operation is returned whole: the b6 writer had no frame bound, and the
// upgrade carries such a record as a spanned record. Anything else is refused.
func readOversizedFrame(reader io.Reader, size uint32, version byte, carryHistorical bool) (WALEntry, int64, error) {
	if version != 3 && version != walFormatVersionCurrent {
		return WALEntry{}, 0, fmt.Errorf("%w: oversized frame in unsupported frame version %d", ErrWALCorrupted, version)
	}
	outer := crc32.New(crc32Table)
	payload := io.TeeReader(io.LimitReader(reader, int64(size)), outer)
	var word [4]byte
	if _, err := io.ReadFull(payload, word[:]); err != nil {
		return WALEntry{}, 0, fmt.Errorf("%w: oversized frame header: %w", ErrWALCorrupted, err)
	}
	headerLen := binary.LittleEndian.Uint32(word[:])
	if headerLen > walMaxEntrySize-8 || uint64(headerLen)+8 > uint64(size) {
		return WALEntry{}, 0, fmt.Errorf("%w: invalid oversized frame header length", ErrWALCorrupted)
	}
	header := make([]byte, headerLen)
	if _, err := io.ReadFull(payload, header); err != nil {
		return WALEntry{}, 0, fmt.Errorf("%w: oversized frame header: %w", ErrWALCorrupted, err)
	}
	var entry WALEntry
	if err := jsontext.Unmarshal(header, &entry); err != nil {
		return WALEntry{}, 0, fmt.Errorf("%w: invalid oversized frame envelope: %w", ErrWALCorrupted, err)
	}
	audit := entry.Operation == OpUpdateEmbedding && len(entry.Data) == 0
	carry := carryHistorical && version == 3 && len(entry.Data) == 0
	if !audit && !carry {
		return WALEntry{}, 0, fmt.Errorf("%w: entry size %d exceeds maximum %d", ErrWALCorrupted, size, walMaxEntrySize)
	}
	if _, err := io.ReadFull(payload, word[:]); err != nil {
		return WALEntry{}, 0, fmt.Errorf("%w: oversized frame data length: %w", ErrWALCorrupted, err)
	}
	dataLen := binary.LittleEndian.Uint32(word[:])
	if uint64(headerLen)+8+uint64(dataLen) != uint64(size) {
		return WALEntry{}, 0, fmt.Errorf("%w: invalid oversized frame data length", ErrWALCorrupted)
	}
	inner := crc32.New(crc32Table)
	var data []byte
	if audit {
		if _, err := io.CopyN(inner, payload, int64(dataLen)); err != nil {
			return WALEntry{}, 0, fmt.Errorf("%w: incomplete oversized frame: %w", ErrWALCorrupted, err)
		}
	} else {
		// Grows with the bytes actually present, so a damaged length cannot
		// allocate more than the file holds.
		var err error
		data, err = io.ReadAll(io.LimitReader(payload, int64(dataLen)))
		if err == nil && len(data) != int(dataLen) {
			err = io.ErrUnexpectedEOF
		}
		if err != nil {
			return WALEntry{}, 0, fmt.Errorf("%w: incomplete oversized frame: %w", ErrWALCorrupted, err)
		}
		_, _ = inner.Write(data)
	}
	if inner.Sum32() != entry.Checksum {
		return WALEntry{}, 0, fmt.Errorf("%w: oversized frame data checksum", ErrWALChecksumFailed)
	}
	if _, err := io.ReadFull(reader, word[:]); err != nil {
		return WALEntry{}, 0, fmt.Errorf("%w: oversized frame checksum: %w", ErrWALCorrupted, err)
	}
	if binary.LittleEndian.Uint32(word[:]) != outer.Sum32() {
		return WALEntry{}, 0, fmt.Errorf("%w: oversized frame checksum", ErrWALChecksumFailed)
	}
	var trailer [8]byte
	if _, err := io.ReadFull(reader, trailer[:]); err != nil {
		return WALEntry{}, 0, fmt.Errorf("%w: oversized frame trailer: %w", ErrWALCorrupted, err)
	}
	if binary.LittleEndian.Uint64(trailer[:]) != walTrailer {
		return WALEntry{}, 0, fmt.Errorf("%w: invalid oversized frame trailer", ErrWALCorrupted)
	}
	rawLength := int64(9) + int64(size) + 4 + 8
	length := alignUp(rawLength)
	var padding [7]byte
	if _, err := io.ReadFull(reader, padding[:length-rawLength]); err != nil {
		return WALEntry{}, 0, fmt.Errorf("%w: oversized frame padding: %w", ErrWALCorrupted, err)
	}
	if !audit {
		entry.Data = data
		entry.framingVersion = version
		return entry, length, nil
	}
	entry.Data = []byte(walOversizedAuditOmission)
	entry.Checksum = crc32Checksum(entry.Data)
	entry.framingVersion = walFormatVersionCurrent
	return entry, length, nil
}
