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
	if version != 3 && version != walFormatVersionCurrent {
		return WALEntry{}, 0, fmt.Errorf("%w: oversized audit in unsupported frame version %d", ErrWALCorrupted, version)
	}
	outer := crc32.New(crc32Table)
	payload := io.TeeReader(io.LimitReader(reader, int64(size)), outer)
	var word [4]byte
	if _, err := io.ReadFull(payload, word[:]); err != nil {
		return WALEntry{}, 0, fmt.Errorf("%w: oversized audit header: %w", ErrWALCorrupted, err)
	}
	headerLen := binary.LittleEndian.Uint32(word[:])
	if headerLen > walMaxEntrySize-8 || uint64(headerLen)+8 > uint64(size) {
		return WALEntry{}, 0, fmt.Errorf("%w: invalid oversized audit header length", ErrWALCorrupted)
	}
	header := make([]byte, headerLen)
	if _, err := io.ReadFull(payload, header); err != nil {
		return WALEntry{}, 0, fmt.Errorf("%w: oversized audit header: %w", ErrWALCorrupted, err)
	}
	var entry WALEntry
	if err := jsontext.Unmarshal(header, &entry); err != nil {
		return WALEntry{}, 0, fmt.Errorf("%w: invalid oversized audit envelope: %w", ErrWALCorrupted, err)
	}
	if entry.Operation != OpUpdateEmbedding || len(entry.Data) != 0 {
		return WALEntry{}, 0, fmt.Errorf("%w: entry size %d exceeds maximum %d", ErrWALCorrupted, size, walMaxEntrySize)
	}
	if _, err := io.ReadFull(payload, word[:]); err != nil {
		return WALEntry{}, 0, fmt.Errorf("%w: oversized audit data length: %w", ErrWALCorrupted, err)
	}
	dataLen := binary.LittleEndian.Uint32(word[:])
	if uint64(headerLen)+8+uint64(dataLen) != uint64(size) {
		return WALEntry{}, 0, fmt.Errorf("%w: invalid oversized audit data length", ErrWALCorrupted)
	}
	inner := crc32.New(crc32Table)
	if _, err := io.CopyN(inner, payload, int64(dataLen)); err != nil {
		return WALEntry{}, 0, fmt.Errorf("%w: incomplete oversized audit: %w", ErrWALCorrupted, err)
	}
	if inner.Sum32() != entry.Checksum {
		return WALEntry{}, 0, fmt.Errorf("%w: oversized audit data checksum", ErrWALChecksumFailed)
	}
	if _, err := io.ReadFull(reader, word[:]); err != nil {
		return WALEntry{}, 0, fmt.Errorf("%w: oversized audit checksum: %w", ErrWALCorrupted, err)
	}
	if binary.LittleEndian.Uint32(word[:]) != outer.Sum32() {
		return WALEntry{}, 0, fmt.Errorf("%w: oversized audit frame checksum", ErrWALChecksumFailed)
	}
	var trailer [8]byte
	if _, err := io.ReadFull(reader, trailer[:]); err != nil {
		return WALEntry{}, 0, fmt.Errorf("%w: oversized audit trailer: %w", ErrWALCorrupted, err)
	}
	if binary.LittleEndian.Uint64(trailer[:]) != walTrailer {
		return WALEntry{}, 0, fmt.Errorf("%w: invalid oversized audit trailer", ErrWALCorrupted)
	}
	rawLength := int64(9) + int64(size) + 4 + 8
	length := alignUp(rawLength)
	var padding [7]byte
	if _, err := io.ReadFull(reader, padding[:length-rawLength]); err != nil {
		return WALEntry{}, 0, fmt.Errorf("%w: oversized audit padding: %w", ErrWALCorrupted, err)
	}
	entry.Data = []byte(walOversizedAuditOmission)
	entry.Checksum = crc32Checksum(entry.Data)
	entry.framingVersion = walFormatVersionCurrent
	return entry, length, nil
}
