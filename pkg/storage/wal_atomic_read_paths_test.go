package storage

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// mustWalEntryPayload encodes an entry the way every current writer does: the
// current frame version carrying the raw-Data payload layout.
func mustWalEntryPayload(t *testing.T, seq uint64, op OperationType, data []byte, checksum uint32) []byte {
	t.Helper()
	entry := WALEntry{
		Sequence:  seq,
		Timestamp: time.Unix(0, int64(seq)).UTC(),
		Operation: op,
		Data:      data,
		Checksum:  checksum,
	}
	payload, err := processWALEntryPayload(walPayloadEncode, &bytes.Buffer{}, &entry, nil, 0)
	require.NoError(t, err)
	return payload
}

// buildAtomicRecordRaw frames a payload with the supplied version, CRC and
// trailer so a test can corrupt exactly one part of an otherwise readable
// record. The current version always carries the trailer and (optionally) the
// alignment padding.
func buildAtomicRecordRaw(payload []byte, version byte, crc uint32, trailer uint64, includePadding bool) []byte {
	header := make([]byte, 9)
	binary.LittleEndian.PutUint32(header[0:4], walMagic)
	header[4] = version
	binary.LittleEndian.PutUint32(header[5:9], uint32(len(payload)))

	record := append([]byte{}, header...)
	record = append(record, payload...)

	crcBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(crcBuf, crc)
	record = append(record, crcBuf...)

	if version >= 2 {
		trailerBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(trailerBuf, trailer)
		record = append(record, trailerBuf...)
		if includePadding {
			rawLen := int64(9 + len(payload) + 4 + 8)
			aligned := alignUp(rawLen)
			pad := int(aligned - rawLen)
			if pad > 0 {
				record = append(record, make([]byte, pad)...)
			}
		}
	}

	return record
}

func TestReadAtomicWALEntries_SkipsCorruptedEmbeddingAndReturnsOthers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	good1Data := []byte(`{"node":{"id":"n1"}}`)
	good1Payload := mustWalEntryPayload(t, 1, OpCreateNode, good1Data, crc32Checksum(good1Data))
	good1 := buildAtomicRecordRaw(good1Payload, walFormatVersionCurrent, crc32Checksum(good1Payload), walTrailer, true)

	embData := []byte(`{"node":{"id":"n1"},"embedding":[0.1,0.2]}`)
	embPayload := mustWalEntryPayload(t, 2, OpUpdateEmbedding, embData, crc32Checksum(embData))
	badEmbedding := buildAtomicRecordRaw(embPayload, walFormatVersionCurrent, crc32Checksum(embPayload)+1, walTrailer, true)

	good2Data := []byte(`{"node":{"id":"n2"}}`)
	good2Payload := mustWalEntryPayload(t, 3, OpCreateNode, good2Data, crc32Checksum(good2Data))
	good2 := buildAtomicRecordRaw(good2Payload, walFormatVersionCurrent, crc32Checksum(good2Payload), walTrailer, true)

	require.NoError(t, os.WriteFile(path, append(append(good1, badEmbedding...), good2...), 0644))

	entries, err := ReadWALEntries(path, nil)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Equal(t, uint64(1), entries[0].Sequence)
	require.Equal(t, good1Data, entries[0].Data)
	require.Equal(t, uint64(3), entries[1].Sequence)
	require.Equal(t, good2Data, entries[1].Data)
}

func TestReadAtomicWALEntries_DetectsVersionLengthAndDataChecksumErrors(t *testing.T) {
	t.Run("unsupported_version", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "wal.log")
		data := []byte(`{"node":{"id":"n1"}}`)
		payload := mustWalEntryPayload(t, 1, OpCreateNode, data, crc32Checksum(data))
		rec := buildAtomicRecordRaw(payload, walFormatVersionCurrent+1, crc32Checksum(payload), walTrailer, true)
		require.NoError(t, os.WriteFile(path, rec, 0644))

		_, err := ReadWALEntries(path, nil)
		require.Error(t, err)
		require.ErrorContains(t, err, "unsupported WAL version")
	})

	t.Run("payload_too_large", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "wal.log")
		header := make([]byte, 9)
		binary.LittleEndian.PutUint32(header[0:4], walMagic)
		header[4] = walFormatVersionCurrent
		binary.LittleEndian.PutUint32(header[5:9], walMaxEntrySize+1)
		require.NoError(t, os.WriteFile(path, header, 0644))

		_, err := ReadWALEntries(path, nil)
		require.Error(t, err)
		require.ErrorContains(t, err, "exceeds maximum")
	})

	t.Run("data_checksum_mismatch_non_embedding_fails", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "wal.log")
		data := []byte(`{"node":{"id":"n1"}}`)
		payload := mustWalEntryPayload(t, 1, OpCreateNode, data, crc32Checksum(data)+1)
		rec := buildAtomicRecordRaw(payload, walFormatVersionCurrent, crc32Checksum(payload), walTrailer, true)
		require.NoError(t, os.WriteFile(path, rec, 0644))

		_, err := ReadWALEntries(path, nil)
		require.Error(t, err)
		require.ErrorContains(t, err, "data checksum mismatch")
	})

	t.Run("data_checksum_mismatch_embedding_is_skipped", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "wal.log")

		goodData := []byte(`{"node":{"id":"n0"}}`)
		good := buildAtomicWALRecord(t, WALEntry{
			Sequence:  1,
			Timestamp: time.Unix(0, 1).UTC(),
			Operation: OpCreateNode,
			Data:      goodData,
			Checksum:  crc32Checksum(goodData),
		})

		embData := []byte(`{"node":{"id":"n0"},"embedding":[1,2,3]}`)
		badEmbedding := buildAtomicWALRecord(t, WALEntry{
			Sequence:  2,
			Timestamp: time.Unix(0, 2).UTC(),
			Operation: OpUpdateEmbedding,
			Data:      embData,
			Checksum:  crc32Checksum(embData) + 1, // inner checksum mismatch
		})

		require.NoError(t, os.WriteFile(path, append(good, badEmbedding...), 0644))

		entries, err := ReadWALEntries(path, nil)
		require.NoError(t, err)
		require.Len(t, entries, 1)
		require.Equal(t, uint64(1), entries[0].Sequence)
		require.Equal(t, goodData, entries[0].Data)
	})
}

func TestReadAtomicWALEntries_PartialTrailerAndPaddingHandledAsTail(t *testing.T) {
	t.Run("partial_trailer", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "wal.log")
		data := []byte(`{"node":{"id":"n1"}}`)
		payload := mustWalEntryPayload(t, 1, OpCreateNode, data, crc32Checksum(data))
		rec := buildAtomicRecordRaw(payload, walFormatVersionCurrent, crc32Checksum(payload), walTrailer, true)
		// Cut inside the trailer, independent of this payload's padding length.
		rec = rec[:9+len(payload)+4+5]
		require.NoError(t, os.WriteFile(path, rec, 0644))

		entries, err := ReadWALEntries(path, nil)
		require.NoError(t, err)
		require.Len(t, entries, 0)
	})

	t.Run("missing_padding_after_valid_record", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "wal.log")
		data := []byte(`{"node":{"id":"n1"}}`)
		payload := mustWalEntryPayload(t, 1, OpCreateNode, data, crc32Checksum(data))
		recNoPadding := buildAtomicRecordRaw(payload, walFormatVersionCurrent, crc32Checksum(payload), walTrailer, false)
		require.NoError(t, os.WriteFile(path, recNoPadding, 0644))

		_, err := ReadWALEntries(path, nil)
		require.Error(t, err)
		require.ErrorContains(t, err, "failed to read padding")
	})
}

func TestReadAtomicWALEntriesForTruncation_SkipsMalformedAndFiltersAfterSeq(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	good1Data := []byte(`{"node":{"id":"n1"}}`)
	good1Payload := mustWalEntryPayload(t, 1, OpCreateNode, good1Data, crc32Checksum(good1Data))
	good1 := buildAtomicRecordRaw(good1Payload, walFormatVersionCurrent, crc32Checksum(good1Payload), walTrailer, true)

	// Truncated JSON header: the payload frame is intact, the entry inside it is
	// not decodable, so truncation must salvage around it.
	badJSONPayload := []byte(`{"seq":2,"op":"CREATE_NODE"`)
	badJSON := buildAtomicRecordRaw(badJSONPayload, walFormatVersionCurrent, crc32Checksum(badJSONPayload), walTrailer, true)

	good2Data := []byte(`{"node":{"id":"n3"}}`)
	good2Payload := mustWalEntryPayload(t, 3, OpCreateNode, good2Data, crc32Checksum(good2Data))
	good2 := buildAtomicRecordRaw(good2Payload, walFormatVersionCurrent, crc32Checksum(good2Payload), walTrailer, true)

	require.NoError(t, os.WriteFile(path, append(append(good1, badJSON...), good2...), 0644))

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	entries, err := readAtomicWALEntriesForTruncation(f, 1)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, uint64(3), entries[0].Sequence)
	require.Equal(t, good2Data, entries[0].Data)
}
