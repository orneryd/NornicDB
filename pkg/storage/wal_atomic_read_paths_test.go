package storage

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func mustWalEntryPayload(t *testing.T, seq uint64, op OperationType, data []byte, checksum uint32) []byte {
	t.Helper()
	entry := WALEntry{
		Sequence:  seq,
		Timestamp: time.Unix(0, int64(seq)).UTC(),
		Operation: op,
		Data:      data,
		Checksum:  checksum,
	}
	payload, err := json.Marshal(entry)
	require.NoError(t, err)
	return payload
}

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
	good1 := buildAtomicRecordRaw(good1Payload, 1, crc32Checksum(good1Payload), 0, false)

	embData := []byte(`{"node":{"id":"n1"},"embedding":[0.1,0.2]}`)
	embPayload := mustWalEntryPayload(t, 2, OpUpdateEmbedding, embData, crc32Checksum(embData))
	badEmbedding := buildAtomicRecordRaw(embPayload, 1, crc32Checksum(embPayload)+1, 0, false)

	good2Data := []byte(`{"node":{"id":"n2"}}`)
	good2Payload := mustWalEntryPayload(t, 3, OpCreateNode, good2Data, crc32Checksum(good2Data))
	good2 := buildAtomicRecordRaw(good2Payload, 1, crc32Checksum(good2Payload), 0, false)

	require.NoError(t, os.WriteFile(path, append(append(good1, badEmbedding...), good2...), 0644))

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	entries, err := readAtomicWALEntries(f, nil)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Equal(t, uint64(1), entries[0].Sequence)
	require.Equal(t, uint64(3), entries[1].Sequence)
}

func TestReadAtomicWALEntries_DetectsVersionLengthAndDataChecksumErrors(t *testing.T) {
	t.Run("unsupported_version", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "wal.log")
		data := []byte(`{"node":{"id":"n1"}}`)
		payload := mustWalEntryPayload(t, 1, OpCreateNode, data, crc32Checksum(data))
		rec := buildAtomicRecordRaw(payload, walFormatVersionPropertyEpoch+1, crc32Checksum(payload), walTrailer, true)
		require.NoError(t, os.WriteFile(path, rec, 0644))

		f, err := os.Open(path)
		require.NoError(t, err)
		defer f.Close()

		_, err = readAtomicWALEntries(f, nil)
		require.Error(t, err)
		require.ErrorContains(t, err, "unsupported WAL version")
	})

	t.Run("payload_too_large", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "wal.log")
		header := make([]byte, 9)
		binary.LittleEndian.PutUint32(header[0:4], walMagic)
		header[4] = walFormatVersion
		binary.LittleEndian.PutUint32(header[5:9], walMaxEntrySize+1)
		require.NoError(t, os.WriteFile(path, header, 0644))

		f, err := os.Open(path)
		require.NoError(t, err)
		defer f.Close()

		_, err = readAtomicWALEntries(f, nil)
		require.Error(t, err)
		require.ErrorContains(t, err, "exceeds maximum")
	})

	t.Run("data_checksum_mismatch_non_embedding_fails", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "wal.log")
		data := []byte(`{"node":{"id":"n1"}}`)
		payload := mustWalEntryPayload(t, 1, OpCreateNode, data, crc32Checksum(data)+1)
		rec := buildAtomicRecordRaw(payload, 2, crc32Checksum(payload), walTrailer, true)
		require.NoError(t, os.WriteFile(path, rec, 0644))

		f, err := os.Open(path)
		require.NoError(t, err)
		defer f.Close()

		_, err = readAtomicWALEntries(f, nil)
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

		f, err := os.Open(path)
		require.NoError(t, err)
		defer f.Close()

		entries, err := readAtomicWALEntries(f, nil)
		require.NoError(t, err)
		require.Len(t, entries, 1)
		require.Equal(t, uint64(1), entries[0].Sequence)
	})
}

func TestReadAtomicWALEntries_PartialTrailerAndPaddingHandledAsTail(t *testing.T) {
	t.Run("partial_trailer", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "wal.log")
		data := []byte(`{"node":{"id":"n1"}}`)
		payload := mustWalEntryPayload(t, 1, OpCreateNode, data, crc32Checksum(data))
		rec := buildAtomicRecordRaw(payload, 2, crc32Checksum(payload), walTrailer, true)
		// remove a few trailer/padding bytes to force partial tail detection
		rec = rec[:len(rec)-3]
		require.NoError(t, os.WriteFile(path, rec, 0644))

		f, err := os.Open(path)
		require.NoError(t, err)
		defer f.Close()

		entries, err := readAtomicWALEntries(f, nil)
		require.NoError(t, err)
		require.Len(t, entries, 0)
	})

	t.Run("missing_padding_after_valid_record", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "wal.log")
		data := []byte(`{"node":{"id":"n1"}}`)
		payload := mustWalEntryPayload(t, 1, OpCreateNode, data, crc32Checksum(data))
		recNoPadding := buildAtomicRecordRaw(payload, 2, crc32Checksum(payload), walTrailer, false)
		require.NoError(t, os.WriteFile(path, recNoPadding, 0644))

		f, err := os.Open(path)
		require.NoError(t, err)
		defer f.Close()

		_, err = readAtomicWALEntries(f, nil)
		require.Error(t, err)
		require.ErrorContains(t, err, "failed to read padding")
	})
}

func TestReadAtomicWALEntriesForTruncation_SkipsMalformedAndFiltersAfterSeq(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	good1Data := []byte(`{"node":{"id":"n1"}}`)
	good1Payload := mustWalEntryPayload(t, 1, OpCreateNode, good1Data, crc32Checksum(good1Data))
	good1 := buildAtomicRecordRaw(good1Payload, 2, crc32Checksum(good1Payload), walTrailer, true)

	badJSONPayload := []byte(`{"sequence":2,"operation":"CREATE_NODE"`) // malformed JSON
	badJSON := buildAtomicRecordRaw(badJSONPayload, 2, crc32Checksum(badJSONPayload), walTrailer, true)

	good2Data := []byte(`{"node":{"id":"n3"}}`)
	good2Payload := mustWalEntryPayload(t, 3, OpCreateNode, good2Data, crc32Checksum(good2Data))
	good2 := buildAtomicRecordRaw(good2Payload, 2, crc32Checksum(good2Payload), walTrailer, true)

	require.NoError(t, os.WriteFile(path, append(append(good1, badJSON...), good2...), 0644))

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	entries, err := readAtomicWALEntriesForTruncation(f, 1)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, uint64(3), entries[0].Sequence)
}
