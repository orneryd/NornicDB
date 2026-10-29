package storage

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/orneryd/nornicdb/pkg/config"
)

func TestWALEntryPayloadRejectsUnknownModeWithoutMutation(t *testing.T) {
	entry := WALEntry{Sequence: 42, Data: []byte("unchanged")}
	buffer := bytes.NewBufferString("unchanged")
	result, err := processWALEntryPayload(walPayloadMode(255), buffer, &entry, []byte("unused"), walFormatVersionCurrent)
	require.ErrorContains(t, err, "unknown entry payload mode")
	require.Nil(t, result)
	require.Equal(t, uint64(42), entry.Sequence)
	require.Equal(t, []byte("unchanged"), entry.Data)
	require.Equal(t, "unchanged", buffer.String())
}

func TestWALEntryPayloadClearsEntryOnDecodeFailure(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload []byte
		version uint8
	}{
		{name: "unsupported version", version: 0},
		{name: "short payload", version: walFormatVersionCurrent},
		{name: "invalid header JSON", payload: []byte{1, 0, 0, 0, '{', 0, 0, 0, 0}, version: walFormatVersionCurrent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry := WALEntry{Sequence: 42, Data: []byte("stale")}
			result, err := processWALEntryPayload(walPayloadDecode, nil, &entry, tc.payload, tc.version)
			require.Error(t, err)
			require.Nil(t, result)
			require.Equal(t, WALEntry{}, entry)
		})
	}
}

// The raw-Data payload codec wrote v3 records when it was introduced. The
// current prototype kept that payload layout but moved the frame to
// walFormatVersionCurrent and refuses every older frame, so these tests build
// and read their fixtures with the current version only.

func TestWALEntryCodecRawPayloadRoundTrip(t *testing.T) {
	// Data with every byte value + the zstd magic so we prove the raw Data
	// field is binary-safe end to end (base64 would have mangled none of it,
	// but the raw layout must reproduce it byte-for-byte).
	data := make([]byte, 0, 1024)
	for i := 0; i < 1024; i++ {
		data = append(data, byte(i))
	}
	data[0], data[1], data[2], data[3] = 0x28, 0xB5, 0x2F, 0xFD

	entry := WALEntry{
		Sequence:  7,
		Timestamp: time.Now().UTC(),
		Operation: OpBufferedCommit,
		Data:      data,
		Checksum:  crc32Checksum(data),
		Database:  "bench",
	}

	var buf bytes.Buffer
	payload, err := processWALEntryPayload(walPayloadEncode, &buf, &entry, nil, 0)
	require.NoError(t, err)

	decoded, err := WALEntry{}, error(nil)
	_, err = processWALEntryPayload(walPayloadDecode, nil, &decoded, payload, walFormatVersionCurrent)
	require.NoError(t, err)
	require.Equal(t, entry.Sequence, decoded.Sequence)
	require.Equal(t, entry.Operation, decoded.Operation)
	require.Equal(t, entry.Database, decoded.Database)
	require.Equal(t, entry.Checksum, decoded.Checksum)
	require.Equal(t, entry.Data, decoded.Data, "Data must round-trip byte-for-byte")
	require.Equal(t, entry.Timestamp.UnixNano(), decoded.Timestamp.UnixNano())
	require.Equal(t, walFormatVersionCurrent, decoded.framingVersion, "decoded entries must carry the frame they came from")
}

func TestWALEntryCodecRawPayloadSkipsBase64Tax(t *testing.T) {
	// Large, highly-entropic Data (worst case for base64): 100KB of patterned
	// bytes. The superseded whole-entry JSON payload stored it as base64; the
	// current payload stores it raw.
	data := make([]byte, 100*1024)
	for i := range data {
		data[i] = byte((i * 31) % 256)
	}

	entry := WALEntry{
		Sequence:  1,
		Timestamp: time.Now().UTC(),
		Operation: OpCreateNode,
		Data:      data,
		Checksum:  crc32Checksum(data),
	}

	base64Payload, err := json.Marshal(entry)
	require.NoError(t, err)

	var buf bytes.Buffer
	rawPayload, err := processWALEntryPayload(walPayloadEncode, &buf, &entry, nil, 0)
	require.NoError(t, err)

	// The current payload must be smaller than the base64 layout by at least
	// the base64 expansion (~25% of the raw data: 100KB -> ~133KB base64).
	// Assert conservatively.
	require.Less(t, len(rawPayload), len(base64Payload), "the current payload must drop the base64 expansion")
	require.Less(t, len(rawPayload), len(data)+len(base64Payload)/4)
}

func TestWALRefusesSupersededCodecRecordsWithoutChangingFiles(t *testing.T) {
	data := []byte(`{"node":{"id":"n1"}}`)
	checksum := crc32Checksum(data)
	entry := WALEntry{Sequence: 1, Timestamp: time.Now().UTC(), Operation: OpCreateNode, Data: data, Checksum: checksum}

	// Build the payload each superseded writer produced: v1/v2 stored the whole
	// entry as JSON with base64 Data, v3 stored the raw-Data payload.
	jsonPayload, err := json.Marshal(entry)
	require.NoError(t, err)
	var buf bytes.Buffer
	rawPayload, err := processWALEntryPayload(walPayloadEncode, &buf, &entry, nil, 0)
	require.NoError(t, err)

	cases := []struct {
		name    string
		version uint8
		payload []byte
	}{
		{name: "v1-json", version: 1, payload: jsonPayload},
		{name: "v2-json", version: walFormatVersion, payload: jsonPayload},
		{name: "v3-raw", version: walFormatVersionV3, payload: rawPayload},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.NotEqual(t, walFormatVersionCurrent, tc.version)

			// The decoder refuses the payload by version alone.
			_, err := processWALEntryPayload(walPayloadDecode, nil, &WALEntry{}, tc.payload, tc.version)
			require.ErrorIs(t, err, ErrWALUnsupportedVersion)

			// A complete record is refused by both readers and left untouched.
			record, _ := buildAtomicRecord(tc.payload, tc.version)
			path := filepath.Join(t.TempDir(), "wal.log")
			require.NoError(t, os.WriteFile(path, record, 0644))

			all, err := ReadWALEntries(path, nil)
			require.ErrorIs(t, err, ErrWALUnsupportedVersion)
			require.Empty(t, all)

			salvaged, err := readWALEntriesForTruncation(path, 0)
			require.ErrorIs(t, err, ErrWALUnsupportedVersion)
			require.Empty(t, salvaged)

			after, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, record, after, "an unsupported frame is refused, never rewritten")
		})
	}

	t.Run("payload-layout-mismatch-in-current-frame", func(t *testing.T) {
		// A base64 JSON payload inside the current frame is not a torn record:
		// it is a payload the current codec cannot decode, and is reported.
		_, err := processWALEntryPayload(walPayloadDecode, nil, &WALEntry{}, jsonPayload, walFormatVersionCurrent)
		require.Error(t, err)
	})
}

func TestWALCurrentPayloadRecordsReadBack(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	data := []byte(`{"node":{"id":"n1"}}`)
	checksum := crc32Checksum(data)

	var buf bytes.Buffer
	payload, err := processWALEntryPayload(walPayloadEncode, &buf, &WALEntry{Sequence: 2, Timestamp: time.Now().UTC(), Operation: OpCreateNode, Data: data, Checksum: checksum}, nil, 0)
	require.NoError(t, err)
	rec, recLen := buildAtomicRecord(payload, walFormatVersionCurrent)
	require.NoError(t, os.WriteFile(path, rec, 0644))
	require.Equal(t, int(recLen), len(rec), "a current record is exactly its aligned length")

	entries, err := readWALEntriesForTruncation(path, 0)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, uint64(2), entries[0].Sequence)
	require.Equal(t, data, entries[0].Data, "raw Data must decode byte-for-byte")

	// Also verify the full ReadWALEntries path.
	all, err := ReadWALEntries(path, nil)
	require.NoError(t, err)
	require.Len(t, all, 1)
	require.Equal(t, data, all[0].Data)
	require.Equal(t, walFormatVersionCurrent, all[0].framingVersion)
}

func TestWALCurrentFrameEndToEndViaAppend(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()
	wal, err := NewWAL(dir, nil)
	require.NoError(t, err)

	_, err = wal.AppendWithDatabaseReturningSeq(OpCreateNode, WALNodeData{Node: &Node{ID: "n1", Labels: []string{"T"}}}, "bench")
	require.NoError(t, err)
	require.NoError(t, wal.Close())

	raw, err := os.ReadFile(filepath.Join(dir, "wal.log"))
	require.NoError(t, err)
	require.Greater(t, len(raw), 9)
	require.Equal(t, walFormatVersionCurrent, raw[4], "Append must write current-frame records")

	entries, err := ReadWALEntries(filepath.Join(dir, "wal.log"), nil)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, OpCreateNode, entries[0].Operation)
	require.NotNil(t, entries[0].Data)

	// The appended node must decode through the current WAL payload codec.
	var nodeData WALNodeData
	require.NoError(t, decodeRecoveryPayload(entries[0].Data, &nodeData))
	require.Equal(t, "n1", string(nodeData.Node.ID))
}
