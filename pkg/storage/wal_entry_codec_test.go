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

func TestWALEntryCodecV3RoundTrip(t *testing.T) {
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
	payload, err := encodeWALEntryV3(&buf, &entry)
	require.NoError(t, err)

	decoded, err := decodeWALEntry(payload, walFormatVersionV3)
	require.NoError(t, err)
	require.Equal(t, entry.Sequence, decoded.Sequence)
	require.Equal(t, entry.Operation, decoded.Operation)
	require.Equal(t, entry.Database, decoded.Database)
	require.Equal(t, entry.Checksum, decoded.Checksum)
	require.Equal(t, entry.Data, decoded.Data, "Data must round-trip byte-for-byte")
	require.Equal(t, entry.Timestamp.UnixNano(), decoded.Timestamp.UnixNano())
}

func TestWALEntryCodecV3SkipsBase64Tax(t *testing.T) {
	// Large, highly-entropic Data (worst case for base64): 100KB of random
	// bytes. v2 stores it as base64 JSON; v3 stores it raw.
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

	v2Payload, err := json.Marshal(entry)
	require.NoError(t, err)

	var buf bytes.Buffer
	v3Payload, err := encodeWALEntryV3(&buf, &entry)
	require.NoError(t, err)

	// v3 must be smaller than v2 by at least the base64 expansion (~25% of
	// the raw data: 100KB -> ~133KB base64). Assert conservatively.
	require.Less(t, len(v3Payload), len(v2Payload), "v3 must drop the base64 expansion")
	require.Less(t, len(v3Payload), len(data)+len(v2Payload)/4)
}

func TestWALMixedV2AndV3RecordsReadBack(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	data := []byte(`{"node":{"id":"n1"}}`)
	checksum := crc32Checksum(data)

	// v2 record: full JSON entry with base64 Data.
	v2Entry := WALEntry{Sequence: 1, Timestamp: time.Now().UTC(), Operation: OpCreateNode, Data: data, Checksum: checksum}
	v2JSON, err := json.Marshal(v2Entry)
	require.NoError(t, err)
	v2Rec, v2Len := buildAtomicRecord(v2JSON, walFormatVersion)

	// v3 record: raw-Data payload.
	v3Entry := WALEntry{Sequence: 2, Timestamp: time.Now().UTC(), Operation: OpCreateNode, Data: data, Checksum: checksum}
	var buf bytes.Buffer
	v3Payload, err := encodeWALEntryV3(&buf, &v3Entry)
	require.NoError(t, err)
	v3Rec, v3Len := buildAtomicRecord(v3Payload, walFormatVersionV3)

	combined := append(append([]byte(nil), v2Rec...), v3Rec...)
	require.Equal(t, int(v2Len)+int(v3Len), len(combined))
	require.NoError(t, os.WriteFile(path, combined, 0644))

	entries, err := readWALEntriesForTruncation(path, 0)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Equal(t, uint64(1), entries[0].Sequence)
	require.Equal(t, data, entries[0].Data)
	require.Equal(t, uint64(2), entries[1].Sequence)
	require.Equal(t, data, entries[1].Data, "v3 Data must decode byte-for-byte")

	// Also verify the full ReadWALEntries path.
	all, err := ReadWALEntries(path)
	require.NoError(t, err)
	require.Len(t, all, 2)
	require.Equal(t, data, all[1].Data)
}

func TestWALV3EndToEndViaAppend(t *testing.T) {
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
	require.Equal(t, byte(walFormatVersionV3), raw[4], "Append must write v3 records")

	entries, err := ReadWALEntries(filepath.Join(dir, "wal.log"))
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, OpCreateNode, entries[0].Operation)
	require.NotNil(t, entries[0].Data)

	// The appended node JSON must decode back.
	var nodeData WALNodeData
	require.NoError(t, json.Unmarshal(entries[0].Data, &nodeData))
	require.Equal(t, "n1", string(nodeData.Node.ID))
}
