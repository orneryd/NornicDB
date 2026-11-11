package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// b6HistoricalFrame builds one record exactly as the b6d8e762a writer framed
// it (version 3): [headerLen][JSON header with data:null][dataLen][data],
// CRC32-C, trailer and alignment. The b6 writer applied no size limit.
func b6HistoricalFrame(t *testing.T, seq uint64, op OperationType, data []byte) []byte {
	t.Helper()
	header, err := json.Marshal(&WALEntry{Sequence: seq, Timestamp: time.Unix(1_794_000_000+int64(seq), 0).UTC(), Operation: op, Checksum: crc32Checksum(data), Database: "nornic"})
	require.NoError(t, err)
	payload := make([]byte, 4+len(header)+4+len(data))
	binary.LittleEndian.PutUint32(payload, uint32(len(header)))
	copy(payload[4:], header)
	binary.LittleEndian.PutUint32(payload[4+len(header):], uint32(len(data)))
	copy(payload[8+len(header):], data)
	record, _ := buildAtomicRecord(payload, 3)
	return record
}

type b6HistoricalRecord struct {
	op   OperationType
	data []byte
}

// b6OversizedJournal is a journal the old build wrote when a node over 16 MiB
// was created through the storage API and an asynchronous-writes import batch
// over 16 MiB was committed: valid records on both sides of two complete,
// authenticated frames larger than 16 MiB that are not embedding audits.
func b6OversizedJournal(t *testing.T) ([]byte, []b6HistoricalRecord) {
	t.Helper()
	node := func(id, body string) []byte {
		raw, err := json.Marshal(WALNodeData{Node: &Node{ID: NodeID(id), Labels: []string{"Doc"}, Properties: map[string]any{"uid": id, "body": body}}})
		require.NoError(t, err)
		return raw
	}
	// Write-behind commits are zstd-compressed by the old writer; incompressible
	// content (vectors, encoded attachments) keeps the frame over 16 MiB.
	random := make([]byte, 18<<20)
	rand.New(rand.NewSource(7)).Read(random)
	buffered, err := json.Marshal(WALBufferedCommitData{TxID: "import-batch", Namespace: "nornic", Writes: map[string][]byte{"import-batch-value": random}})
	require.NoError(t, err)
	records := []b6HistoricalRecord{
		{OpCreateNode, node("before", "small")},
		{OpCreateNode, node("big", strings.Repeat("x", 18<<20))},
		{OpBufferedCommit, compressWALBufferedCommitPayload(buffered)},
		{OpCreateNode, node("after", "small")},
	}
	var raw bytes.Buffer
	for i, record := range records {
		raw.Write(b6HistoricalFrame(t, uint64(i+1), record.op, record.data))
	}
	require.Greater(t, len(records[1].data), int(walMaxEntrySize))
	require.Greater(t, len(records[2].data), int(walMaxEntrySize))
	return raw.Bytes(), records
}

func walDirDigest(t *testing.T, dir string) map[string][32]byte {
	t.Helper()
	out := map[string][32]byte{}
	require.NoError(t, filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dir, path)
		out[rel] = sha256.Sum256(raw)
		return nil
	}))
	return out
}

// TestV4UpgradeCarriesOversizedHistoricalRecords: an in-place upgrade of an
// old-build journal holding complete records over 16 MiB that are not
// embedding audits converts the journal and keeps every record whole.
func TestV4UpgradeCarriesOversizedHistoricalRecords(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	raw, records := b6OversizedJournal(t)
	dir := filepath.Join(t.TempDir(), "wal")
	require.NoError(t, os.MkdirAll(dir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "wal.log"), raw, 0o600))

	require.NoError(t, ValidateWALFormat(dir, WALValidateV4Upgrade))
	upgraded, err := UpgradeWALV4(dir, WALV4UpgradeOptions{})
	require.NoError(t, err)
	require.True(t, upgraded)
	require.NoError(t, ValidateWALFormat(dir, WALValidateCurrent))

	entries, err := ReadWALEntriesFromDir(t.Context(), dir)
	require.NoError(t, err)
	require.Len(t, entries, len(records))
	for i, entry := range entries {
		require.Equal(t, uint64(i+1), entry.Sequence)
		require.Equal(t, records[i].op, entry.Operation)
		require.True(t, bytes.Equal(records[i].data, entry.Data), "record %d data changed by the upgrade", i+1)
	}
	committed, err := decodeBufferedWALRecord(entries[2])
	require.NoError(t, err)
	require.Equal(t, "import-batch", committed.TxID)
	require.Len(t, committed.Writes["import-batch-value"], 18<<20)

	// Written whole as spanned records: every frame fits the bound.
	converted, err := os.ReadFile(filepath.Join(dir, "wal.log"))
	require.NoError(t, err)
	versions := map[byte]int{}
	for off := int64(0); off < int64(len(converted)); {
		size := binary.LittleEndian.Uint32(converted[off+5:])
		require.LessOrEqual(t, size, walMaxEntrySize)
		versions[converted[off+4]]++
		off += alignUp(9 + int64(size) + 4 + 8)
	}
	require.Positive(t, versions[walFormatVersionSpanned], "the oversized records span frames")
	require.Equal(t, 2, versions[walFormatVersionCurrent], "the small records keep one frame")

	// A WAL with the standard record policy (a cluster member) opens the
	// converted journal without cutting it and reads every record.
	w, err := NewWAL(dir, nil)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	after, err := os.ReadFile(filepath.Join(dir, "wal.log"))
	require.NoError(t, err)
	require.Equal(t, len(converted), len(after), "start-up repair must not cut the carried records")
	entries, err = ReadWALEntriesFromDir(t.Context(), dir)
	require.NoError(t, err)
	require.Len(t, entries, len(records))
}

// TestV4UpgradeOneFrameJournalRefusesOversizedHistoricalRecord: a journal
// kept to one-frame records (the replication journal) refuses an old record
// over one frame, as before, and nothing on disk changes.
func TestV4UpgradeOneFrameJournalRefusesOversizedHistoricalRecord(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	raw, _ := b6OversizedJournal(t)
	parent := t.TempDir()
	dir := filepath.Join(parent, "wal")
	require.NoError(t, os.MkdirAll(dir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "wal.log"), raw, 0o600))
	before := walDirDigest(t, parent)
	_, err := UpgradeWALV4(dir, WALV4UpgradeOptions{OneFrameRecords: true})
	require.ErrorContains(t, err, "exceeds maximum 16777216")
	require.Equal(t, before, walDirDigest(t, parent))
}

// TestV4UpgradeRefusesDamagedOversizedHistoricalRecord: a damaged frame over
// 16 MiB is still refused and nothing on disk changes.
func TestV4UpgradeRefusesDamagedOversizedHistoricalRecord(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, fault := range []string{"payload", "crc", "trailer"} {
		t.Run(fault, func(t *testing.T) {
			raw, _ := b6OversizedJournal(t)
			start := alignUp(9 + int64(binary.LittleEndian.Uint32(raw[5:])) + 4 + 8)
			size := int64(binary.LittleEndian.Uint32(raw[start+5:]))
			switch fault {
			case "payload":
				raw[start+9+size/2] ^= 1
			case "crc":
				raw[start+9+size] ^= 1
			case "trailer":
				raw[start+9+size+4] ^= 1
			}
			parent := t.TempDir()
			dir := filepath.Join(parent, "wal")
			require.NoError(t, os.MkdirAll(dir, 0o700))
			require.NoError(t, os.WriteFile(filepath.Join(dir, "wal.log"), raw, 0o600))
			before := walDirDigest(t, parent)
			require.Error(t, ValidateWALFormat(dir, WALValidateV4Upgrade))
			_, err := UpgradeWALV4(dir, WALV4UpgradeOptions{})
			require.Error(t, err)
			require.Equal(t, before, walDirDigest(t, parent), "a refused upgrade must not change any file")
		})
	}
}
