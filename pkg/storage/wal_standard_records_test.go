package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// NOR-412: a WAL and an engine opened without naming a record policy write
// the records every release before NOR-412 wrote, and refuse what those
// releases refused, with the same error at the same point. This file uses
// only API that predates NOR-412, so the same tests run unchanged on that
// code: its fixture was captured there, and its refusal and reader
// expectations pass there.

// standardRecordsWAL opens a WAL as every caller that names no policy does.
func standardRecordsWAL(t *testing.T, dir string) *WAL {
	t.Helper()
	t.Cleanup(config.WithWALEnabled())
	w, err := NewWAL(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func standardRecordsBytes(n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte('a' + i%23)
	}
	return out
}

type standardRecordsFrame struct {
	Seq     uint64 `json:"seq"`
	Op      string `json:"op"`
	Version uint8  `json:"version"`
	Length  int    `json:"length"`
	SHA256  string `json:"sha256"`
	raw     []byte
}

// standardRecordsFrames parses every physical frame of one WAL file.
func standardRecordsFrames(t *testing.T, path string) []standardRecordsFrame {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var frames []standardRecordsFrame
	for offset := int64(0); offset < int64(len(raw)); {
		require.GreaterOrEqual(t, int64(len(raw))-offset, int64(9), "frame header at %d", offset)
		require.Equal(t, walMagic, binary.LittleEndian.Uint32(raw[offset:]))
		version := raw[offset+4]
		size := int64(binary.LittleEndian.Uint32(raw[offset+5:]))
		end := offset + alignUp(9+size+4+8)
		require.LessOrEqual(t, end, int64(len(raw)))
		var entry WALEntry
		_, err := processWALEntryPayload(walPayloadDecode, nil, &entry, raw[offset+9:offset+9+size], version)
		require.NoError(t, err)
		sum := sha256.Sum256(raw[offset:end])
		frames = append(frames, standardRecordsFrame{Seq: entry.Sequence, Op: string(entry.Operation), Version: version, Length: int(end - offset), SHA256: hex.EncodeToString(sum[:]), raw: raw[offset:end]})
		offset = end
	}
	return frames
}

// standardRecordsHeaderLen is the length of the entry header a record with
// this metadata is written with.
func standardRecordsHeaderLen(t *testing.T, entry WALEntry) int {
	t.Helper()
	header, err := marshalJSONCompact(&bytes.Buffer{}, &WALEntry{Sequence: entry.Sequence, Timestamp: entry.Timestamp, Operation: entry.Operation, Checksum: entry.Checksum, Database: entry.Database})
	require.NoError(t, err)
	return len(header)
}

// standardRecordsExactFit returns data that makes the record with this
// metadata exactly one frame long (the header carries the data's checksum,
// whose decimal length varies, so the size is found by iteration).
func standardRecordsExactFit(t *testing.T, entry WALEntry) []byte {
	t.Helper()
	size := int(walMaxEntrySize) - 8 - 96
	for attempt := 0; attempt < 64; attempt++ {
		data := standardRecordsBytes(size)
		data[0] = byte('A' + attempt%26)
		entry.Checksum = crc32Checksum(data)
		want := int(walMaxEntrySize) - 8 - standardRecordsHeaderLen(t, entry)
		if want == size {
			return data
		}
		size = want
	}
	t.Fatal("no data length makes the record exactly one frame")
	return nil
}

var standardRecordsTime = time.Date(2026, 11, 11, 10, 20, 30, 123456789, time.UTC)

// standardRecordsSemanticPayload is one transaction's semantic record with
// fixed metadata: a node created with managed and named vectors, the same
// node updated with its whole image (vectors unchanged), and a delete. Every
// map has one key, so its encoding has one order.
func standardRecordsSemanticPayload(t *testing.T) []byte {
	t.Helper()
	image := func(property string, value any, updated time.Time) *Node {
		return &Node{
			ID: "test:a", Labels: []string{"Doc"}, Properties: map[string]any{property: value},
			ChunkEmbeddings: [][]float32{{0.5, 1.5, -2.25}, {3.5, 4.75, 5.125}},
			NamedEmbeddings: map[string][]float32{"title": {1, 2, 3}},
			EmbedMeta:       map[string]any{"has_embedding": true},
			CreatedAt:       standardRecordsTime, UpdatedAt: updated,
		}
	}
	payload, err := EncodeEntityTransaction(EntityTransactionRequest{Namespace: "test", LogicalID: "standard-records-fixture", Mode: CommitModeExplicit, Operations: []Operation{
		{Type: OpCreateNode, NodeID: "test:a", Node: image("text", "alpha", standardRecordsTime)},
		{Type: OpUpdateNode, NodeID: "test:a", Node: image("flag", true, standardRecordsTime.Add(time.Second))},
		{Type: OpDeleteNode, NodeID: "test:b"},
	}})
	require.NoError(t, err)
	return payload
}

type standardRecordsFixture struct {
	SemanticPayloadHex string                 `json:"semantic_payload_hex"`
	SmallFrameHex      string                 `json:"small_frame_hex"`
	Frames             []standardRecordsFrame `json:"frames"`
}

// The bytes a default WAL writes for fixed records equal the bytes captured
// from the code before NOR-412: the semantic record of a transaction that
// updates a node whose vectors it keeps (whole image, version 3), and the
// frames the batch writer puts on disk for that record, for a small record
// and for a record of exactly one frame.
func TestStandardRecordsMatchCapturedBytes(t *testing.T) {
	semantic := standardRecordsSemanticPayload(t)
	envelope, ops, err := DecodeEntityTransaction(semantic)
	require.NoError(t, err)
	require.Equal(t, uint16(3), envelope.Version)
	require.Len(t, ops[1].Node.ChunkEmbeddings, 2, "the update's image carries its vectors")
	require.Len(t, ops[1].Node.NamedEmbeddings, 1)

	dir := t.TempDir()
	w := standardRecordsWAL(t, dir)
	batch := w.NewBatch()
	queue := func(op OperationType, data []byte, at time.Time) {
		batch.pending = append(batch.pending, pendingEntry{Operation: op, Data: data, Checksum: crc32Checksum(data), Timestamp: at})
	}
	commitData, err := marshalWALRecoveryPayload(nil, walEntityCommitData{Payload: semantic})
	require.NoError(t, err)
	queue(OpEntityCommit, commitData, standardRecordsTime)
	queue(OperationType("fixture_small"), []byte(`{"k":"v"}`), standardRecordsTime.Add(time.Second))
	exact := WALEntry{Sequence: 3, Timestamp: standardRecordsTime.Add(2 * time.Second), Operation: OperationType("fixture_exact")}
	queue(exact.Operation, standardRecordsExactFit(t, exact), exact.Timestamp)
	first, last, err := batch.CommitWithSeq()
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 3}, []uint64{first, last})
	require.NoError(t, w.Close())

	got := standardRecordsFixture{SemanticPayloadHex: hex.EncodeToString(semantic)}
	for _, frame := range standardRecordsFrames(t, walActivePath(dir)) {
		if frame.Op != string(OpEntityCommit) && !strings.HasPrefix(frame.Op, "fixture_") {
			continue // control records carry per-file identities
		}
		require.Equal(t, uint8(6), frame.Version, "seq %d", frame.Seq)
		if frame.Op == "fixture_small" {
			got.SmallFrameHex = hex.EncodeToString(frame.raw)
		}
		if frame.Op == "fixture_exact" {
			require.Equal(t, int(alignUp(9+int64(walMaxEntrySize)+4+8)), frame.Length, "a record of exactly one frame")
		}
		got.Frames = append(got.Frames, frame)
	}
	require.Len(t, got.Frames, 3)
	encoded, err := json.MarshalIndent(got, "", "  ")
	require.NoError(t, err)

	captured, err := os.ReadFile("testdata/wal_standard_records.json")
	if os.IsNotExist(err) {
		t.Fatalf("no captured fixture; this run produced:\nCAPTURE-BEGIN\n%s\nCAPTURE-END", encoded)
	}
	require.NoError(t, err)
	var want standardRecordsFixture
	require.NoError(t, json.Unmarshal(captured, &want))
	require.Equal(t, want.SemanticPayloadHex, got.SemanticPayloadHex, "semantic record bytes")
	require.Equal(t, want.SmallFrameHex, got.SmallFrameHex, "small frame bytes")
	require.Equal(t, len(want.Frames), len(got.Frames))
	for i := range want.Frames {
		want.Frames[i].raw, got.Frames[i].raw = nil, nil
		require.Equal(t, want.Frames[i], got.Frames[i], "frame %d", i)
	}
}

const standardRecordsFrameError = "wal: entry exceeds maximum WAL frame size of 16777216 bytes"

// A default WAL refuses a record that does not fit one frame, with the frame
// error, on the ordinary append and in a batch; the refusal consumes the
// record's sequence number, leaves what was written before it, and the WAL
// keeps appending. One byte decides at the frame boundary.
func TestStandardRecordsRefusals(t *testing.T) {
	dir := t.TempDir()
	w := standardRecordsWAL(t, dir)
	large := walEntityCommitData{Payload: standardRecordsBytes(int(walMaxEntrySize) + 1024)}
	small := walEntityCommitData{Payload: []byte("small")}
	sequences := func() []uint64 {
		t.Helper()
		require.NoError(t, w.Sync())
		entries, err := ReadWALEntriesFromDir(t.Context(), dir)
		require.NoError(t, err)
		var out []uint64
		for _, entry := range entries {
			out = append(out, entry.Sequence)
		}
		return out
	}

	_, err := w.AppendWithDatabaseReturningSeq(OpEntityCommit, large, "test")
	require.EqualError(t, err, "wal: failed to serialize entry: "+standardRecordsFrameError)
	var rejection *walRecordRejection
	require.True(t, errors.As(err, &rejection), "refused before anything is written")
	require.EqualValues(t, 1, w.Sequence(), "the refused record consumed its sequence")
	seq, err := w.AppendWithDatabaseReturningSeq(OpEntityCommit, small, "test")
	require.NoError(t, err)
	require.EqualValues(t, 2, seq)
	require.Equal(t, []uint64{2}, sequences())

	// A header that alone exceeds the frame is refused the same way.
	_, err = w.AppendWithDatabaseReturningSeq(OpEntityCommit, small, strings.Repeat("d", int(walMaxEntrySize)))
	require.EqualError(t, err, "wal: failed to serialize entry: "+standardRecordsFrameError)
	require.EqualValues(t, 3, w.Sequence())

	// A batch whose second record does not fit: the first is written, the
	// second consumes its sequence and fails the batch, the third never runs.
	batch := w.NewBatch()
	for _, data := range []walEntityCommitData{small, large, small} {
		require.NoError(t, batch.append(OpEntityCommit, data))
	}
	_, _, err = batch.CommitWithSeq()
	require.EqualError(t, err, "wal batch: failed to serialize entry seq 5: "+standardRecordsFrameError)
	require.EqualValues(t, 5, w.Sequence())
	require.Equal(t, []uint64{2, 4}, sequences())
	seq, err = w.AppendWithDatabaseReturningSeq(OpEntityCommit, small, "test")
	require.NoError(t, err)
	require.EqualValues(t, 6, seq)

	// The frame boundary, with fixed metadata through the batch writer.
	at := WALEntry{Sequence: 7, Timestamp: standardRecordsTime, Operation: OpEntityCommit}
	fit := standardRecordsExactFit(t, at)
	batch = w.NewBatch()
	batch.pending = append(batch.pending, pendingEntry{Operation: at.Operation, Data: fit, Checksum: crc32Checksum(fit), Timestamp: at.Timestamp})
	first, _, err := batch.CommitWithSeq()
	require.NoError(t, err)
	require.EqualValues(t, 7, first)
	// One more byte; its value is chosen so the new checksum does not make
	// the header shorter, which would let the longer record fit again.
	at.Sequence = 8
	var over []byte
	for last := byte('a'); last <= 'z' && over == nil; last++ {
		candidate := append(append([]byte(nil), fit...), last)
		at.Checksum = crc32Checksum(candidate)
		if len(candidate) > int(walMaxEntrySize)-8-standardRecordsHeaderLen(t, at) {
			over = candidate
		}
	}
	require.NotNil(t, over, "one byte over the frame")
	batch = w.NewBatch()
	batch.pending = append(batch.pending, pendingEntry{Operation: at.Operation, Data: over, Checksum: at.Checksum, Timestamp: at.Timestamp})
	_, _, err = batch.CommitWithSeq()
	require.EqualError(t, err, "wal batch: failed to serialize entry seq 8: "+standardRecordsFrameError)
	require.Equal(t, []uint64{2, 4, 6, 7}, sequences())
	for _, frame := range standardRecordsFrames(t, walActivePath(dir)) {
		require.Equal(t, uint8(6), frame.Version, "seq %d", frame.Seq)
	}
}

// An ordinary record whose operation happens to be called "record_part" is
// an ordinary record: it is read, counted and kept by every reader, a reopen
// repairs nothing and continues the sequence after it, and a snapshot rewrite
// retains it.
func TestOrdinaryRecordNamedRecordPartIsAnOrdinaryRecord(t *testing.T) {
	dir := t.TempDir()
	w := standardRecordsWAL(t, dir)
	named := OperationType("record_part")
	seq, err := w.AppendWithDatabaseReturningSeq(named, map[string]any{"k": "v"}, "test")
	require.NoError(t, err)
	require.EqualValues(t, 1, seq)
	_, err = w.AppendWithDatabaseReturningSeq(OpEntityCommit, walEntityCommitData{Payload: []byte("after")}, "test")
	require.NoError(t, err)
	require.NoError(t, w.Close())
	active := walActivePath(dir)
	before, err := os.ReadFile(active)
	require.NoError(t, err)

	operations := func(entries []WALEntry) []OperationType {
		var out []OperationType
		for _, entry := range entries {
			out = append(out, entry.Operation)
		}
		return out
	}
	want := []OperationType{named, OpEntityCommit}
	entries, err := ReadWALEntries(t.Context(), active, nil)
	require.NoError(t, err)
	require.Equal(t, want, operations(entries))
	report, err := CheckWALIntegrity(active)
	require.NoError(t, err)
	require.True(t, report.Healthy, "%v", report.Errors)
	require.Equal(t, 2, report.TotalEntries)
	salvaged, err := readWALEntriesForTruncation(active, 0)
	require.NoError(t, err)
	require.Equal(t, want, operations(salvaged))
	repaired, _, err := repairWALTailIfNeeded(active, nil)
	require.NoError(t, err)
	require.False(t, repaired)

	w = standardRecordsWAL(t, dir)
	after, err := os.ReadFile(active)
	require.NoError(t, err)
	require.Equal(t, before, after, "opening the WAL changes nothing in it")
	require.EqualValues(t, 2, w.Sequence())
	seq, err = w.AppendWithDatabaseReturningSeq(OpEntityCommit, walEntityCommitData{Payload: []byte("later")}, "test")
	require.NoError(t, err)
	require.EqualValues(t, 3, seq, "the next record continues the sequence")

	require.NoError(t, w.ManageSnapshot(t.Context(), 0, WALSnapshotTruncate))
	require.NoError(t, w.Close())
	entries, err = ReadWALEntriesFromDir(t.Context(), dir)
	require.NoError(t, err)
	require.Equal(t, []OperationType{named, OpEntityCommit, OpEntityCommit}, operations(entries), "a snapshot rewrite keeps it")
}
