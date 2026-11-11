package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// NOR-412 round 2: one transaction's journal record is no longer bound to one
// 16 MiB WAL frame. A larger record is written as one spanned record (parts
// in consecutive frames, all with the record's sequence) and every reader
// sees one logical record. These tests drive the WAL owner directly.

type walTestFrame struct {
	version uint8
	seq     uint64
	op      OperationType
	end     int64
}

// walTestFrames parses every physical frame of one WAL file.
func walTestFrames(t *testing.T, path string) []walTestFrame {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var frames []walTestFrame
	for offset := int64(0); offset < int64(len(raw)); {
		require.GreaterOrEqual(t, int64(len(raw))-offset, int64(9), "frame header at %d", offset)
		require.Equal(t, walMagic, binary.LittleEndian.Uint32(raw[offset:]))
		version := raw[offset+4]
		size := int64(binary.LittleEndian.Uint32(raw[offset+5:]))
		payload := raw[offset+9 : offset+9+size]
		var entry WALEntry
		_, err := processWALEntryPayload(walPayloadDecode, nil, &entry, payload, version)
		require.NoError(t, err)
		offset += alignUp(9 + size + 4 + 8)
		frames = append(frames, walTestFrame{version: version, seq: entry.Sequence, op: entry.Operation, end: offset})
	}
	return frames
}

func TestWALSpannedRecordIsOneLogicalRecordForEveryReader(t *testing.T) {
	dir := t.TempDir()
	w := newSpannedTestWAL(t, dir, nil)
	small1, big, small2 := []byte("before"), spannedTestData(40<<20), []byte("after")
	seqSmall1 := appendSpannedTestRecord(t, w, small1)
	seqBig := appendSpannedTestRecord(t, w, big)
	seqSmall2 := appendSpannedTestRecord(t, w, small2)
	require.NoError(t, w.Close())
	active := walActivePath(dir)

	// Physical layout: ordinary frames around three parts with one sequence.
	frames := walTestFrames(t, active)
	require.Len(t, frames, 5)
	require.Equal(t, walFormatVersionCurrent, frames[0].version)
	for _, frame := range frames[1:4] {
		require.Equal(t, walFormatVersionSpanned, frame.version)
		require.Equal(t, OpRecordPart, frame.op)
		require.Equal(t, seqBig, frame.seq)
	}
	require.Equal(t, walFormatVersionCurrent, frames[4].version)

	want := [][]byte{small1, big, small2}
	entries, err := ReadWALEntriesFromDir(t.Context(), dir)
	require.NoError(t, err)
	require.Equal(t, want, spannedTestPayloads(t, entries))
	require.Equal(t, []uint64{seqSmall1, seqBig, seqSmall2}, []uint64{entries[0].Sequence, entries[1].Sequence, entries[2].Sequence})
	require.Equal(t, uint64(frames[3].end), entries[1].recordEnd, "the record ends after its last part")

	var visited []WALEntry
	require.NoError(t, VisitWALEntriesAfterFromDir(t.Context(), dir, seqSmall1, func(entry WALEntry) error {
		visited = append(visited, entry)
		return nil
	}))
	require.Equal(t, want[1:], spannedTestPayloads(t, visited))

	report, err := CheckWALIntegrity(active)
	require.NoError(t, err)
	require.True(t, report.Healthy, "%v", report.Errors)
	require.Equal(t, 3, report.TotalEntries)
	require.Equal(t, seqSmall2, report.LastSeq)
	require.Empty(t, report.Errors)

	require.NoError(t, ValidateWALFormat(dir, WALValidateCurrent))
	repaired, _, err := repairWALTailIfNeeded(active, nil)
	require.NoError(t, err)
	require.False(t, repaired, "a complete spanned record is not a torn tail")
	current, err := walV5OrLaterOnly(dir)
	require.NoError(t, err)
	require.True(t, current, "the upgrade scanner treats spanned frames as current framing")

	salvaged, err := readWALEntriesForTruncation(active, 0)
	require.NoError(t, err)
	require.Equal(t, want, spannedTestPayloads(t, salvaged))

	var sealVisited []WALEntry
	_, err = scanWALPropertyEpochFile(active, func(entry WALEntry) error {
		sealVisited = append(sealVisited, entry)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, want, spannedTestPayloads(t, sealVisited))

	// Reopening finds the last sequence and appends after the record.
	w = newSpannedTestWAL(t, dir, nil)
	defer w.Close()
	require.Equal(t, seqSmall2, w.Sequence())
	require.Greater(t, appendSpannedTestRecord(t, w, []byte("later")), seqSmall2)
}

// A record that fits one frame is written exactly as before; the first byte
// over the frame makes it a two-part record.
func TestWALSingleFrameRecordIsUnchanged(t *testing.T) {
	var buf bytes.Buffer
	entry := WALEntry{Sequence: 7, Timestamp: time.Unix(1700000000, 5).UTC(), Operation: OpEntityCommit, Data: []byte("payload"), Database: "test"}
	entry.Checksum = crc32Checksum(entry.Data)
	before, err := processWALEntryPayload(walPayloadEncode, &buf, &entry, nil, 0)
	require.NoError(t, err)
	frames, err := encodeWALEntryFrames(&buf, &entry)
	require.NoError(t, err)
	require.Equal(t, []walFrame{{payload: before, version: walFormatVersionCurrent}}, frames)

	overhead := len(before) - len(entry.Data)
	entry.Data = spannedTestData(int(walMaxEntrySize) - overhead)
	entry.Checksum = crc32Checksum(entry.Data)
	frames, err = encodeWALEntryFrames(&buf, &entry)
	require.NoError(t, err)
	require.Len(t, frames, 1, "a payload of exactly one frame stays one frame")
	require.Len(t, frames[0].payload, int(walMaxEntrySize))

	entry.Data = append(entry.Data, 1)
	entry.Checksum = crc32Checksum(entry.Data)
	frames, err = encodeWALEntryFrames(&buf, &entry)
	require.NoError(t, err)
	require.Len(t, frames, 2)
	for _, frame := range frames {
		require.Equal(t, walFormatVersionSpanned, frame.version)
		require.LessOrEqual(t, len(frame.payload), int(walMaxEntrySize))
	}
}

// writeSpannedTestParts appends the first keep parts of a spanned record
// straight to a closed WAL's active file: the state a crash leaves after
// those parts were written and before the record's last part.
func writeSpannedTestParts(t *testing.T, path string, seq uint64, data []byte, keep int) (groupStart int64) {
	t.Helper()
	entry := WALEntry{Sequence: seq, Timestamp: time.Now(), Operation: OpEntityCommit, Database: "test"}
	encoded, err := marshalWALRecoveryPayload(nil, walEntityCommitData{Payload: data})
	require.NoError(t, err)
	entry.Data, entry.Checksum = encoded, crc32Checksum(encoded)
	var buf bytes.Buffer
	frames, err := encodeWALEntryFrames(&buf, &entry)
	require.NoError(t, err)
	require.Greater(t, len(frames), keep)
	info, err := os.Stat(path)
	require.NoError(t, err)
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	defer file.Close()
	writer := bufio.NewWriter(file)
	_, err = writeWALEntryFrames(writer, frames[:keep])
	require.NoError(t, err)
	require.NoError(t, writer.Flush())
	return info.Size()
}

// Crash after some parts and before the last one: the record was never
// acknowledged. No reader delivers it, repair removes it whole, and the log
// accepts and replays later records.
func TestWALSpannedRecordWithoutLastPartIsNeverDelivered(t *testing.T) {
	for _, keep := range []int{1, 2} {
		t.Run(fmt.Sprintf("parts%d", keep), func(t *testing.T) {
			dir := t.TempDir()
			w := newSpannedTestWAL(t, dir, nil)
			first := appendSpannedTestRecord(t, w, []byte("acknowledged"))
			require.NoError(t, w.Close())
			active := walActivePath(dir)
			groupStart := writeSpannedTestParts(t, active, first+1, spannedTestData(40<<20), keep)

			entries, err := ReadWALEntries(t.Context(), active, nil)
			require.NoError(t, err)
			require.Equal(t, [][]byte{[]byte("acknowledged")}, spannedTestPayloads(t, entries))
			report, err := CheckWALIntegrity(active)
			require.NoError(t, err)
			require.Equal(t, 1, report.TotalEntries)
			require.Len(t, report.Errors, 1)
			require.Contains(t, report.Errors[0], "incomplete spanned record")

			w = newSpannedTestWAL(t, dir, nil)
			info, err := os.Stat(active)
			require.NoError(t, err)
			require.Equal(t, groupStart, info.Size(), "repair removed every part of the unfinished record")
			later := appendSpannedTestRecord(t, w, []byte("later"))
			require.NoError(t, w.Close())
			entries, err = ReadWALEntriesFromDir(t.Context(), dir)
			require.NoError(t, err)
			require.Equal(t, [][]byte{[]byte("acknowledged"), []byte("later")}, spannedTestPayloads(t, entries))
			require.Equal(t, later, entries[1].Sequence)
		})
	}
}

// A torn write inside a part: repair cuts the whole record, never a prefix.
func TestWALSpannedRecordTornInsideAPart(t *testing.T) {
	for _, cutPart := range []int{0, 1, 2} {
		t.Run(fmt.Sprintf("part%d", cutPart), func(t *testing.T) {
			dir := t.TempDir()
			w := newSpannedTestWAL(t, dir, nil)
			appendSpannedTestRecord(t, w, []byte("acknowledged"))
			appendSpannedTestRecord(t, w, spannedTestData(40<<20))
			require.NoError(t, w.Close())
			active := walActivePath(dir)
			frames := walTestFrames(t, active)
			require.Len(t, frames, 4)
			partStart := frames[cutPart].end
			// Cut in the middle of the chosen part's payload.
			require.NoError(t, os.Truncate(active, partStart+int64(1<<20)))

			entries, err := ReadWALEntries(t.Context(), active, nil)
			require.NoError(t, err)
			require.Equal(t, [][]byte{[]byte("acknowledged")}, spannedTestPayloads(t, entries))

			w = newSpannedTestWAL(t, dir, nil)
			defer w.Close()
			info, err := os.Stat(active)
			require.NoError(t, err)
			require.Equal(t, frames[0].end, info.Size())
			appendSpannedTestRecord(t, w, []byte("later"))
			entries, err = ReadWALEntriesFromDir(t.Context(), dir)
			require.NoError(t, err)
			require.Equal(t, [][]byte{[]byte("acknowledged"), []byte("later")}, spannedTestPayloads(t, entries))
		})
	}
}

// An unfinished group that later records follow (no repair ran) is dropped;
// a part that does not continue its record is corruption, not data.
func TestWALSpannedRecordAssemblerBoundaries(t *testing.T) {
	dir := t.TempDir()
	w := newSpannedTestWAL(t, dir, nil)
	seq := appendSpannedTestRecord(t, w, []byte("first"))
	require.NoError(t, w.Close())
	active := walActivePath(dir)
	writeSpannedTestParts(t, active, seq+1, spannedTestData(40<<20), 2)
	file, err := os.OpenFile(active, os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	entry := WALEntry{Sequence: seq + 2, Timestamp: time.Now(), Operation: OpEntityCommit, Database: "test"}
	entry.Data, err = marshalWALRecoveryPayload(nil, walEntityCommitData{Payload: []byte("after-orphan")})
	require.NoError(t, err)
	entry.Checksum = crc32Checksum(entry.Data)
	payload, err := processWALEntryPayload(walPayloadEncode, &bytes.Buffer{}, &entry, nil, 0)
	require.NoError(t, err)
	writer := bufio.NewWriter(file)
	_, err = writeAtomicRecordBufio(writer, payload, walFormatVersionCurrent)
	require.NoError(t, err)
	require.NoError(t, writer.Flush())
	require.NoError(t, file.Close())
	entries, err := ReadWALEntries(t.Context(), active, nil)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("first"), []byte("after-orphan")}, spannedTestPayloads(t, entries))

	var a walRecordAssembler
	part := func(index, count uint32, s uint64) WALEntry {
		data := make([]byte, walSpannedPartHeaderLen+4)
		binary.LittleEndian.PutUint32(data[0:4], index)
		binary.LittleEndian.PutUint32(data[4:8], count)
		return WALEntry{Sequence: s, Operation: OpRecordPart, Data: data}
	}
	_, _, err = a.add(part(1, 3, 9), walFormatVersionSpanned)
	require.ErrorIs(t, err, ErrWALCorrupted, "a middle part without its first part")
	_, _, err = a.add(part(0, 3, 9), walFormatVersionSpanned)
	require.NoError(t, err)
	_, _, err = a.add(part(2, 3, 9), walFormatVersionSpanned)
	require.ErrorIs(t, err, ErrWALCorrupted, "a skipped part")
	_, _, err = a.add(part(0, 1, 9), walFormatVersionSpanned)
	require.ErrorIs(t, err, ErrWALCorrupted, "a one-part record is not spanned")
	_, _, err = a.add(WALEntry{Sequence: 9, Operation: OpRecordPart, Data: make([]byte, 12)}, walFormatVersionCurrent)
	require.ErrorIs(t, err, ErrWALCorrupted, "a record part in an ordinary frame")
}

// A frame version this build does not know is refused, never skipped.
func TestWALUnknownFrameVersionIsRefused(t *testing.T) {
	dir := t.TempDir()
	w := newSpannedTestWAL(t, dir, nil)
	appendSpannedTestRecord(t, w, []byte("first"))
	require.NoError(t, w.Close())
	active := walActivePath(dir)
	raw, err := os.ReadFile(active)
	require.NoError(t, err)
	raw[4] = walFormatVersionSpanned + 1
	require.NoError(t, os.WriteFile(active, raw, 0o644))
	require.ErrorIs(t, ValidateWALFormat(dir, WALValidateCurrent), ErrWALUnsupportedVersion)
	_, err = ReadWALEntries(t.Context(), active, nil)
	require.ErrorIs(t, err, ErrWALUnsupportedVersion)
}

// Snapshot truncation keeps or drops a spanned record whole, and rewrites a
// retained one in parts.
func TestWALSnapshotTruncationKeepsSpannedRecordsWhole(t *testing.T) {
	for _, cut := range []string{"before", "at"} {
		t.Run(cut, func(t *testing.T) {
			dir := t.TempDir()
			w := newSpannedTestWAL(t, dir, nil)
			first := appendSpannedTestRecord(t, w, []byte("first"))
			big := spannedTestData(40 << 20)
			bigSeq := appendSpannedTestRecord(t, w, big)
			appendSpannedTestRecord(t, w, []byte("last"))
			snapshotSeq := first
			want := [][]byte{big, []byte("last")}
			if cut == "at" {
				snapshotSeq = bigSeq
				want = want[1:]
			}
			require.NoError(t, w.ManageSnapshot(t.Context(), snapshotSeq, WALSnapshotTruncate))
			require.NoError(t, w.Close())
			entries, err := ReadWALEntriesFromDir(t.Context(), dir)
			require.NoError(t, err)
			require.Equal(t, want, spannedTestPayloads(t, entries))
			for _, frame := range walTestFrames(t, walActivePath(dir)) {
				if frame.version == walFormatVersionSpanned {
					require.Equal(t, bigSeq, frame.seq)
				}
			}
			repaired, _, err := repairWALTailIfNeeded(walActivePath(dir), nil)
			require.NoError(t, err)
			require.False(t, repaired)
		})
	}
}

// Rotation happens after a whole logical record: a spanned record never
// spans two segment files, and the directory readers see every record.
func TestWALSpannedRecordNeverCrossesASegment(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultWALConfig()
	cfg.MaxEntries = 1
	w := newSpannedTestWAL(t, dir, cfg)
	big := spannedTestData(40 << 20)
	appendSpannedTestRecord(t, w, []byte("first"))
	bigSeq := appendSpannedTestRecord(t, w, big)
	appendSpannedTestRecord(t, w, []byte("last"))
	require.NoError(t, w.Close())
	manifest, err := accessWALManifest(dir, walReadManifest, nil)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(manifest.Segments), 2)
	parts := 0
	for _, segment := range manifest.Segments {
		path, err := resolveWALSegmentPath(dir, segment.Path)
		require.NoError(t, err)
		frames := walTestFrames(t, path)
		segmentParts := 0
		for _, frame := range frames {
			if frame.version == walFormatVersionSpanned {
				segmentParts++
				require.Equal(t, bigSeq, frame.seq)
			}
		}
		require.True(t, segmentParts == 0 || segmentParts == 3, "all parts in one segment")
		parts += segmentParts
	}
	require.Equal(t, 3, parts)
	entries, err := ReadWALEntriesFromDir(t.Context(), dir)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("first"), big, []byte("last")}, spannedTestPayloads(t, entries))
}

// Some parts are durable and the last is not: nothing is applied, the log is
// repaired and the next commit is journaled and replayed normally.
func TestSpannedEntityCommitWithoutLastPartIsNotApplied(t *testing.T) {
	dir := t.TempDir()
	b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true, synchronous: true, manualDrain: true})
	w := b.bufferedCommitWAL
	payload, err := EncodeEntityTransaction(EntityTransactionRequest{Namespace: "test", LogicalID: "spanned-torn", Operations: spannedTestOperations(600, 40<<10), Mode: CommitModeExplicit})
	require.NoError(t, err)
	seq := w.Sequence()
	crashClose(t, b, w)
	writeSpannedTestParts(t, walActivePath(filepath.Join(dir, "wal")), seq+1, payload, 1)

	b = newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true, synchronous: true, manualDrain: true})
	w = b.bufferedCommitWAL
	n, err := RecoverBufferedCommitsAtOpen(b, w)
	require.NoError(t, err)
	require.Zero(t, n)
	requireSpannedTestNodes(t, b, 0)
	small, err := EncodeEntityTransaction(EntityTransactionRequest{Namespace: "test", LogicalID: "after-torn", Operations: spannedTestOperations(1, 10), Mode: CommitModeExplicit})
	require.NoError(t, err)
	require.NoError(t, w.AppendWithDatabase(OpEntityCommit, walEntityCommitData{Payload: small}, "test"))
	crashClose(t, b, w)
	b = newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true, synchronous: true, manualDrain: true})
	n, err = RecoverBufferedCommitsAtOpen(b, b.bufferedCommitWAL)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	requireSpannedTestNodes(t, b, 1)
}
