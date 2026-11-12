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

// NOR-412: these tests drive a WAL that writes large records (WALRecordsLarge)
// directly. A record of 40 MiB is written as parts in consecutive frames that
// carry the record's sequence, and the readers each test names return it as one
// record.

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

// A small record, a 40 MiB record and a small record. The file holds five
// frames: an ordinary one, three part frames with the large record's sequence,
// an ordinary one. ReadWALEntriesFromDir, VisitWALEntriesAfterFromDir,
// readWALEntriesForTruncation and scanWALPropertyEpochFile return the large
// record as one entry; CheckWALIntegrity counts three entries and reports no
// error; ValidateWALFormat accepts the directory, repairWALTailIfNeeded repairs
// nothing and walV5OrLaterOnly reports current framing; a reopened WAL
// continues after the last sequence.
func TestWALSpannedRecordIsReadAsOneLogicalRecord(t *testing.T) {
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

// encodeWALRecord gives a record that fits one frame the bytes
// processWALEntryPayload gives it, with spanning allowed and without. A record
// of exactly one frame has no parts; one byte more makes two parts, each within
// a frame, written with the spanned frame version.
func TestWALRecordOfOneFrameIsEncodedAsOneFrame(t *testing.T) {
	var buf bytes.Buffer
	entry := WALEntry{Sequence: 7, Timestamp: time.Unix(1700000000, 5).UTC(), Operation: OpEntityCommit, Data: []byte("payload"), Database: "test"}
	entry.Checksum = crc32Checksum(entry.Data)
	before, err := processWALEntryPayload(walPayloadEncode, &buf, &entry, nil, 0)
	require.NoError(t, err)
	for _, spanning := range []bool{false, true} {
		fitting, err := encodeWALRecord(&buf, &entry, spanning)
		require.NoError(t, err)
		require.Equal(t, walRecordFrames{single: before}, fitting, "spanning=%t", spanning)
	}

	overhead := len(before) - len(entry.Data)
	entry.Data = spannedTestData(int(walMaxEntrySize) - overhead)
	entry.Checksum = crc32Checksum(entry.Data)
	record, err := encodeWALRecord(&buf, &entry, true)
	require.NoError(t, err)
	require.Nil(t, record.parts, "a payload of exactly one frame stays one frame")
	require.Len(t, record.single, int(walMaxEntrySize))

	entry.Data = append(entry.Data, 1)
	entry.Checksum = crc32Checksum(entry.Data)
	record, err = encodeWALRecord(&buf, &entry, true)
	require.NoError(t, err)
	require.Nil(t, record.single)
	require.Len(t, record.parts, 2)
	for _, part := range record.parts {
		require.LessOrEqual(t, len(part), int(walMaxEntrySize))
	}
	var written bytes.Buffer
	_, err = record.write(&written, writeAtomicRecordBufio)
	require.NoError(t, err)
	require.Equal(t, walFormatVersionSpanned, written.Bytes()[4], "parts are written with the spanned frame version")
}

// writeSpannedTestParts appends the first keep parts of a spanned record of data
// with sequence seq to the file at path, which then ends without the record's
// last part. It returns the file's size before the append: where the record begins.
func writeSpannedTestParts(t *testing.T, path string, seq uint64, data []byte, keep int) (groupStart int64) {
	t.Helper()
	entry := WALEntry{Sequence: seq, Timestamp: time.Now(), Operation: OpEntityCommit, Database: "test"}
	encoded, err := marshalWALRecoveryPayload(nil, walEntityCommitData{Payload: data})
	require.NoError(t, err)
	entry.Data, entry.Checksum = encoded, crc32Checksum(encoded)
	var buf bytes.Buffer
	record, err := encodeWALRecord(&buf, &entry, true)
	require.NoError(t, err)
	require.Greater(t, len(record.parts), keep)
	info, err := os.Stat(path)
	require.NoError(t, err)
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	defer file.Close()
	writer := bufio.NewWriter(file)
	_, err = walRecordFrames{parts: record.parts[:keep]}.write(writer, writeAtomicRecordBufio)
	require.NoError(t, err)
	require.NoError(t, writer.Flush())
	return info.Size()
}

// A journal file that ends after one or two of a record's three parts:
// ReadWALEntries returns only the earlier record, CheckWALIntegrity counts one
// entry and reports "incomplete spanned record", reopening the WAL cuts the
// file back to where the record began, and a record appended afterwards reads
// back after the first.
func TestWALSpannedRecordWithoutLastPartIsNotDelivered(t *testing.T) {
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
			require.Equal(t, groupStart, info.Size(), "the file ends where the unfinished record began")
			later := appendSpannedTestRecord(t, w, []byte("later"))
			require.NoError(t, w.Close())
			entries, err = ReadWALEntriesFromDir(t.Context(), dir)
			require.NoError(t, err)
			require.Equal(t, [][]byte{[]byte("acknowledged"), []byte("later")}, spannedTestPayloads(t, entries))
			require.Equal(t, later, entries[1].Sequence)
		})
	}
}

// A file cut 1 MiB into the first, second or third part of a spanned record:
// ReadWALEntries returns only the earlier record, reopening the WAL cuts the
// file back to the end of the earlier record, and a record appended afterwards
// reads back after it.
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

// Two parts of a three-part record followed by an ordinary record:
// ReadWALEntries returns the records before and after and not the unfinished
// one. The assembler returns ErrWALCorrupted for a middle part without its
// first part, for a skipped part and for a record of one part, and returns an
// ordinary frame whose operation is "record_part" as a record.
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
	// An ordinary frame whose operation has the parts' name is returned as a
	// record.
	named := WALEntry{Sequence: 9, Operation: OpRecordPart, Data: make([]byte, 12)}
	logical, complete, err := a.add(named, walFormatVersionCurrent)
	require.NoError(t, err)
	require.True(t, complete)
	require.Equal(t, named, logical)
}

// A frame whose version byte is one above the spanned version is refused with
// ErrWALUnsupportedVersion by ValidateWALFormat and by ReadWALEntries.
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

// ManageSnapshot with truncation: with the snapshot sequence before a spanned
// record the record is kept (it reads back whole, and the part frames of the
// rewritten file carry its sequence); with the snapshot sequence at the record
// it is dropped. Neither leaves a tail to repair.
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

// With one entry per segment (MaxEntries 1), the three parts of a 40 MiB record
// are in one segment file, and ReadWALEntriesFromDir returns the three records
// in order.
func TestWALSpannedRecordStaysInOneSegment(t *testing.T) {
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
		require.True(t, segmentParts == 0 || segmentParts == 3, "a segment holds none of the parts or all three")
		parts += segmentParts
	}
	require.Equal(t, 3, parts)
	entries, err := ReadWALEntriesFromDir(t.Context(), dir)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("first"), big, []byte("last")}, spannedTestPayloads(t, entries))
}

// The journal file ends after the first part of a transaction's record
// (appended by hand after crashClose): the recovery at open applies no record
// and the store has none of its nodes. A small transaction record appended
// afterwards is applied by the next recovery at open.
func TestSpannedEntityCommitWithoutLastPartIsNotApplied(t *testing.T) {
	dir := t.TempDir()
	b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true, synchronous: true, manualDrain: true, records: WALRecordsLarge})
	w := b.bufferedCommitWAL
	payload, err := EncodeEntityTransaction(EntityTransactionRequest{Namespace: "test", LogicalID: "spanned-torn", Operations: spannedTestOperations(600, 40<<10), Mode: CommitModeExplicit})
	require.NoError(t, err)
	seq := w.Sequence()
	crashClose(t, b, w)
	writeSpannedTestParts(t, walActivePath(filepath.Join(dir, "wal")), seq+1, payload, 1)

	b = newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true, synchronous: true, manualDrain: true, records: WALRecordsLarge})
	w = b.bufferedCommitWAL
	n, err := RecoverBufferedCommitsAtOpen(b, w)
	require.NoError(t, err)
	require.Zero(t, n)
	requireSpannedTestNodes(t, b, 0)
	small, err := EncodeEntityTransaction(EntityTransactionRequest{Namespace: "test", LogicalID: "after-torn", Operations: spannedTestOperations(1, 10), Mode: CommitModeExplicit})
	require.NoError(t, err)
	require.NoError(t, w.AppendWithDatabase(OpEntityCommit, walEntityCommitData{Payload: small}, "test"))
	crashClose(t, b, w)
	b = newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true, synchronous: true, manualDrain: true, records: WALRecordsLarge})
	n, err = RecoverBufferedCommitsAtOpen(b, b.bufferedCommitWAL)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	requireSpannedTestNodes(t, b, 1)
}
