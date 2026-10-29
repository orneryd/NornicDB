package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The fixtures here write through storage's real public WAL writer
// (NewWAL, AppendWithDatabaseReturningSeq), so the command reads records the
// production writer produced. Where a test needs a corrupt payload, an
// out-of-order sequence or an unreadable segment, it writes the published
// atomic WAL framing directly and asserts the reader observes that content;
// the frame version is read back from a record the real writer emitted, and
// the raw payload layout below is the one decodeWALEntry accepts.

// commandWALMagic and commandWALTrailer are the public atomic WAL framing
// markers ("WALE" little-endian and the incomplete-write canary). The frame
// version is not restated: commandWALVersion reads it from a record the real
// writer produced, so a fixture cannot pin a version the writer no longer
// emits.
const (
	commandWALMagic   uint32 = 0x454C4157
	commandWALTrailer uint64 = 0xDEADBEEFFEEDFACE
)

// commandWALVersion is the per-record frame version the production writer
// currently emits. It is derived from a record written by the real WAL writer
// rather than hardcoded, and each fixture write re-checks it against the
// record it frames.
func commandWALVersion(t *testing.T) byte {
	t.Helper()
	dir, _ := writeWALFixture(t, controlFrame("version-probe", "test"))
	record, err := os.ReadFile(filepath.Join(dir, "wal.log"))
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(record), 9)
	require.Equal(t, commandWALMagic, binary.LittleEndian.Uint32(record[:4]))
	return record[4]
}

// walCheckTable is the CRC table the WAL's record and data checksums use.
var walCheckTable = crc32.MakeTable(crc32.Castagnoli)

// frame is one entry the fixture intends to write with the real writer.
type frame struct {
	op       storage.OperationType
	data     interface{}
	database string
}

// nodeFrame and controlFrame build the fixture entries used by several tests.
func nodeFrame(id string, database string) frame {
	return frame{
		op: storage.OpCreateNode,
		data: storage.WALNodeData{Node: &storage.Node{
			ID:         storage.NodeID(id),
			Labels:     []string{"Person"},
			Properties: map[string]any{"name": "Ada", "age": int64(36)},
		}},
		database: database,
	}
}

func controlFrame(txID string, database string) frame {
	return frame{op: storage.OpTxBegin, data: storage.WALTxData{TxID: txID}, database: database}
}

// writeWALFixture writes frames with the real public WAL writer and closes it
// so every record is flushed. The sequence each frame was assigned is returned
// alongside the WAL directory.
func writeWALFixture(t *testing.T, frames ...frame) (string, []uint64) {
	t.Helper()
	dir := t.TempDir()
	wal, err := storage.NewWAL(dir, &storage.WALConfig{Dir: dir, SyncMode: "immediate"})
	require.NoError(t, err)
	sequences := make([]uint64, 0, len(frames))
	for _, f := range frames {
		seq, err := wal.AppendWithDatabaseReturningSeq(f.op, f.data, f.database)
		require.NoError(t, err, "append %s", f.op)
		require.NotZero(t, seq, "the WAL must be enabled for a sequence to be assigned")
		sequences = append(sequences, seq)
	}
	require.NoError(t, wal.Close())
	return dir, sequences
}

// runCommand runs the command in-process and returns its exit code, stdout and
// stderr.
func runCommand(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := run(args, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

// outputLine is the decoded shape of one printed line.
type outputLine struct {
	Seq       uint64          `json:"seq"`
	Timestamp string          `json:"timestamp"`
	Database  string          `json:"database"`
	Kind      string          `json:"kind"`
	Payload   json.RawMessage `json:"payload"`
}

// decodeLines decodes one JSON object per non-empty line and fails on any
// other content, so stray text or a broken line cannot pass unnoticed.
func decodeLines(t *testing.T, stdout string) []outputLine {
	t.Helper()
	lines := []outputLine{}
	for _, raw := range bytes.Split([]byte(stdout), []byte("\n")) {
		if len(bytes.TrimSpace(raw)) == 0 {
			continue
		}
		var line outputLine
		require.NoError(t, json.Unmarshal(raw, &line), "every line is one JSON object: %q", raw)
		require.True(t, json.Valid(line.Payload), "payload is raw JSON: %q", raw)
		lines = append(lines, line)
	}
	return lines
}

// sequencesOf lists the printed sequence numbers in output order.
func sequencesOf(lines []outputLine) []uint64 {
	out := make([]uint64, 0, len(lines))
	for _, line := range lines {
		out = append(out, line.Seq)
	}
	return out
}

// TestWalReadPrintsTypedAndControlEntries checks the printed shape: a typed
// node record carries its payload as decoded JSON (not the record JSON-escaped
// into a string) and a control record keeps its historical JSON payload.
func TestWalReadPrintsTypedAndControlEntries(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dir, seqs := writeWALFixture(t, nodeFrame("n1", "test"), controlFrame("tx-1", "test"))
	require.Len(t, seqs, 2)

	code, stdout, stderr := runCommand(t, "-dir", dir)
	require.Equal(t, exitOK, code, "stderr: %s", stderr)
	require.Empty(t, stderr)

	lines := decodeLines(t, stdout)
	require.Len(t, lines, 2)

	node := lines[0]
	require.Equal(t, seqs[0], node.Seq)
	require.Equal(t, "create_node", node.Kind)
	require.Equal(t, "test", node.Database)
	require.NotEmpty(t, node.Timestamp, "a written entry has a timestamp")
	_, err := time.Parse(time.RFC3339Nano, node.Timestamp)
	require.NoError(t, err, "timestamp is RFC 3339Nano: %q", node.Timestamp)
	require.Contains(t, node.Timestamp, "Z", "timestamps are UTC")

	var nodePayload storage.WALNodeData
	require.NoError(t, json.Unmarshal(node.Payload, &nodePayload), "payload is the record's JSON: %s", node.Payload)
	require.Equal(t, storage.NodeID("n1"), nodePayload.Node.ID)
	require.Equal(t, []string{"Person"}, nodePayload.Node.Labels)
	require.Equal(t, "Ada", nodePayload.Node.Properties["name"])
	require.NotContains(t, string(node.Payload), `"node\":\"`, "the payload must not be a JSON-escaped string")

	control := lines[1]
	require.Equal(t, seqs[1], control.Seq)
	require.Equal(t, "tx_begin", control.Kind)
	require.JSONEq(t, `{"tx_id":"tx-1"}`, string(control.Payload))

	// Each line has exactly the five documented members.
	require.Equal(t, []string{"seq", "timestamp", "database", "kind", "payload"}, firstLineKeys(t, stdout))
}

func TestWalReadUnpacksActualCompressedBufferedCommit(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dir := t.TempDir()
	engine, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		DataDir: dir, WriteBehind: true, WriteBehindInterval: time.Hour,
	}))
	require.NoError(t, err)
	walDir := filepath.Join(dir, "wal")
	wal, err := storage.NewWAL(walDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })
	t.Cleanup(func() { _ = engine.Close() })
	engine.SetBufferedCommitWAL(wal)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.SetImplicit(true))
	for i := 0; i < 40; i++ {
		_, err = tx.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("test:n%d", i)), Properties: map[string]any{"name": "Ada"}})
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered())
	entries, err := storage.ReadWALEntriesFromDir(walDir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, []byte{0x28, 0xb5, 0x2f, 0xfd}, entries[0].Data[:4], "the actual commit must be zstd compressed")

	code, stdout, stderr := runCommand(t, "-dir", walDir)
	require.Equal(t, exitOK, code, stderr)
	lines := decodeLines(t, stdout)
	require.Len(t, lines, 1)
	require.Equal(t, string(storage.OpBufferedCommit), lines[0].Kind)
	var payload struct {
		Ops []storage.Operation `json:"ops"`
	}
	require.NoError(t, json.Unmarshal(lines[0].Payload, &payload))
	require.Len(t, payload.Ops, 40)
	require.Equal(t, "Ada", payload.Ops[0].Node.Properties["name"])
}

// firstLineKeys lists the member names of the first printed line in the
// documented order, and fails if the line carries any other member.
func firstLineKeys(t *testing.T, stdout string) []string {
	t.Helper()
	first := bytes.SplitN([]byte(stdout), []byte("\n"), 2)[0]
	var object map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(first, &object))
	var keys []string
	for _, member := range []string{"seq", "timestamp", "database", "kind", "payload"} {
		if _, ok := object[member]; ok {
			keys = append(keys, member)
		}
	}
	require.Len(t, object, 5, "no other members: %s", first)
	return keys
}

// TestWalReadHonoursInclusiveBounds checks inclusive -from and -to bounds,
// including bounds outside the WAL.
func TestWalReadHonoursInclusiveBounds(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	frames := make([]frame, 0, 5)
	for i := 0; i < 5; i++ {
		frames = append(frames, nodeFrame(string(rune('a'+i)), "test"))
	}
	dir, seqs := writeWALFixture(t, frames...)
	require.Equal(t, []uint64{1, 2, 3, 4, 5}, seqs)

	for _, test := range []struct {
		name string
		args []string
		want []uint64
	}{
		{name: "no bounds prints every entry", want: []uint64{1, 2, 3, 4, 5}},
		{name: "from is inclusive", args: []string{"-from", "3"}, want: []uint64{3, 4, 5}},
		{name: "to is inclusive", args: []string{"-to", "2"}, want: []uint64{1, 2}},
		{name: "both bounds", args: []string{"-from", "2", "-to", "4"}, want: []uint64{2, 3, 4}},
		{name: "a single sequence", args: []string{"-from", "3", "-to", "3"}, want: []uint64{3}},
		{name: "to 0 means no upper bound", args: []string{"-from", "5", "-to", "0"}, want: []uint64{5}},
		{name: "a to bound past the WAL keeps the rest", args: []string{"-from", "4", "-to", "99"}, want: []uint64{4, 5}},
		{name: "from past the WAL prints nothing", args: []string{"-from", "6"}, want: []uint64{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			args := append([]string{"-dir", dir}, test.args...)
			code, stdout, stderr := runCommand(t, args...)
			require.Equal(t, exitOK, code, "stderr: %s", stderr)
			require.Empty(t, stderr)
			lines := decodeLines(t, stdout)
			require.Equal(t, test.want, sequencesOf(lines))
			for _, line := range lines {
				require.Equal(t, "create_node", line.Kind)
				require.Equal(t, "test", line.Database)
			}
		})
	}
}

// TestWalReadDoesNotStopEarlyAtUnreachableUpperBound runs against a WAL whose
// stored records are not in sequence order, as a sealed segment followed by
// the active file, or interleaved appends, can leave them. An entry above the
// upper bound must be skipped, never treated as the end of the read.
func TestWalReadDoesNotStopEarlyAtUnreachableUpperBound(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dir := t.TempDir()
	writeRawFrames(t, filepath.Join(dir, "wal.log"),
		rawFrame{seq: 4, op: storage.OpTxBegin, data: storage.WALTxData{TxID: "tx-4"}},
		rawFrame{seq: 2, op: storage.OpTxBegin, data: storage.WALTxData{TxID: "tx-2"}},
		rawFrame{seq: 9, op: storage.OpTxBegin, data: storage.WALTxData{TxID: "tx-9"}},
		rawFrame{seq: 3, op: storage.OpTxBegin, data: storage.WALTxData{TxID: "tx-3"}},
	)

	code, stdout, stderr := runCommand(t, "-dir", dir, "-to", "3")
	require.Equal(t, exitOK, code, "stderr: %s", stderr)
	require.Equal(t, []uint64{2, 3}, sequencesOf(decodeLines(t, stdout)),
		"entries above the upper bound are skipped, not treated as the end")

	code, stdout, stderr = runCommand(t, "-dir", dir, "-from", "3", "-to", "3")
	require.Equal(t, exitOK, code, "stderr: %s", stderr)
	require.Equal(t, []uint64{3}, sequencesOf(decodeLines(t, stdout)))

	// The fixture is stored out of order and the reader visits it that way, so
	// filtering past an entry above the bound is what keeps the read complete.
	code, stdout, stderr = runCommand(t, "-dir", dir)
	require.Equal(t, exitOK, code, "stderr: %s", stderr)
	require.Equal(t, []uint64{4, 2, 9, 3}, sequencesOf(decodeLines(t, stdout)))
}

// TestWalReadRejectsInvalidArgumentsAndMissingDirectory checks every rejected
// invocation: it exits with the usage status, explains why and prints nothing
// on stdout.
func TestWalReadRejectsInvalidArgumentsAndMissingDirectory(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dir, _ := writeWALFixture(t, nodeFrame("n1", "test"))
	file := filepath.Join(t.TempDir(), "not-a-dir")
	require.NoError(t, os.WriteFile(file, []byte("x"), 0o600))

	for _, test := range []struct {
		name  string
		args  []string
		match string
	}{
		{name: "no arguments requires -dir", match: "-dir is required"},
		{name: "an empty -dir", args: []string{"-dir", ""}, match: "-dir is required"},
		{name: "a missing directory", args: []string{"-dir", filepath.Join(dir, "absent")}, match: "no such file"},
		{name: "a file instead of a directory", args: []string{"-dir", file}, match: "is not a directory"},
		{name: "a zero -from", args: []string{"-dir", dir, "-from", "0"}, match: "-from must be at least 1"},
		{name: "a negative -from", args: []string{"-dir", dir, "-from", "-2"}, match: "-from must be at least 1"},
		{name: "a negative -to", args: []string{"-dir", dir, "-to", "-1"}, match: "-to must not be negative"},
		{name: "a -to below -from", args: []string{"-dir", dir, "-from", "4", "-to", "2"}, match: "below -from"},
		{name: "a non-numeric bound", args: []string{"-dir", dir, "-from", "x"}, match: "invalid arguments"},
		{name: "a stray argument", args: []string{"-dir", dir, "extra"}, match: "unexpected argument"},
		{name: "an unknown flag", args: []string{"-dir", dir, "-seq", "1"}, match: "invalid arguments"},
	} {
		t.Run(test.name, func(t *testing.T) {
			code, stdout, stderr := runCommand(t, test.args...)
			require.Equal(t, exitUsage, code)
			require.Empty(t, stdout, "a rejected read prints no entries")
			require.Contains(t, stderr, test.match)
		})
	}

	// -h is a request for help, not a failed read.
	code, stdout, stderr := runCommand(t, "-h")
	require.Equal(t, exitOK, code)
	require.Empty(t, stdout)
	require.Contains(t, stderr, "usage: go run ./scripts/wal-read")
}

// TestWalReadReportsConversionAndReadErrors checks that a payload the display
// owner cannot render and a WAL the reader cannot read both fail the read with
// the read status and a cause, and that neither invents an entry.
func TestWalReadReportsConversionAndReadErrors(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())

	t.Run("a typed payload that cannot be decoded", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "wal.log"),
			framedRecord(t, storage.OpCreateNode, []byte{0x82, 0xa4, 'n', 'o', 'd', 'e'}, 1), 0o600))
		code, stdout, stderr := runCommand(t, "-dir", dir)
		require.Equal(t, exitRead, code)
		require.Empty(t, stdout, "a failed entry prints no line")
		require.Contains(t, stderr, "failed to decode entry 1 (create_node) payload")
	})

	t.Run("an operation without a display payload", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "wal.log"),
			framedRecord(t, storage.OperationType("not_an_operation"), []byte("{}"), 1), 0o600))
		code, stdout, stderr := runCommand(t, "-dir", dir)
		require.Equal(t, exitRead, code)
		require.Empty(t, stdout)
		require.Contains(t, stderr, "unsupported operation")
	})

	t.Run("a corrupt record payload", func(t *testing.T) {
		dir := t.TempDir()
		record := framedRecord(t, storage.OpTxBegin, json.RawMessage(`{"tx_id":"tx-1"}`), 1)
		// Flip a bit inside the payload so the frame's CRC no longer matches.
		record[12] ^= 0xFF
		require.NoError(t, os.WriteFile(filepath.Join(dir, "wal.log"), record, 0o600))
		code, stdout, stderr := runCommand(t, "-dir", dir)
		require.Equal(t, exitRead, code)
		require.Empty(t, stdout)
		require.Contains(t, stderr, "checksum")
	})

	t.Run("a manifest naming an escaping segment", func(t *testing.T) {
		dir := t.TempDir()
		segmentDir := filepath.Join(dir, "segments")
		require.NoError(t, os.Mkdir(segmentDir, 0o700))
		manifest := `{"version":1,"segments":[{"first_seq":1,"last_seq":2,"size_bytes":0,` +
			`"created_at":"2026-01-01T00:00:00Z","path":"../wal.log"}]}`
		require.NoError(t, os.WriteFile(filepath.Join(segmentDir, "manifest.json"), []byte(manifest), 0o600))
		code, stdout, stderr := runCommand(t, "-dir", dir)
		require.Equal(t, exitRead, code)
		require.Empty(t, stdout)
		require.Contains(t, stderr, "invalid segment path")
	})

	t.Run("a manifest that is not JSON", func(t *testing.T) {
		dir := t.TempDir()
		segmentDir := filepath.Join(dir, "segments")
		require.NoError(t, os.Mkdir(segmentDir, 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(segmentDir, "manifest.json"), []byte("not json"), 0o600))
		code, stdout, stderr := runCommand(t, "-dir", dir)
		require.Equal(t, exitRead, code)
		require.Empty(t, stdout)
		require.Contains(t, stderr, "wal-read:")
	})

	if os.Geteuid() == 0 {
		t.Log("skipping the permission fixtures: root reads every file")
		return
	}

	t.Run("an unreadable segment", func(t *testing.T) {
		dir := t.TempDir()
		segmentDir := filepath.Join(dir, "segments")
		require.NoError(t, os.Mkdir(segmentDir, 0o700))
		name := "seg-00000000000000000001-00000000000000000002.wal"
		path := filepath.Join(segmentDir, name)
		require.NoError(t, os.WriteFile(path, framedRecord(t, storage.OpTxBegin,
			json.RawMessage(`{"tx_id":"tx-1"}`), 1), 0o600))
		requireUnreadable(t, path)
		manifest := `{"version":1,"segments":[{"first_seq":1,"last_seq":2,"size_bytes":0,` +
			`"created_at":"2026-01-01T00:00:00Z","path":"` + name + `"}]}`
		require.NoError(t, os.WriteFile(filepath.Join(segmentDir, "manifest.json"), []byte(manifest), 0o600))

		code, stdout, stderr := runCommand(t, "-dir", dir)
		require.Equal(t, exitRead, code)
		require.Empty(t, stdout)
		require.Contains(t, stderr, "wal-read:")
	})

	t.Run("an unreadable active WAL", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "wal.log")
		require.NoError(t, os.WriteFile(path, framedRecord(t, storage.OpTxBegin,
			json.RawMessage(`{"tx_id":"tx-1"}`), 1), 0o600))
		requireUnreadable(t, path)
		code, stdout, stderr := runCommand(t, "-dir", dir)
		require.Equal(t, exitRead, code)
		require.Empty(t, stdout)
		require.Contains(t, stderr, "wal-read:")
	})
}

// requireUnreadable removes every permission from path and fails the test when
// the platform still allows reading it, so a permission fixture cannot pass
// without exercising the IO failure it claims to.
func requireUnreadable(t *testing.T, path string) {
	t.Helper()
	require.NoError(t, os.Chmod(path, 0o000))
	t.Cleanup(func() { _ = os.Chmod(path, 0o600) })
	file, err := os.Open(path)
	if err == nil {
		require.NoError(t, file.Close())
		t.Fatalf("%s is still readable after chmod 000; the fixture cannot test an IO error", path)
	}
	require.Error(t, err)
}

// TestWalReadLeavesTheWALBytesUnchanged checks the command's read-only
// guarantee: after a full read and a bounded read, every file in the WAL
// directory is byte-for-byte what the writer left, with the same modification
// time, and the directory listing is unchanged.
func TestWalReadLeavesTheWALBytesUnchanged(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dir, seqs := writeWALFixture(t,
		nodeFrame("n1", "test"),
		controlFrame("tx-1", "test"),
		nodeFrame("n2", "other"),
	)
	require.Len(t, seqs, 3)
	entries, err := storage.ReadWALEntriesFromDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 3, "the fixture's entries are readable before the run")

	before := snapshotDir(t, dir)
	require.NotEmpty(t, before, "the WAL must have files to compare")

	code, stdout, stderr := runCommand(t, "-dir", dir)
	require.Equal(t, exitOK, code, "stderr: %s", stderr)
	require.Len(t, decodeLines(t, stdout), 3)

	code, stdout, stderr = runCommand(t, "-dir", dir, "-from", "2", "-to", "2")
	require.Equal(t, exitOK, code, "stderr: %s", stderr)
	require.Len(t, decodeLines(t, stdout), 1)

	require.Equal(t, before, snapshotDir(t, dir), "reading the WAL must not change its bytes, times or listing")
}

// TestWalReadLeavesACorruptWALUnchanged checks that a failing read is also
// read-only: no repair, truncation or backup file appears.
func TestWalReadLeavesACorruptWALUnchanged(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dir := t.TempDir()
	record := framedRecord(t, storage.OpTxBegin, json.RawMessage(`{"tx_id":"tx-1"}`), 1)
	record[12] ^= 0xFF
	require.NoError(t, os.WriteFile(filepath.Join(dir, "wal.log"), record, 0o600))

	before := snapshotDir(t, dir)
	code, stdout, stderr := runCommand(t, "-dir", dir)
	require.Equal(t, exitRead, code)
	require.Empty(t, stdout)
	require.NotEmpty(t, stderr)
	require.Equal(t, before, snapshotDir(t, dir))
}

// TestWalReadPrintsEachEntryOnce checks that a WAL spanning a sealed segment
// and the active file prints every entry exactly once, across the boundary.
func TestWalReadPrintsEachEntryOnce(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dir := t.TempDir()
	wal, err := storage.NewWAL(dir, &storage.WALConfig{Dir: dir, SyncMode: "immediate", MaxEntries: 2})
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		require.NoError(t, wal.AppendWithDatabase(storage.OpCreateNode, storage.WALNodeData{
			Node: &storage.Node{ID: storage.NodeID(string(rune('a' + i)))},
		}, "test"))
	}
	require.NoError(t, wal.Close())

	code, stdout, stderr := runCommand(t, "-dir", dir)
	require.Equal(t, exitOK, code, "stderr: %s", stderr)
	require.Equal(t, []uint64{1, 2, 3, 4, 5}, sequencesOf(decodeLines(t, stdout)))

	code, stdout, stderr = runCommand(t, "-dir", dir, "-from", "3", "-to", "4")
	require.Equal(t, exitOK, code, "stderr: %s", stderr)
	require.Equal(t, []uint64{3, 4}, sequencesOf(decodeLines(t, stdout)))
}

// TestFormatTimestamp checks the RFC 3339Nano rendering, including the zero
// time and an out-of-range year that RFC 3339 cannot spell.
func TestFormatTimestamp(t *testing.T) {
	zone, err := time.LoadLocation("America/Phoenix")
	require.NoError(t, err)
	when := time.Date(2026, 3, 4, 5, 6, 7, 8, zone)
	require.Equal(t, when.UTC().Format(time.RFC3339Nano), formatTimestamp(when))
	require.Contains(t, formatTimestamp(when), "Z")
	require.Empty(t, formatTimestamp(time.Time{}))
	require.Empty(t, formatTimestamp(time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)))
}

// TestParseBounds pins the accepted and rejected sequence spans and the
// inclusive filter they drive.
func TestParseBounds(t *testing.T) {
	span, err := parseBounds(1, 0)
	require.NoError(t, err)
	require.Equal(t, bounds{max: 0, scanAfter: 0}, span)
	require.True(t, span.retained(1))
	require.True(t, span.retained(1_000_000))

	span, err = parseBounds(4, 6)
	require.NoError(t, err)
	require.Equal(t, bounds{max: 6, scanAfter: 3}, span)
	require.False(t, span.retained(3))
	require.True(t, span.retained(4))
	require.True(t, span.retained(6))
	require.False(t, span.retained(7))

	for _, test := range []struct{ from, to int64 }{{0, 0}, {-1, 0}, {1, -1}, {5, 4}} {
		_, err := parseBounds(test.from, test.to)
		require.Error(t, err, "from %d to %d", test.from, test.to)
	}
}

// --- fixture helpers -----------------------------------------------------

// rawFrame is one record written directly in the atomic WAL framing, so a test
// can control the stored sequence order or the payload.
type rawFrame struct {
	seq  uint64
	op   storage.OperationType
	data interface{}
}

// writeRawFrames writes frames to path in the order given, each with its own
// sequence number, and verifies with the storage reader that they decode as
// intended, so a fixture mistake cannot masquerade as a command defect.
func writeRawFrames(t *testing.T, path string, frames ...rawFrame) {
	t.Helper()
	var content []byte
	for _, f := range frames {
		data, err := json.Marshal(f.data)
		require.NoError(t, err)
		content = append(content, framedRecord(t, f.op, data, f.seq)...)
	}
	require.NoError(t, os.WriteFile(path, content, 0o600))

	read, err := storage.ReadWALEntriesFromDir(filepath.Dir(path))
	require.NoError(t, err)
	require.Len(t, read, len(frames), "the fixture's frames must all be readable")
	for i, entry := range read {
		require.Equal(t, frames[i].seq, entry.Sequence, "the reader sees the stored order")
		require.Equal(t, frames[i].op, entry.Operation)
	}
}

// framedRecord frames one entry in the current atomic WAL format:
// [magic:4][version:1][length:4][payload:N][crc:4][trailer:8][padding:0-7].
// The payload is the current raw-entry layout the reader decodes:
//
//	[uint32 LE json length][WALEntry JSON header omitting Data][uint32 LE data length][raw data]
//
// The frame CRC covers that whole payload and the header's checksum covers the
// raw data, so a fixture write exercises the same bytes the production writer
// emits. The version byte is the one a real writer record carries.
func framedRecord(t *testing.T, op storage.OperationType, data []byte, seq uint64) []byte {
	t.Helper()
	_, record := rawEntryRecord(t, op, data, seq)
	return record
}

// rawEntryRecord builds one entry record with the current framing and returns
// the version byte together with the encoded record, so a test can derive the
// version from output the real writer produced.
func rawEntryRecord(t *testing.T, op storage.OperationType, data []byte, seq uint64) (byte, []byte) {
	t.Helper()
	header := struct {
		Sequence  uint64                `json:"seq"`
		Timestamp time.Time             `json:"ts"`
		Operation storage.OperationType `json:"op"`
		Checksum  uint32                `json:"checksum"`
		Database  string                `json:"database,omitempty"`
	}{
		Sequence:  seq,
		Timestamp: time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC),
		Operation: op,
		Checksum:  crc32.Checksum(data, walCheckTable),
		Database:  "test",
	}
	jsonHeader, err := json.Marshal(header)
	if err != nil {
		panic(err)
	}
	payload := make([]byte, 4+len(jsonHeader)+4+len(data))
	binary.LittleEndian.PutUint32(payload[0:], uint32(len(jsonHeader)))
	copy(payload[4:], jsonHeader)
	dataOff := 4 + len(jsonHeader)
	binary.LittleEndian.PutUint32(payload[dataOff:], uint32(len(data)))
	copy(payload[dataOff+4:], data)

	version := commandWALVersion(t)
	rawLen := int64(9 + len(payload) + 4 + 8)
	record := make([]byte, (rawLen+7)&^int64(7))
	binary.LittleEndian.PutUint32(record[0:], commandWALMagic)
	record[4] = version
	binary.LittleEndian.PutUint32(record[5:], uint32(len(payload)))
	copy(record[9:], payload)
	offset := 9 + len(payload)
	binary.LittleEndian.PutUint32(record[offset:], crc32.Checksum(payload, walCheckTable))
	offset += 4
	binary.LittleEndian.PutUint64(record[offset:], commandWALTrailer)
	return version, record
}

// fileSnapshot is one file's identity as the read-only guarantee observes it.
type fileSnapshot struct {
	Size    int64
	ModTime time.Time
	Bytes   []byte
}

// snapshotDir records every regular file under dir, including nested segment
// files, with its bytes and modification time.
func snapshotDir(t *testing.T, dir string) map[string]fileSnapshot {
	t.Helper()
	snapshot := make(map[string]fileSnapshot)
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		snapshot[path] = fileSnapshot{Size: info.Size(), ModTime: info.ModTime(), Bytes: content}
		return nil
	})
	require.NoError(t, err)
	return snapshot
}

// openFixtureEngine opens the directory-backed engine this command's fixtures
// read and write, entering the fixture lifetime owner before use. It fails the
// test rather than returning a half-open store.
