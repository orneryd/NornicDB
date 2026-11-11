package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// NOR-412: under WALRecordsLarge, one transaction's journal record larger
// than one 16 MiB WAL frame commits, replays exactly once after a crash
// before or during its storage apply, and an oversized frame an older writer
// left behind is handled as before.

// spannedTestData returns n bytes that differ along their whole length, so a
// misplaced chunk cannot reassemble into the same record.
func spannedTestData(n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte(i*7 + i/4096)
	}
	return out
}

// newSpannedTestWAL opens a WAL that writes large records, as a single-node
// server's does.
func newSpannedTestWAL(t *testing.T, dir string, cfg *WALConfig) *WAL {
	t.Helper()
	t.Cleanup(config.WithWALEnabled())
	if cfg == nil {
		cfg = DefaultWALConfig()
	}
	cfg.Dir = dir
	cfg.SyncMode = "immediate"
	cfg.RecordPolicy = WALRecordsLarge
	w, err := NewWAL(dir, cfg)
	require.NoError(t, err)
	return w
}

func appendSpannedTestRecord(t *testing.T, w *WAL, data []byte) uint64 {
	t.Helper()
	seq, err := w.AppendWithDatabaseReturningSeq(OpEntityCommit, walEntityCommitData{Payload: data}, "test")
	require.NoError(t, err)
	return seq
}

func spannedTestPayloads(t *testing.T, entries []WALEntry) [][]byte {
	t.Helper()
	out := make([][]byte, 0, len(entries))
	for _, entry := range entries {
		require.NotEqual(t, OperationType("record_part"), entry.Operation, "a reader saw a record part")
		var data walEntityCommitData
		require.NoError(t, decodeRecoveryPayload(entry.Data, &data))
		out = append(out, data.Payload)
	}
	return out
}

// spannedTestOperations creates n nodes of size bytes each: n*size well over
// one WAL frame.
func spannedTestOperations(n, size int) []Operation {
	ops := make([]Operation, n)
	text := strings.Repeat("y", size)
	for i := range ops {
		ops[i] = Operation{Type: OpCreateNode, Node: &Node{ID: NodeID(fmt.Sprintf("test:large-%d", i)), Labels: []string{"Large"}, Properties: map[string]any{"i": int64(i), "text": text}}}
	}
	return ops
}

func requireSpannedTestNodes(t *testing.T, b *BadgerEngine, n int) {
	t.Helper()
	count, err := b.NodeCount(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, n, count)
	if n > 0 {
		last, err := b.GetNode(t.Context(), NodeID(fmt.Sprintf("test:large-%d", n-1)))
		require.NoError(t, err)
		require.Equal(t, int64(n-1), last.Properties["i"])
	}
}

// The last part is durable and the commit was never applied: replay applies
// the whole transaction exactly once.
func TestSpannedEntityCommitReplaysOnceAfterCrashBeforeApplication(t *testing.T) {
	dir := t.TempDir()
	b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true, synchronous: true, manualDrain: true, records: WALRecordsLarge})
	w := b.bufferedCommitWAL
	payload, err := EncodeEntityTransaction(EntityTransactionRequest{Namespace: "test", LogicalID: "spanned-unapplied", Operations: spannedTestOperations(600, 40<<10), Mode: CommitModeExplicit})
	require.NoError(t, err)
	require.Greater(t, len(payload), int(walMaxEntrySize))
	require.NoError(t, w.AppendWithDatabase(OpEntityCommit, walEntityCommitData{Payload: payload}, "test"))
	requireSpannedTestNodes(t, b, 0)
	crashClose(t, b, w)

	b = newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true, synchronous: true, manualDrain: true, records: WALRecordsLarge})
	w = b.bufferedCommitWAL
	n, err := RecoverBufferedCommitsAtOpen(b, w)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	requireSpannedTestNodes(t, b, 600)
	n, err = RecoverBufferedCommitsAtOpen(b, w)
	require.NoError(t, err)
	require.Zero(t, n, "the receipt makes replay exactly once")
	requireSpannedTestNodes(t, b, 600)
}

// The record is durable and the storage apply of the large commit dies after
// some of its Badger batches: the open rolls the partial apply back and replay
// applies the whole transaction exactly once. Large values go to Badger's
// value log and barely count toward its batch limit, so the transaction also
// carries a chain of small nodes that needs several Badger batches.
func TestSpannedEntityCommitCrashDuringStorageApply(t *testing.T) {
	dir := t.TempDir()
	options := writeBehindTestOptions{dataDir: dir, wal: true, synchronous: true, manualDrain: true, records: WALRecordsLarge, lowMemory: true, finalOutcome: func(testing.TB, error) {}}
	b := newWriteBehindTestEngine(t, time.Hour, options)
	w := b.bufferedCommitWAL
	const small = 6000
	operations := spannedTestOperations(600, 40<<10)
	for i := 0; i < small; i++ {
		id := NodeID(fmt.Sprintf("test:small-%d", i))
		operations = append(operations, Operation{Type: OpCreateNode, Node: &Node{ID: id, Labels: []string{"Small"}, Properties: map[string]any{"i": int64(i), "pad": "0123456789abcdef"}}})
		if i > 0 {
			operations = append(operations, Operation{Type: OpCreateEdge, Edge: &Edge{ID: EdgeID(fmt.Sprintf("test:small-e-%d", i)), StartNode: NodeID(fmt.Sprintf("test:small-%d", i-1)), EndNode: id, Type: "NEXT"}})
		}
	}
	prepared, _, err := PrepareEntityChanges(t.Context(), b, "test", operations, CommitModeDirect)
	require.NoError(t, err)
	crashed := errors.New("simulated crash during storage apply")
	batches := 0
	restore := setLargeCommitBatchHook(func(batch int) error {
		batches++
		if batch == 2 {
			_ = b.db.DB.Close()
			return crashed
		}
		return nil
	})
	receipt, err := NewWALEngine(b, w).SubmitEntityCommit(context.Background(), prepared, nil)
	if err == nil {
		// The simulated crash stops the store mid-apply; the commit stays
		// pending recovery, as a killed process would leave it.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		outcome := receipt.WaitOutcome(ctx)
		cancel()
		err = outcome.Err
		t.Logf("interrupted apply: batches=%d state=%v err=%v", batches, outcome.State, err)
	} else {
		t.Logf("interrupted apply: batches=%d err=%v", batches, err)
	}
	restore()
	require.GreaterOrEqual(t, batches, 2, "the apply was a multi-batch large commit")
	entries, err := ReadWALEntriesFromDir(t.Context(), filepath.Join(dir, "wal"))
	require.NoError(t, err)
	var journaled bool
	for _, entry := range entries {
		journaled = journaled || entry.Operation == OpEntityCommit && len(entry.Data) > int(walMaxEntrySize)
	}
	require.True(t, journaled, "the spanned record was durable before the apply")
	_ = b.Close()
	_ = w.Close()

	b = newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true, synchronous: true, manualDrain: true, records: WALRecordsLarge, lowMemory: true})
	require.NoError(t, b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(largeCommitIntentKey)
		require.ErrorIs(t, err, badger.ErrKeyNotFound, "the interrupted apply was rolled back at open")
		return nil
	}))
	n, err := RecoverBufferedCommitsAtOpen(b, b.bufferedCommitWAL)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	requireWhole := func() {
		t.Helper()
		count, err := b.NodeCount(t.Context())
		require.NoError(t, err)
		require.EqualValues(t, 600+small, count)
		edges, err := b.EdgeCount(t.Context())
		require.NoError(t, err)
		require.EqualValues(t, small-1, edges)
		for _, id := range []NodeID{"test:large-599", NodeID(fmt.Sprintf("test:small-%d", small-1))} {
			_, err := b.GetNode(t.Context(), id)
			require.NoError(t, err)
		}
	}
	requireWhole()
	n, err = RecoverBufferedCommitsAtOpen(b, b.bufferedCommitWAL)
	require.NoError(t, err)
	require.Zero(t, n)
	requireWhole()
}

// A WAL frame whose length is over 16 MiB that is not a historical embedding
// audit record (the oversized frames an older writer without a write-side
// check could leave): this build refuses it as before. The spanned-record
// change reads such a frame exactly as its predecessor did.
func TestWALLegacyOversizedFrameBehaviourIsUnchanged(t *testing.T) {
	dir := t.TempDir()
	w := newSpannedTestWAL(t, dir, nil)
	appendSpannedTestRecord(t, w, []byte("first"))
	require.NoError(t, w.Close())
	active := walActivePath(dir)
	entry := WALEntry{Sequence: 99, Timestamp: time.Now(), Operation: OpEntityCommit, Database: "test"}
	entry.Data, _ = marshalWALRecoveryPayload(nil, walEntityCommitData{Payload: spannedTestData(20 << 20)})
	entry.Checksum = crc32Checksum(entry.Data)
	// An ordinary frame whose length is over the frame limit, built by hand.
	header, err := marshalJSONCompact(&bytes.Buffer{}, &WALEntry{Sequence: entry.Sequence, Timestamp: entry.Timestamp, Operation: entry.Operation, Checksum: entry.Checksum, Database: entry.Database})
	require.NoError(t, err)
	payload := binary.LittleEndian.AppendUint32(nil, uint32(len(header)))
	payload = append(payload, header...)
	payload = binary.LittleEndian.AppendUint32(payload, uint32(len(entry.Data)))
	payload = append(payload, entry.Data...)
	record, _ := buildAtomicRecord(payload, walFormatVersionCurrent)
	file, err := os.OpenFile(active, os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = file.Write(record)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	before, err := os.Stat(active)
	require.NoError(t, err)

	require.NoError(t, ValidateWALFormat(dir, WALValidateCurrent), "validation leaves a malformed frame to repair")
	_, err = ReadWALEntries(t.Context(), active, nil)
	require.ErrorIs(t, err, ErrWALCorrupted)
	t.Logf("LEGACY read error: %v", err)
	report, err := CheckWALIntegrity(active)
	require.NoError(t, err)
	require.False(t, report.Healthy)
	t.Logf("LEGACY integrity: %+v", report.Errors)
	_, _, err = repairWALTailIfNeeded(active, nil)
	require.NoError(t, err)
	after, err := os.Stat(active)
	require.NoError(t, err)
	t.Logf("LEGACY repair: size %d -> %d", before.Size(), after.Size())
	require.Less(t, after.Size(), before.Size(), "repair truncates the frame it cannot read, as before")
	entries, err := ReadWALEntries(t.Context(), active, nil)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("first")}, spannedTestPayloads(t, entries))
}

// A transaction whose journal record is larger than one WAL frame commits
// through the native path: the record is written as one spanned record, reads
// back as one logical entry and leaves the wrapper healthy. (The same
// transaction on a default WAL is refused:
// TestWALNativeOversizedRecordRejectionDoesNotFence.)
func TestWALNativeRecordLargerThanOneFrameCommits(t *testing.T) {
	native := openLargeCommitTestEngine(t, t.TempDir())
	wal := newSpannedTestWAL(t, t.TempDir(), nil)
	fixtureowner.OwnChecked(t, wal)
	wrapped := fixtureowner.OwnChecked(t, NewWALEngine(native, wal))
	nodes := make([]*Node, 512)
	payload := strings.Repeat("x", 32<<10)
	for i := range nodes {
		nodes[i] = &Node{ID: NodeID(fmt.Sprintf("test:frame-%d", i)), Properties: map[string]any{"payload": payload}}
	}
	require.NoError(t, wrapped.BulkCreateNodes(t.Context(), nodes))
	var commits int
	for _, entry := range walClientEntries(t, wal) {
		if entry.Operation == OpEntityCommit {
			commits++
			require.Greater(t, len(entry.Data), int(walMaxEntrySize), "one logical record larger than one frame")
		}
		require.NotEqual(t, OpRecordPart, entry.Operation, "readers see only the logical record")
	}
	require.Equal(t, 1, commits)
	require.Nil(t, native.db.oracle.failure())
	count, err := native.NodeCount(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, len(nodes), count)
	_, err = wrapped.CreateNode(t.Context(), &Node{ID: "test:healthy-after-large-record"})
	require.NoError(t, err)
}

// A prepared transaction whose record is larger than one frame is accepted,
// journaled once as a spanned record and applied. (On a default WAL it is
// refused: TestWALPreparedOversizedRecordRejectsWithoutAcceptedOwner.)
func TestWALPreparedRecordLargerThanOneFrameIsAccepted(t *testing.T) {
	native := newTestMemoryEngine(t)
	wal := newSpannedTestWAL(t, t.TempDir(), nil)
	fixtureowner.OwnChecked(t, wal)
	wrapped := fixtureowner.OwnChecked(t, NewWALEngine(native, wal))
	operations := make([]Operation, 512)
	payload := strings.Repeat("x", 32<<10)
	for i := range operations {
		operations[i] = Operation{Type: OpCreateNode, Node: &Node{ID: NodeID(fmt.Sprintf("test:prepared-frame-%d", i)), Properties: map[string]any{"payload": payload}}}
	}
	prepared, _, err := PrepareEntityChanges(t.Context(), native, "test", operations, CommitModeDirect)
	require.NoError(t, err)
	require.NotNil(t, prepared)
	receipt, err := wrapped.SubmitEntityCommit(t.Context(), prepared, nil)
	require.NoError(t, err)
	require.NoError(t, receipt.Wait(t.Context()))
	entries := walClientEntries(t, wal)
	require.Len(t, entries, 1)
	require.Equal(t, OpEntityCommit, entries[0].Operation)
	require.Greater(t, len(entries[0].Data), int(walMaxEntrySize))
	count, err := native.NodeCount(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, len(operations), count)
	pending := 0
	wrapped.pendingEntityCommits.Range(func(_, _ any) bool { pending++; return true })
	require.Zero(t, pending)
	_, err = wrapped.CreateNode(t.Context(), &Node{ID: "test:after-prepared-large-record"})
	require.NoError(t, err)
}

// A buffered commit that fits the decoded-payload limit but exceeds one WAL
// frame is written as one spanned record and reads back as one logical
// record. A larger decoded payload stays refused
// (TestBufferedWALRejectsOversizedTypedPayloadBeforeAppend), and a default WAL
// refuses this one (TestBufferedWALRejectsUnreadableOversizedFrameBeforeAppend).
func TestBufferedWALWritesRecordLargerThanOneFrame(t *testing.T) {
	b, _, commit := bindingCostFixture(t, 1)
	wal := newSpannedTestWAL(t, t.TempDir(), nil)
	t.Cleanup(func() { _ = wal.Close() })
	value := make([]byte, int(walMaxEntrySize)+4096)
	_, err := rand.New(rand.NewSource(42)).Read(value)
	require.NoError(t, err)
	commit.writes[string([]byte{prefixNode})+"incompressible"] = value
	require.NoError(t, wal.AppendBufferedCommit(commit, b.propKeyDict))
	entries, err := ReadWALEntriesFromDir(t.Context(), wal.Config().Dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, OpBufferedCommit, entries[0].Operation)
	require.Greater(t, len(entries[0].Data), int(walMaxEntrySize))
}
