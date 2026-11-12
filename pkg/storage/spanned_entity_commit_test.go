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

// NOR-412: transaction records larger than one 16 MiB WAL frame on an engine
// whose journal writes large records (WALRecordsLarge): what the recovery at
// open does with one that was journaled and not applied, or applied in part,
// and how a hand-built ordinary frame over the frame limit is read and
// repaired.

// spannedTestData returns n bytes; byte i is i*7 + i/4096,
// modulo 256.
func spannedTestData(n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte(i*7 + i/4096)
	}
	return out
}

// newSpannedTestWAL opens a WAL that writes large records, with immediate sync.
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

// spannedTestOperations returns n create-node operations: nodes test:large-0 to
// test:large-(n-1) with the label Large, an index i and a text of size bytes.
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

// A record of 600 nodes of 40 KB is appended to the journal and not applied,
// and the engine is closed with crashClose (which closes the journal file
// normally). The recovery at open applies one record and the store has the 600
// nodes; a second recovery applies none.
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
	require.Zero(t, n, "a second recovery applies no record")
	requireSpannedTestNodes(t, b, 600)
}

// On a low-memory store the apply of a large commit is stopped at its second
// Badger batch by closing the store under it. The journal then holds the
// transaction's record. After a reopen the large commit's intent key is gone,
// the recovery at open applies one record and the store has all the
// transaction's nodes and relationships; a second recovery applies none. The
// transaction carries a chain of small nodes so that its apply takes several
// Badger batches (asserted: at least two).
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
		// The store was closed under the apply; the outcome is logged, not asserted.
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
	require.True(t, journaled, "the journal holds the transaction's record")
	_ = b.Close()
	_ = w.Close()

	b = newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true, synchronous: true, manualDrain: true, records: WALRecordsLarge, lowMemory: true})
	require.NoError(t, b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(largeCommitIntentKey)
		require.ErrorIs(t, err, badger.ErrKeyNotFound, "the large commit's intent key is gone after the reopen")
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

// An ordinary frame (version 6) whose length is over 16 MiB, built by hand and
// appended after a good record: ValidateWALFormat accepts the directory,
// ReadWALEntries returns ErrWALCorrupted, CheckWALIntegrity reports the file
// unhealthy, and repairWALTailIfNeeded makes the file shorter, after which
// ReadWALEntries returns the good record.
func TestWALOversizedOrdinaryFrameIsRefusedAndRepaired(t *testing.T) {
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

	require.NoError(t, ValidateWALFormat(dir, WALValidateCurrent), "ValidateWALFormat accepts the directory")
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
	require.Less(t, after.Size(), before.Size(), "the repair makes the file shorter")
	entries, err := ReadWALEntries(t.Context(), active, nil)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("first")}, spannedTestPayloads(t, entries))
}

// BulkCreateNodes of 512 nodes of 32 KB through a WAL engine whose journal
// writes large records: the journal reads back one transaction record, larger
// than a frame, and no part record; the store has the nodes and reports no
// failure; a further CreateNode succeeds.
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
		require.NotEqual(t, OpRecordPart, entry.Operation, "the reader returned a part record")
	}
	require.Equal(t, 1, commits)
	require.Nil(t, native.db.oracle.failure())
	count, err := native.NodeCount(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, len(nodes), count)
	_, err = wrapped.CreateNode(t.Context(), &Node{ID: "test:healthy-after-large-record"})
	require.NoError(t, err)
}

// A prepared transaction of 512 nodes of 32 KB submitted to a WAL engine whose
// journal writes large records: the journal reads back one transaction record,
// larger than a frame; the store has the nodes; no entity commit is left
// pending; a further CreateNode succeeds.
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

// AppendBufferedCommit with a value of one frame plus 4 KiB of random bytes, on
// a WAL that writes large records: the journal reads back one buffered-commit
// record, larger than a frame.
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
