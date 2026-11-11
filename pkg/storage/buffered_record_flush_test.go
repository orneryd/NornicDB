package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// NOR-412: with asynchronous writes an implicit commit is acknowledged from the
// write buffer. These tests take commits that are admitted to the write buffer
// and leave their buffered record out of the journal, and check what the
// journal's writer still holds in the process when such a commit returns. With
// DeferAppendFlush the journal's writer keeps appended records in the process
// until the next sync; nornicdb.Open sets it for asynchronous writes without
// strict durability and with batch sync.

// flushLabEngine opens an asynchronous engine whose journal writes large
// records with deferred flushing set as asked.
func flushLabEngine(t *testing.T, dir string, deferFlush bool) *BadgerEngine {
	t.Helper()
	b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true, manualDrain: true, records: WALRecordsLarge})
	b.bufferedCommitWAL.config.DeferAppendFlush = deferFlush
	return b
}

// journalOperations counts the journal's records of operation op.
func journalOperations(t *testing.T, dir string, op OperationType) int {
	t.Helper()
	entries, err := ReadWALEntriesFromDir(context.Background(), filepath.Join(dir, "wal"))
	require.NoError(t, err)
	count := 0
	for _, entry := range entries {
		if entry.Operation == op {
			count++
		}
	}
	return count
}

// killProcess drops what the journal's writer still holds in the process (the
// writer is reset without a flush) and then closes b with crashClose, which
// drops the write buffer without a drain and closes the store and the journal
// file.
func killProcess(t *testing.T, b *BadgerEngine) {
	t.Helper()
	w := b.bufferedCommitWAL
	w.mu.Lock()
	w.writer.Reset(w.file)
	w.mu.Unlock()
	crashClose(t, b, w)
}

// flushLabFloats returns a function that gives a node one property, values: a
// list of walSpannedChunkBytes*3/72 float64 values. A commit of eight such
// nodes is admitted to the write buffer and leaves its buffered record out of
// the journal (both asserted in the test below).
func flushLabFloats() func(int) map[string]any {
	values := make([]float64, walSpannedChunkBytes*3/(8*9))
	for i := range values {
		values[i] = float64(i) / 3
	}
	return func(int) map[string]any { return map[string]any{"values": values} }
}

// longNameNode stages the node test:admission-0 with a text of fill bytes and
// names integer properties whose names are nameBytes long and begin with salt
// and a number (a different salt gives names an earlier call did not use).
func longNameNode(salt string, names, nameBytes, fill int) func(context.Context, *BadgerTransaction) error {
	return func(ctx context.Context, tx *BadgerTransaction) error {
		properties := map[string]any{"text": strings.Repeat("f", fill)}
		for i := range names {
			name := fmt.Sprintf("%s%03d", salt, i)
			properties[name+strings.Repeat("n", nameBytes-len(name))] = int64(i)
		}
		_, err := tx.CreateNode(ctx, &Node{ID: "test:admission-0", Labels: []string{"Admission"}, Properties: properties})
		return err
	}
}

// admissionForecast stages stage in an implicit transaction, which it rolls back, and returns estimateBufferedPayloadBytes of it, called as bufferedForecastFitsLocked calls it.
func admissionForecast(t *testing.T, b *BadgerEngine, stage func(context.Context, *BadgerTransaction) error) int64 {
	t.Helper()
	ctx := context.Background()
	tx, err := BeginEntityTransaction(ctx, b, CommitModeImplicit)
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, stage(ctx, tx))
	return estimateBufferedPayloadBytes(tx.namespace, tx.ID, tx.physicalOperationsLocked(), tx.pendingWrites, tx.pendingDeletes, nil, propKeyTxnDrain{}, 0, 0, 0)
}

// For both fixtures, with DeferAppendFlush on and off: the commit returns
// applied with operations pending in the write buffer, and the journal's writer
// then holds no bytes in the process. After killProcess the journal file has no
// buffered-commit record and one transaction record of the commit, and after
// the recovery at open the store holds the commit's nodes.
func TestBufferedCommitWithoutItsRecordLeavesNoJournalBytesInTheProcess(t *testing.T) {
	type omitted struct {
		name  string
		nodes int
		stage func(t *testing.T, b *BadgerEngine) func(context.Context, *BadgerTransaction) error
	}
	cases := []omitted{
		// Eight nodes of flushLabFloats.
		{name: "eight nodes of float64 lists", nodes: 8, stage: func(*testing.T, *BadgerEngine) func(context.Context, *BadgerTransaction) error {
			return admissionNodes(8, flushLabFloats())
		}},
		// One node with 60 property names of 30,000 characters, sized so that
		// admission's forecast is within the write buffer's limit and that forecast
		// plus the names' bytes is over it (both asserted). walSpannedChunkBytes is
		// lowered to 48 KiB for this case.
		{name: "one node with long new property names", nodes: 1, stage: func(t *testing.T, b *BadgerEngine) func(context.Context, *BadgerTransaction) error {
			previous := walSpannedChunkBytes
			walSpannedChunkBytes = 48 << 10
			t.Cleanup(func() { walSpannedChunkBytes = previous })
			const names, nameBytes = 60, 30_000
			low := admissionForecast(t, b, longNameNode("x", names, nameBytes, 1<<20))
			high := admissionForecast(t, b, longNameNode("y", names, nameBytes, 2<<20))
			perByte := (high - low) / (1 << 20)
			require.Positive(t, perByte)
			// The forecast is set half the names' bytes below the limit.
			fill := 1<<20 + int((maxBufferedWALPayloadBytes-names*nameBytes/2-low)/perByte)
			forecast := admissionForecast(t, b, longNameNode("z", names, nameBytes, fill))
			require.LessOrEqual(t, forecast, int64(maxBufferedWALPayloadBytes), "admission's forecast is within the write buffer's limit")
			require.Greater(t, forecast+names*nameBytes, int64(maxBufferedWALPayloadBytes), "the forecast plus the names' bytes is over the limit")
			return longNameNode("r", names, nameBytes, fill)
		}},
	}
	for _, c := range cases {
		for _, deferFlush := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/deferred flush=%v", c.name, deferFlush), func(t *testing.T) {
				dir := t.TempDir()
				b := flushLabEngine(t, dir, deferFlush)
				stage := c.stage(t, b)
				require.NoError(t, admissionCommit(t, b, CommitModeImplicit, "omitted-record", stage), "the commit is acknowledged")
				require.Positive(t, b.writeBehind.PendingOps(), "from the write buffer")
				w := b.bufferedCommitWAL
				w.mu.Lock()
				inProcess := w.writer.Buffered()
				w.mu.Unlock()
				require.Zero(t, inProcess, "the journal's writer holds no bytes in the process after the commit returned")

				killProcess(t, b)
				require.Zero(t, journalOperations(t, dir, OpBufferedCommit), "the commit's buffered record was left out")
				require.Equal(t, 1, admissionRecords(t, dir, "omitted-record"), "the journal file holds the commit's transaction record")
				b = flushLabEngine(t, dir, deferFlush)
				_, err := RecoverBufferedCommitsAtOpen(b, b.bufferedCommitWAL)
				require.NoError(t, err)
				require.Equal(t, c.nodes, admissionStored(t, b), "the store holds the commit's nodes after killProcess and the recovery at open")
			})
		}
	}
}

// failingTailWriter passes writes of 64 KiB or more through and fails shorter
// ones.
type failingTailWriter struct {
	io.Writer
	err error
}

func (w failingTailWriter) Write(p []byte) (int, error) {
	if len(p) < 64<<10 {
		return 0, w.err
	}
	return w.Writer.Write(p)
}

// With DeferAppendFlush on and a journal writer that fails every write shorter
// than 64 KiB, an implicit commit of eight flushLabFloats nodes returns an
// error, the write buffer has no pending operation and no Admission node is
// visible. The test does not identify which write failed. It also passes on
// this PR's previous head (bb48f4a85), which did not have the flush this PR
// adds, so it does not show that flush.
func TestBufferedCommitWithoutItsRecordFailsWhenAShortJournalWriteFails(t *testing.T) {
	// The fixture's close can report the failed commit, so its outcome is not checked.
	b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{wal: true, manualDrain: true, records: WALRecordsLarge, finalOutcome: func(testing.TB, error) {}})
	w := b.bufferedCommitWAL
	w.config.DeferAppendFlush = true
	injected := errors.New("injected flush failure")
	w.mu.Lock()
	require.NoError(t, w.writer.Flush())
	w.writer.Reset(failingTailWriter{Writer: w.file, err: injected})
	w.mu.Unlock()
	err := admissionCommit(t, b, CommitModeImplicit, "failed-flush", admissionNodes(8, flushLabFloats()))
	require.Error(t, err, "the commit returns an error")
	t.Logf("reported: %v", err)
	require.Zero(t, b.writeBehind.PendingOps(), "the write buffer has no pending operation")
	nodes, err := b.GetNodesByLabel(context.Background(), "Admission")
	require.NoError(t, err)
	require.Empty(t, nodes, "and no Admission node is visible")
	w.mu.Lock()
	w.writer.Reset(w.file)
	w.mu.Unlock()
}

// BenchmarkJournalFlushOfAShortLastPart appends a 2 KiB record to a journal
// with DeferAppendFlush on. With flush=true it calls the journal's
// flushAppended after each append, the call a buffered commit without its
// record makes; with flush=false the record stays in the journal's writer.
func BenchmarkJournalFlushOfAShortLastPart(b *testing.B) {
	for _, flush := range []bool{false, true} {
		b.Run(fmt.Sprintf("flush=%v", flush), func(b *testing.B) {
			b.Cleanup(config.WithWALEnabled())
			cfg := DefaultWALConfig()
			cfg.Dir = b.TempDir()
			cfg.SyncMode = "batch"
			cfg.BatchSyncInterval = time.Hour
			cfg.DeferAppendFlush = true
			cfg.RecordPolicy = WALRecordsLarge
			w, err := NewWAL(cfg.Dir, cfg)
			require.NoError(b, err)
			b.Cleanup(func() { _ = w.Close() })
			record := walEntityCommitData{Payload: make([]byte, 2<<10)}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := w.AppendWithDatabase(OpEntityCommit, record, "test"); err != nil {
					b.Fatal(err)
				}
				if flush {
					if err := w.flushAppended(); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
