//go:build closeout_l2_e34

package storage

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// Report entry 34: the open-time recovery routine (RecoverBufferedCommitsAtOpen,
// called by nornicdb.Open) decodes every journaled entity_commit record and its
// complete transaction image before it looks up whether that commit was
// already applied. On a cleanly closed store it replays nothing, yet its cost
// grows with the journal. The entry's open timings compare two builds; in one
// process this test measures the routine itself, against the bare read of the
// same journal.

const (
	l2E34Transactions = 20  // the entry's small store: ten statements of 1,000 rows
	l2E34NodesPerTx   = 500 // 10,000 small nodes in 20 entity commits
	l2E34Calls        = 5   // measured calls; medians are used
)

type l2E34Store struct {
	dir    string
	engine *BadgerEngine
	wal    *WAL
	closed bool
}

func l2E34Open(t *testing.T, dir string) *l2E34Store {
	t.Helper()
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: filepath.Join(dir, "data")})
	require.NoError(t, err)
	cfg := DefaultWALConfig()
	cfg.Dir = filepath.Join(dir, "wal")
	cfg.SyncMode = "immediate"
	wal, err := NewWAL(cfg.Dir, cfg)
	if err != nil {
		_ = engine.Close()
		require.NoError(t, err)
	}
	s := &l2E34Store{dir: dir, engine: engine, wal: wal}
	t.Cleanup(func() {
		if !s.closed {
			s.closed = true
			_ = engine.Close()
			_ = wal.Close()
		}
	})
	return s
}

func (s *l2E34Store) close(t *testing.T) {
	t.Helper()
	if s.closed {
		return
	}
	s.closed = true
	engineErr := s.engine.Close()
	walErr := s.wal.Close()
	require.NoError(t, engineErr)
	require.NoError(t, walErr)
}

func (s *l2E34Store) walDir() string { return filepath.Join(s.dir, "wal") }

// l2E34Write commits the nodes through the managed entity-commit path the
// database manager uses: BeginEntityTransaction, PrepareEntityCommit and the
// journal engine's SubmitEntityCommit (pkg/multidb/storage_transaction.go).
func l2E34Write(t *testing.T, s *l2E34Store) {
	t.Helper()
	ctx := context.Background()
	w := NewWALEngine(s.engine, s.wal)
	for k := 0; k < l2E34Transactions; k++ {
		tx, err := BeginEntityTransaction(ctx, w, CommitModeExplicit)
		require.NoError(t, err)
		require.NoError(t, tx.SetNamespace("test"))
		for j := 0; j < l2E34NodesPerTx; j++ {
			i := k*l2E34NodesPerTx + j
			_, err := tx.CreateNode(ctx, &Node{
				ID:         NodeID(fmt.Sprintf("test:s-%d", i)),
				Labels:     []string{"Start"},
				Properties: map[string]any{"id": int64(i)},
			})
			require.NoError(t, err)
		}
		p, err := tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: fmt.Sprintf("l2-e34-%d", k)})
		require.NoError(t, err)
		t.Cleanup(p.Abort)
		receipt, err := w.SubmitEntityCommit(ctx, p, nil)
		require.NoError(t, err)
		waitCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		err = receipt.Wait(waitCtx)
		cancel()
		require.NoError(t, err)
	}
}

// l2E34Prepare writes the store, closes it cleanly and opens it again, running
// the open-time recovery once as nornicdb.Open does.
func l2E34Prepare(t *testing.T) (*l2E34Store, int) {
	t.Helper()
	t.Cleanup(config.WithWALEnabled())
	dir := t.TempDir()
	s := l2E34Open(t, dir)
	l2E34Write(t, s)
	s.close(t)
	s = l2E34Open(t, dir)
	first, err := RecoverBufferedCommitsAtOpen(s.engine, s.wal)
	require.NoError(t, err)
	return s, first
}

func l2E34JournalBytes(t *testing.T, dir string) int64 {
	t.Helper()
	var total int64
	require.NoError(t, filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			info, err := d.Info()
			if err != nil {
				return err
			}
			total += info.Size()
		}
		return nil
	}))
	return total
}

// l2E34Measure returns the bytes allocated (TotalAlloc delta) and the elapsed
// time of one call of f.
func l2E34Measure(f func()) (uint64, time.Duration) {
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now()
	f()
	elapsed := time.Since(start)
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc, elapsed
}

func l2E34MedianU(values []uint64) uint64 {
	sorted := append([]uint64(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return sorted[len(sorted)/2]
}

func l2E34MedianD(values []time.Duration) time.Duration {
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return sorted[len(sorted)/2]
}

type l2E34Figures struct {
	journalBytes   int64
	entityRecords  int
	firstReplayed  int
	replayed       []int
	recoverAlloc   uint64
	recoverTime    time.Duration
	visitAlloc     uint64
	visitTime      time.Duration
	allocRatio     float64
	recoverAllocs  []uint64
	visitAllocList []uint64
}

func l2E34Run(t *testing.T) (*l2E34Store, l2E34Figures) {
	t.Helper()
	s, first := l2E34Prepare(t)
	ctx := context.Background()
	f := l2E34Figures{journalBytes: l2E34JournalBytes(t, s.walDir()), firstReplayed: first}
	var visitTimes, recoverTimes []time.Duration
	for i := 0; i < l2E34Calls; i++ {
		records := 0
		alloc, elapsed := l2E34Measure(func() {
			require.NoError(t, VisitWALEntriesAfterFromDir(ctx, s.walDir(), 0, func(e WALEntry) error {
				if e.Operation == OpEntityCommit {
					records++
				}
				return nil
			}))
		})
		f.entityRecords = records
		f.visitAllocList = append(f.visitAllocList, alloc)
		visitTimes = append(visitTimes, elapsed)
	}
	for i := 0; i < l2E34Calls; i++ {
		var replayed int
		alloc, elapsed := l2E34Measure(func() {
			n, err := RecoverBufferedCommitsAtOpen(s.engine, s.wal)
			require.NoError(t, err)
			replayed = n
		})
		f.replayed = append(f.replayed, replayed)
		f.recoverAllocs = append(f.recoverAllocs, alloc)
		recoverTimes = append(recoverTimes, elapsed)
	}
	f.visitAlloc, f.visitTime = l2E34MedianU(f.visitAllocList), l2E34MedianD(visitTimes)
	f.recoverAlloc, f.recoverTime = l2E34MedianU(f.recoverAllocs), l2E34MedianD(recoverTimes)
	if f.visitAlloc > 0 {
		f.allocRatio = float64(f.recoverAlloc) / float64(f.visitAlloc)
	}
	return s, f
}

// TestL2_e34_Repro: on a cleanly closed store whose journal holds 20 applied
// entity_commit records (10,000 nodes), each further call of the recovery
// routine replays 0 commits. Threshold: the routine is a defect when it
// allocates at least 2.0 times what the bare read of the same journal
// (VisitWALEntriesAfterFromDir with an empty visitor, the floor any recovery
// that reads the journal pays) allocates. Decoding the complete images of
// small nodes allocates several times their encoded size (the entry: 57 MB
// per call on an 8,148,504-byte journal, about 7 times), while a routine that
// skips applied records before decoding them adds only receipt lookups to the
// bare read.
func TestL2_e34_Repro(t *testing.T) {
	_, f := l2E34Run(t)
	zero := true
	for _, n := range f.replayed {
		if n != 0 {
			zero = false
		}
	}
	line := fmt.Sprintf("journal %d B, %d entity_commit records, first recovery replayed %d, measured calls replayed %v; recovery median alloc %d B (%v) time %v; bare journal read median alloc %d B (%v) time %v; ratio %.2f",
		f.journalBytes, f.entityRecords, f.firstReplayed, f.replayed,
		f.recoverAlloc, f.recoverAllocs, f.recoverTime, f.visitAlloc, f.visitAllocList, f.visitTime, f.allocRatio)
	if !zero {
		t.Fatalf("premise: a cleanly closed store must replay 0 commits; %s", line)
	}
	if f.allocRatio >= 2.0 {
		t.Logf("L2RESULT E34 DEFECT %s", line)
		t.Errorf("recovery of an applied journal allocates %.2f times the bare journal read (threshold 2.0) while replaying nothing", f.allocRatio)
		return
	}
	t.Logf("L2RESULT E34 OK %s", line)
}

// TestL2_e34_Control: the harness writes and recovers correctly: the journal
// holds one entity_commit per transaction, the clean reopen replays nothing,
// and every node is stored.
func TestL2_e34_Control(t *testing.T) {
	s, f := l2E34Run(t)
	count, err := s.engine.NodeCount(context.Background())
	require.NoError(t, err)
	t.Logf("L2CONTROL E34 entity_commit records %d, first replay %d, node count %d, journal %d B", f.entityRecords, f.firstReplayed, count, f.journalBytes)
	require.Equal(t, l2E34Transactions, f.entityRecords)
	require.Equal(t, 0, f.firstReplayed)
	require.EqualValues(t, l2E34Transactions*l2E34NodesPerTx, count)
}

// TestL2_e34_Cause: a test-side copy of the routine's entity_commit branch
// (pkg/storage/badger_write_behind_recovery.go:100-115 at the pin): for every
// record it decodes the record and its transaction image first, then reads
// the receipt. It logs the bytes the decode step allocates, how many receipts
// were already present (every one: all decodes were wasted), and that share of
// the routine's own allocation.
func TestL2_e34_Cause(t *testing.T) {
	s, f := l2E34Run(t)
	ctx := context.Background()
	records, present, ops := 0, 0, 0
	var decodeAlloc uint64
	require.NoError(t, VisitWALEntriesAfterFromDir(ctx, s.walDir(), 0, func(e WALEntry) error {
		if e.Operation != OpEntityCommit {
			return nil
		}
		records++
		var envelope EntityTransactionEnvelope
		var decodeErr error
		alloc, _ := l2E34Measure(func() {
			var record walEntityCommitData
			if decodeErr = decodeRecoveryPayload(e.Data, &record); decodeErr != nil {
				return
			}
			var operations []Operation
			envelope, operations, decodeErr = record.decode()
			ops += len(operations)
		})
		if decodeErr != nil {
			return decodeErr
		}
		decodeAlloc += alloc
		receipt, err := s.engine.observeBufferedCommitReceipt(ctx, bufferedReceiptRead{namespace: envelope.Namespace, logicalID: envelope.LogicalTxID})
		if err != nil {
			return err
		}
		if receipt.present {
			present++
		}
		return nil
	}))
	share := 0.0
	if extra := int64(f.recoverAlloc) - int64(f.visitAlloc); extra > 0 {
		share = float64(decodeAlloc) / float64(extra)
	}
	t.Logf("L2CAUSE E34 %d entity_commit records decoded (%d operations) before the receipt check; receipts already present %d of %d; decode step allocated %d B; recovery median %d B, bare read %d B; decode share of the routine's extra allocation %.2f",
		records, ops, present, records, decodeAlloc, f.recoverAlloc, f.visitAlloc, share)
	if present != records {
		t.Errorf("premise: every record of the clean store is applied, but only %d of %d receipts are present", present, records)
	}
}
