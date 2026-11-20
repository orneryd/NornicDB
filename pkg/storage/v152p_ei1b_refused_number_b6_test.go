package storage

// Untracked probe (batch 1b, follow-up run 4 / entry 46, previous build): the
// direct journal tests of entry 46 (wt-bug42/pkg/storage/wal_sequence_written_test.go,
// TestWALRefusedRecordTakesNoNumber, ...ThenSnapshotSealReopenAndRollOver,
// ...DoesNotHideALaterRecordFromSnapshotRecovery) ported to b6d8e762a's API.
// Prints outcomes; asserts nothing.
//
// Forms that exist on this build: an ordinary append over the 16 MiB frame
// limit; a batch whose only record is over the limit; a batch of two whose
// second is over it; a buffered commit over the limit. Not on this build: the
// "large record declined" form (no appendDecidingLargeRecord / WALRecordsLarge)
// and the "property dictionary required" form (AppendBufferedCommit takes no
// dictionary here). The seal is this build's TruncateAfterSnapshot with
// retained segments (it seals the active file up to the journal's number,
// wal.go:1497-1510), the step automatic compaction takes on this build.

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
)

type zzEI1bRefusalB6 struct {
	name    string
	written uint64
	refuse  func(w *WAL) error
}

func zzEI1bRefusalsB6() []zzEI1bRefusalB6 {
	over := func() map[string]any { return map[string]any{"blob": make([]byte, int(walMaxEntrySize))} }
	return []zzEI1bRefusalB6{
		{name: "ordinary_frame_size", refuse: func(w *WAL) error {
			_, err := w.AppendReturningSeq(OpCheckpoint, over())
			return err
		}},
		{name: "batch_frame_size", refuse: func(w *WAL) error {
			b := w.NewBatch()
			if err := b.append(OpCheckpoint, over(), ""); err != nil {
				return fmt.Errorf("append: %w", err)
			}
			_, _, err := b.CommitWithSeq()
			return err
		}},
		{name: "batch_second_entry_frame_size", written: 1, refuse: func(w *WAL) error {
			b := w.NewBatch()
			if err := b.append(OpCheckpoint, map[string]any{"value": "batch entry before the refused one"}, ""); err != nil {
				return err
			}
			if err := b.append(OpCheckpoint, over(), ""); err != nil {
				return fmt.Errorf("append: %w", err)
			}
			_, _, err := b.CommitWithSeq()
			return err
		}},
		{name: "buffered_commit_frame_size", refuse: func(w *WAL) error {
			raw := make([]byte, 17<<20)
			rand.New(rand.NewSource(1801)).Read(raw)
			c := bufferedCommit{txID: "refused", namespace: "test", writes: map[string][]byte{"k": raw}}
			return w.AppendBufferedCommit(&c, newPropertyKeyDictionary())
		}},
	}
}

func TestZZEI1bRefusedNumberB6(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, r := range zzEI1bRefusalsB6() {
		// 1. Number check.
		func() {
			cfg := DefaultWALConfig()
			cfg.SyncMode = "immediate"
			w, err := NewWAL(t.TempDir(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = w.Close() }()
			first, _ := w.AppendReturningSeq(OpCheckpoint, map[string]any{"value": "accepted"})
			rerr := r.refuse(w)
			seq := w.Sequence()
			next, nerr := w.AppendReturningSeq(OpCheckpoint, map[string]any{"value": "after the refusal"})
			t.Logf("ZZEI1B-B6 number %s: first=%d refusal err=%v; sequence after=%d (records written=%d); next append=%d err=%v", r.name, first, rerr, seq, first+r.written, next, nerr)
		}()
		// 2. Seal with retained segments, reopen, roll-overs.
		func() {
			dir := t.TempDir()
			cfg := DefaultWALConfig()
			cfg.SyncMode = "immediate"
			cfg.MaxEntries, cfg.MaxFileSize = 0, 0
			cfg.RetentionMaxSegments, cfg.RetentionMaxAge = 100, time.Hour
			w, err := NewWAL(dir, cfg)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = w.AppendReturningSeq(OpCheckpoint, map[string]any{"value": "accepted"})
			rerr := r.refuse(w)
			sealSeq := w.Sequence()
			terr := w.ManageSnapshot(context.Background(), sealSeq, WALSnapshotTruncate)
			_ = w.Close()
			segs, _ := os.ReadDir(filepath.Join(dir, "segments"))
			var names []string
			for _, s := range segs {
				names = append(names, s.Name())
			}
			cfg.MaxEntries = 2
			re, err := NewWAL(dir, cfg)
			if err != nil {
				t.Logf("ZZEI1B-B6 seal %s: refusal err=%v seal at %d err=%v segments=%v; reopen error %v", r.name, rerr, sealSeq, terr, names, err)
				return
			}
			reSeq := re.Sequence()
			var appendErrs []string
			for i := 0; i < 5; i++ {
				seq, err := re.AppendReturningSeq(OpCheckpoint, map[string]any{"value": fmt.Sprintf("after reopen %d", i)})
				appendErrs = append(appendErrs, fmt.Sprintf("%d:%v", seq, err))
			}
			_ = re.Close()
			segs, _ = os.ReadDir(filepath.Join(dir, "segments"))
			var after []string
			for _, s := range segs {
				after = append(after, s.Name())
			}
			t.Logf("ZZEI1B-B6 seal %s: refusal err=%v; seal at %d err=%v; segments after seal=%v; reopen sequence=%d; five appends (number:error)=%v; segments after=%v", r.name, rerr, sealSeq, terr, names, reSeq, appendErrs, after)
		}()
		// 3. Snapshot at the journal's number, reopen, a new node, recovery.
		func() {
			dir := t.TempDir()
			cfg := DefaultWALConfig()
			cfg.SyncMode = "immediate"
			cfg.RetentionMaxSegments, cfg.RetentionMaxAge = 100, time.Hour
			store := newTestEngine(t)
			w, err := NewWAL(dir, cfg)
			if err != nil {
				t.Fatal(err)
			}
			_, cerr := NewWALEngine(store, w).CreateNode(context.Background(), &Node{ID: "test:before", Properties: map[string]any{"v": int64(1)}})
			rerr := r.refuse(w)
			snap := filepath.Join(t.TempDir(), "snapshot")
			serr := SaveStreamingSnapshot(context.Background(), store, snap, SnapshotOptions{Sequence: w.Sequence()})
			terr := w.ManageSnapshot(context.Background(), w.Sequence(), WALSnapshotTruncate)
			_ = w.Close()
			re, err := NewWAL(dir, cfg)
			if err != nil {
				t.Logf("ZZEI1B-B6 recovery %s: reopen error %v", r.name, err)
				return
			}
			_, aerr := NewWALEngine(store, re).CreateNode(context.Background(), &Node{ID: "test:after", Properties: map[string]any{"v": int64(2)}})
			_ = re.Close()
			fresh := newTestEngine(t)
			result, _, recErr := RecoverIntoEngine(context.Background(), fresh, dir, snap)
			before, berr := fresh.GetNode(context.Background(), "test:before")
			afterNode, afErr := fresh.GetNode(context.Background(), "test:after")
			t.Logf("ZZEI1B-B6 recovery %s: create before err=%v; refusal err=%v; snapshot err=%v; seal err=%v; create after err=%v; recovery err=%v applied=%d failed=%d; test:before found=%v (%v); test:after found=%v (%v)",
				r.name, cerr, rerr, serr, terr, aerr, recErr, result.Applied, result.Failed, before != nil, berr, afterNode != nil, afErr)
		}()
	}
}
