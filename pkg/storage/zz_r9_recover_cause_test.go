package storage

// R9-1 cause diagnostic (campaign scratch test, no product change; pin e8cd436e5 only).
// Store: the v1.5.0 asynchronous store "asyncdocs-snap-clean" written by the upgrade lane, used as found
// (the order-B equivalent: snapshot + journal of the old store replayed into a fresh engine).
// Variants, interleaved in every iteration, each on its own copy:
//   V1 RecoverIntoEngine into a fresh engine with write-behind ON (the options nornicdb.Open gives an
//      async store: WriteBehind, interval 0 = auto, max ops 0 = auto, HighPerformance, caches 10000/50).
//   V2 the same with write-behind OFF.
//   V3 a copy of RecoverIntoEngine (wal_recovery_stream.go:30-128 at e8cd436e5) that timestamps every
//      journal entry's replay and, through a wrapper around WriteBehindBuffer.apply (the background
//      generation apply), every generation apply; it prints, for each failed entry and for seq 164,
//      whether a generation apply overlapped that entry's replay, and the PendingOps seen at its start.
//   V4 the same copy, but before each buffered_commit entry it flushes write-behind and waits until no
//      generation apply is in flight (quiescent destination), then replays.
// Env: R9_CAUSE_AREA (scratch dir under /work/tnrun/v152-upg), R9_CAUSE_N (iterations, default 10).

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Store: R9_ROOT (default /work/tnrun/v152-upg, the lab tree) + R9_DIAG_STORE (default
// v150/uor/stores/asyncdocs-snap-clean) + data. R9_CAUSE_AREA: scratch below R9_ROOT (default a fresh
// r9cause-* directory). Fail-closed: a missing store or snapshot fails; the test fails when V1 or V3 fails.
var r9CauseSource string

// r9EntryHook, when set, runs in r9RecoverCopy before each journal entry's replay (overlap-inducing test only).
var r9EntryHook func(WALEntry)

type r9Span struct {
	start, end time.Time
	ops        int
	err        error
}

type r9ApplyLog struct {
	mu       sync.Mutex
	spans    []r9Span
	inflight atomic.Int64
}

func (l *r9ApplyLog) wrap(b *BadgerEngine) {
	if b.writeBehind == nil {
		return
	}
	// set before any write reaches the buffer (the engine is fresh), so no apply runs concurrently with this
	orig := b.writeBehind.apply
	b.writeBehind.apply = func(buf *CommitBuffer) error {
		l.inflight.Add(1)
		s := r9Span{start: time.Now(), ops: buf.ops}
		err := orig(buf)
		s.end, s.err = time.Now(), err
		l.inflight.Add(-1)
		l.mu.Lock()
		l.spans = append(l.spans, s)
		l.mu.Unlock()
		return err
	}
}

func (l *r9ApplyLog) overlapping(t0, t1 time.Time) []r9Span {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []r9Span
	for _, s := range l.spans {
		if s.start.Before(t1) && s.end.After(t0) {
			out = append(out, s)
		}
	}
	return out
}

func r9CauseOptions(dir string, writeBehind bool) BadgerOptions {
	return BadgerOptions{
		DataDir:               dir,
		HighPerformance:       true,
		NodeCacheMaxEntries:   10000,
		EdgeTypeCacheMaxTypes: 50,
		AllowStorageUpgrade:   true,
		WriteBehind:           writeBehind,
	}
}

func r9CauseLatestSnapshot(t *testing.T, data string) string {
	snaps, _ := filepath.Glob(filepath.Join(data, "snapshots", "snapshot-*.json"))
	sort.Strings(snaps)
	if len(snaps) == 0 {
		t.Fatalf("no snapshot in %s", data)
	}
	return snaps[len(snaps)-1]
}

type r9EntryTiming struct {
	seq     uint64
	op      OperationType
	t0, t1  time.Time
	pending int
	err     error
}

// r9RecoverCopy is RecoverIntoEngine (wal_recovery_stream.go:30-128 at e8cd436e5) with timing hooks and,
// when quiesce is set, a wait for a quiescent write-behind buffer before each buffered_commit replay.
func r9RecoverCopy(ctx context.Context, destination Engine, walDir, snapshotPath string, log *r9ApplyLog, quiesce bool) (ReplayResult, []r9EntryTiming, error) {
	ctx = context.WithoutCancel(ctx)
	result := ReplayResult{Errors: make([]ReplayError, 0)}
	var timings []r9EntryTiming
	if err := ValidateWALFormat(walDir, WALValidateCurrent); err != nil {
		return result, nil, err
	}
	var snapshotSeq uint64
	if err := markRecoveryPropertyForeign(destination); err != nil {
		return result, nil, err
	}
	if snapshotPath != "" {
		streaming, err := IsStreamingSnapshot(snapshotPath)
		if err != nil && !os.IsNotExist(err) {
			return result, nil, err
		}
		if streaming {
			file, err := os.Open(snapshotPath)
			if err != nil {
				return result, nil, err
			}
			if err := beginRecoveryPropertyRestore(destination); err != nil {
				file.Close()
				return result, nil, err
			}
			visitor := newRecoverySnapshotVisitor(destination, recoveryBatchSize)
			metadata, readErr := readSnapshot(ctx, file, visitor, nil, materializedSnapshotRecordSize)
			if readErr == nil {
				readErr = visitor.Flush(ctx)
			}
			file.Close()
			if readErr != nil {
				return result, nil, readErr
			}
			snapshotSeq = metadata.Sequence
		} else {
			snapshot, err := LoadSnapshot(ctx, snapshotPath)
			if err != nil {
				if !os.IsNotExist(err) {
					return result, nil, err
				}
			} else {
				if err := beginRecoveryPropertyRestore(destination); err != nil {
					return result, nil, err
				}
				snapshotSeq = snapshot.Sequence
				if err := restoreLegacySnapshot(ctx, destination, snapshot); err != nil {
					return result, nil, err
				}
			}
		}
		if flusher, ok := UnwrapEngine(destination).(interface{ FlushWriteBehind(context.Context) error }); ok {
			if err := flusher.FlushWriteBehind(ctx); err != nil {
				return result, nil, err
			}
		}
	}
	activePath := walActivePath(walDir)
	if _, _, err := repairWALTailIfNeeded(activePath, defaultWALLogger{}); err != nil {
		return result, nil, err
	}
	b, _ := UnwrapEngine(destination).(*BadgerEngine)
	err := VisitWALEntriesAfterFromDir(ctx, walDir, snapshotSeq, func(entry WALEntry) error {
		if entry.Operation == OpCheckpoint {
			result.Skipped++
			return nil
		}
		if r9EntryHook != nil {
			r9EntryHook(entry)
		}
		if quiesce && entry.Operation == OpBufferedCommit && b != nil && b.writeBehind != nil {
			if err := b.FlushWriteBehind(ctx); err != nil {
				return err
			}
			deadline := time.Now().Add(10 * time.Second)
			for (log.inflight.Load() > 0 || b.writeBehind.PendingOps() > 0) && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
		}
		et := r9EntryTiming{seq: entry.Sequence, op: entry.Operation, t0: time.Now()}
		if b != nil && b.writeBehind != nil {
			et.pending = b.writeBehind.PendingOps()
		}
		rerr := replayWALEntry(ctx, destination, entry)
		et.t1, et.err = time.Now(), rerr
		timings = append(timings, et)
		if rerr == nil {
			result.Applied++
		} else if errors.Is(rerr, ErrAlreadyExists) {
			result.Skipped++
		} else {
			result.Failed++
			result.Errors = append(result.Errors, ReplayError{Sequence: entry.Sequence, Operation: entry.Operation, Error: rerr})
		}
		return nil
	})
	if result.Applied > 0 && b != nil {
		err = b.completeRecoveredCounts(ctx, err)
	}
	return result, timings, err
}

func TestR9RecoverCauseV150AsyncdocsSnapClean(t *testing.T) {
	storeRoot := os.Getenv("R9_ROOT")
	if storeRoot == "" {
		storeRoot = "/work/tnrun/v152-upg"
	}
	store := os.Getenv("R9_DIAG_STORE")
	if store == "" {
		store = "v150/uor/stores/asyncdocs-snap-clean"
	}
	r9CauseSource = filepath.Join(storeRoot, store, "data")
	if st, err := os.Stat(filepath.Join(r9CauseSource, "MANIFEST")); err != nil || st.IsDir() {
		t.Fatalf("selected store is missing or incomplete (no data/MANIFEST): %s (%v)", r9CauseSource, err)
	}
	r9CauseLatestSnapshot(t, r9CauseSource)
	n := 10
	if v, err := strconv.Atoi(os.Getenv("R9_CAUSE_N")); err == nil && v > 0 {
		n = v
	}
	var root string
	if area := os.Getenv("R9_CAUSE_AREA"); area != "" {
		root = filepath.Join(storeRoot, area)
		_ = os.RemoveAll(root)
	} else {
		var err error
		if root, err = os.MkdirTemp(storeRoot, "r9cause-"); err != nil {
			t.Fatalf("scratch area below %s: %v", storeRoot, err)
		}
	}
	defer os.RemoveAll(root)
	fails := map[string]int{}
	ctx := context.Background()
	for i := 0; i < n; i++ {
		for _, variant := range []string{"V1-wb-on", "V2-wb-off", "V3-wb-on-timed", "V4-wb-on-quiesced"} {
			work := filepath.Join(root, strconv.Itoa(i), variant)
			if err := os.MkdirAll(work, 0o755); err != nil {
				t.Fatal(err)
			}
			data := filepath.Join(work, "data")
			if err := r9CauseCopyDir(r9CauseSource, data); err != nil {
				t.Fatalf("copy: %v", err)
			}
			snap := r9CauseLatestSnapshot(t, data)
			fresh, err := NewBadgerEngineWithOptions(r9CauseOptions(filepath.Join(work, "fresh"), variant != "V2-wb-off"))
			if err != nil {
				t.Fatal(err)
			}
			log := &r9ApplyLog{}
			var res ReplayResult
			var timings []r9EntryTiming
			switch variant {
			case "V1-wb-on", "V2-wb-off":
				res, _, err = RecoverIntoEngine(ctx, fresh, filepath.Join(data, "wal"), snap)
			case "V3-wb-on-timed":
				log.wrap(fresh)
				res, timings, err = r9RecoverCopy(ctx, fresh, filepath.Join(data, "wal"), snap, log, false)
			case "V4-wb-on-quiesced":
				log.wrap(fresh)
				res, timings, err = r9RecoverCopy(ctx, fresh, filepath.Join(data, "wal"), snap, log, true)
			}
			fmt.Printf("R9CAUSE iter=%d %s applied=%d skipped=%d failed=%d err=%v\n", i, variant, res.Applied, res.Skipped, res.Failed, err)
			if res.Failed > 0 {
				fails[variant]++
			}
			for _, e := range res.Errors {
				fmt.Printf("R9CAUSE iter=%d %s error seq=%d op=%v err=%v\n", i, variant, e.Sequence, e.Operation, e.Error)
			}
			for _, et := range timings {
				if et.err == nil && et.seq != 164 {
					continue
				}
				ov := log.overlapping(et.t0, et.t1)
				desc := ""
				for _, s := range ov {
					desc += fmt.Sprintf(" [apply ops=%d start=%+dus end=%+dus err=%v]", s.ops, s.start.Sub(et.t0).Microseconds(), s.end.Sub(et.t0).Microseconds(), s.err)
				}
				fmt.Printf("R9CAUSE iter=%d %s entry seq=%d op=%v replay_us=%d pending_at_start=%d overlapping_applies=%d%s err=%v\n",
					i, variant, et.seq, et.op, et.t1.Sub(et.t0).Microseconds(), et.pending, len(ov), desc, et.err)
			}
			if variant == "V3-wb-on-timed" && i == 0 {
				for _, et := range timings {
					fmt.Printf("R9CAUSE iter=0 V3 entries seq=%d op=%v pending=%d\n", et.seq, et.op, et.pending)
				}
			}
			_ = fresh.Close()
			_ = os.RemoveAll(work)
		}
	}
	fmt.Printf("R9CAUSE SUMMARY iterations=%d V1-wb-on=%d V2-wb-off=%d V3-wb-on-timed=%d V4-wb-on-quiesced=%d\n",
		n, fails["V1-wb-on"], fails["V2-wb-off"], fails["V3-wb-on-timed"], fails["V4-wb-on-quiesced"])
	if fails["V1-wb-on"]+fails["V3-wb-on-timed"] > 0 {
		t.Errorf("recovery with write-behind refused journal entries (V1 %d, V3 %d of %d each)", fails["V1-wb-on"], fails["V3-wb-on-timed"], n)
	}
}

// r9CauseCopyDir copies the directory src to dst (dst must not exist). Method: UPR_COPY=gnu (cp -a --sparse=always),
// gcp (gcp -a --sparse=always, GNU cp as installed on macOS/BSD), or plain (cp -R -p, no sparse files: the copy
// takes the store's apparent size on disk). Unset, the first method present that succeeds is used.
func r9CauseCopyDir(src, dst string) error {
	methods := map[string][]string{
		"gnu":   {"cp", "-a", "--sparse=always", src, dst},
		"gcp":   {"gcp", "-a", "--sparse=always", src, dst},
		"plain": {"cp", "-R", "-p", src, dst},
	}
	order := []string{"gnu", "gcp", "plain"}
	if m := os.Getenv("UPR_COPY"); m != "" {
		if _, ok := methods[m]; !ok {
			return fmt.Errorf("UPR_COPY=%q: want gnu, gcp or plain", m)
		}
		order = []string{m}
	}
	var last error
	for _, m := range order {
		args := methods[m]
		if _, err := exec.LookPath(args[0]); err != nil {
			last = fmt.Errorf("copy method %s: %v", m, err)
			continue
		}
		_ = os.RemoveAll(dst)
		out, err := exec.Command(args[0], args[1:]...).CombinedOutput()
		if err == nil {
			return nil
		}
		last = fmt.Errorf("copy method %s (%s): %v: %s", m, strings.Join(args[:len(args)-2], " "), err, out)
	}
	return last
}
