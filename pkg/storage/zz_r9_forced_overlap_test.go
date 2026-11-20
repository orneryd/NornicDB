package storage

// R9-1 forced overlap (campaign scratch test, no product change).
// Deterministic trigger for the replay-path failure: the background write-behind generation apply is forced to
// overlap the direct replay of a buffered_commit journal entry. Test-side only: a wrapper around
// WriteBehindBuffer.apply holds every generation apply that starts during journal replay until the replay of the
// first buffered_commit entry visited while an apply is held (the target), then sleeps offset_ms and calls the
// original apply. Replay goes through r9RecoverCopy (a copy of RecoverIntoEngine, wal_recovery_stream.go:30-128,
// with an entry hook); this covers the replay path only, not the real nornicdb.Open start-up.
// Env: R9_ROOT, R9_DIAG_STORE (as the cause test); R9_FORCED_WB=on|off (default on; off is the control, the same
// replay into an engine with write-behind off, where no generation apply exists); R9_FORCED_OFFSETS_MS
// (comma-separated, default 0,1,2,3,4,5,6,8,10,12,15,20,25,30,40,50); R9_FORCED_HOLD_MAX_MS (default 10000,
// releases a held apply if no target is reached, counted as gate_timeout).
// Prints one R9FORCED run line per offset and an R9FORCED SUMMARY line; fails when any run reports Failed > 0.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type r9Gate struct {
	armed        atomic.Bool
	released     chan struct{}
	once         sync.Once
	delay        time.Duration
	holdMax      time.Duration
	held         atomic.Int64 // applies held by the gate (total)
	waiting      atomic.Int64 // applies currently held
	timeouts     atomic.Int64
	target       atomic.Uint64
	heldAtTarget int64
}

func (g *r9Gate) release() { g.once.Do(func() { close(g.released) }) }

func (g *r9Gate) wrap(b *BadgerEngine) {
	if b.writeBehind == nil {
		return
	}
	orig := b.writeBehind.apply
	b.writeBehind.apply = func(buf *CommitBuffer) error {
		if g.armed.Load() {
			g.held.Add(1)
			g.waiting.Add(1)
			select {
			case <-g.released:
				time.Sleep(g.delay)
			case <-time.After(g.holdMax):
				g.timeouts.Add(1)
			}
			g.waiting.Add(-1)
		}
		return orig(buf)
	}
}

func TestR9ForcedOverlapV150AsyncdocsSnapClean(t *testing.T) {
	storeRoot := os.Getenv("R9_ROOT")
	if storeRoot == "" {
		storeRoot = "/work/tnrun/v152-upg"
	}
	store := os.Getenv("R9_DIAG_STORE")
	if store == "" {
		store = "v150/uor/stores/asyncdocs-snap-clean"
	}
	source := filepath.Join(storeRoot, store, "data")
	if st, err := os.Stat(filepath.Join(source, "MANIFEST")); err != nil || st.IsDir() {
		t.Fatalf("selected store is missing or incomplete (no data/MANIFEST): %s (%v)", source, err)
	}
	r9CauseLatestSnapshot(t, source)
	wb := os.Getenv("R9_FORCED_WB")
	if wb == "" {
		wb = "on"
	}
	if wb != "on" && wb != "off" {
		t.Fatalf("R9_FORCED_WB=%q: want on or off", wb)
	}
	offs := os.Getenv("R9_FORCED_OFFSETS_MS")
	if offs == "" {
		offs = "0,1,2,3,4,5,6,8,10,12,15,20,25,30,40,50"
	}
	var offsets []int
	for _, f := range strings.Split(offs, ",") {
		v, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil || v < 0 {
			t.Fatalf("R9_FORCED_OFFSETS_MS: bad offset %q", f)
		}
		offsets = append(offsets, v)
	}
	holdMax := 10 * time.Second
	if v, err := strconv.Atoi(os.Getenv("R9_FORCED_HOLD_MAX_MS")); err == nil && v > 0 {
		holdMax = time.Duration(v) * time.Millisecond
	}
	root, err := os.MkdirTemp(storeRoot, "r9forced-")
	if err != nil {
		t.Fatalf("scratch area below %s: %v", storeRoot, err)
	}
	defer os.RemoveAll(root)
	defer func() { r9EntryHook = nil }()
	ctx := context.Background()
	failedRuns, heldRuns, timeoutRuns := 0, 0, 0
	for i, off := range offsets {
		work := filepath.Join(root, strconv.Itoa(i))
		data := filepath.Join(work, "data")
		if err := os.MkdirAll(work, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := r9CauseCopyDir(source, data); err != nil {
			t.Fatalf("copy: %v", err)
		}
		snap := r9CauseLatestSnapshot(t, data)
		fresh, err := NewBadgerEngineWithOptions(r9CauseOptions(filepath.Join(work, "fresh"), wb == "on"))
		if err != nil {
			t.Fatal(err)
		}
		g := &r9Gate{released: make(chan struct{}), delay: time.Duration(off) * time.Millisecond, holdMax: holdMax}
		g.wrap(fresh)
		first := true
		r9EntryHook = func(e WALEntry) {
			if first {
				// first journal entry: the snapshot restore and its flush are done, arm the gate
				first = false
				g.armed.Store(true)
			}
			if g.target.Load() == 0 && e.Operation == OpBufferedCommit && g.waiting.Load() > 0 {
				g.target.Store(e.Sequence)
				g.heldAtTarget = g.waiting.Load()
				g.armed.Store(false)
				g.release()
			}
		}
		log := &r9ApplyLog{}
		res, timings, rerr := r9RecoverCopy(ctx, fresh, filepath.Join(data, "wal"), snap, log, false)
		g.armed.Store(false)
		g.release()
		r9EntryHook = nil
		_ = fresh.Close()
		target := "none"
		targetErr := "<none>"
		if s := g.target.Load(); s != 0 {
			target = strconv.FormatUint(s, 10)
			for _, et := range timings {
				if et.seq == s {
					targetErr = fmt.Sprint(et.err)
				}
			}
		}
		fmt.Printf("R9FORCED run=%d wb=%s offset_ms=%d applied=%d skipped=%d failed=%d target_seq=%s held_at_target=%d applies_held=%d gate_timeouts=%d target_err=%s err=%v\n",
			i, wb, off, res.Applied, res.Skipped, res.Failed, target, g.heldAtTarget, g.held.Load(), g.timeouts.Load(), targetErr, rerr)
		for _, e := range res.Errors {
			fmt.Printf("R9FORCED run=%d error seq=%d op=%v err=%v\n", i, e.Sequence, e.Operation, e.Error)
		}
		if res.Failed > 0 {
			failedRuns++
		}
		if g.target.Load() != 0 {
			heldRuns++
		}
		if g.timeouts.Load() > 0 {
			timeoutRuns++
		}
		_ = os.RemoveAll(work)
	}
	fmt.Printf("R9FORCED SUMMARY wb=%s runs=%d failed_runs=%d runs_with_target=%d runs_with_gate_timeout=%d\n",
		wb, len(offsets), failedRuns, heldRuns, timeoutRuns)
	if failedRuns > 0 {
		t.Errorf("forced overlap: %d of %d replays refused journal entries (write-behind %s)", failedRuns, len(offsets), wb)
	}
}
