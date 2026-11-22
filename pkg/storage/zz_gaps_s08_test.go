package storage

// Report gap S08 (v1.5.2 verification campaign, test-side only, pin
// e8cd436e5): which key the failing buffered_commit replay and the
// overlapping background write-behind apply both touch.
//
// Store: the v1.5.0 asynchronous store "asyncdocs-snap-clean" (upgrade lane
// copy under /work/tnrun/v152-upg), replayed from snapshot + journal into a
// fresh engine with write-behind on (the options nornicdb.Open gives an
// asynchronous store), as the earlier cause run's V3 variant: a copy of
// RecoverIntoEngine (wal_recovery_stream.go:30-128) that times each journal
// entry, with the background generation apply wrapped (WriteBehindBuffer.apply)
// to record its span and the Badger versions it committed (MaxVersion before
// and after).
//
// New here: while each buffered_commit entry replays, a sampler collects the
// Badger transactions the replay registers (idDictionary.txnCounters /
// txnCommitting and mvccReservations.byTxn, read under their locks). After a
// failed replay it reads each sampled transaction's read timestamp, read-key
// fingerprints (badger.Txn.reads) and pending writes (badger.Txn.pendingWrites),
// and lists every Badger key with a committed version above that read
// timestamp that the transaction read (Badger's conflict condition) or also
// wrote, with the version and the apply span that committed it.
// Env: GAPS08_AREA (scratch dir under /work/tnrun/v152-upg), GAPS08_N
// (iterations, default 12). Diagnostic: fails only on harness errors.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/ristretto/v2/z"
)

const gapS08Source = "/work/tnrun/v152-upg/v150/uor/stores/asyncdocs-snap-clean/data"

type gapS08Span struct {
	start, end      time.Time
	ops             int
	vBefore, vAfter uint64
	err             error
}

type gapS08ApplyLog struct {
	mu       sync.Mutex
	spans    []gapS08Span
	inflight atomic.Int64

	force         bool
	armed         atomic.Bool
	held          atomic.Bool
	replayStarted chan struct{}
	startOnce     sync.Once
	forcedOps     int
}

// gapS08Versions lists the keys with a committed version in (from, to].
func gapS08Versions(b *BadgerEngine, from, to uint64) map[string][]uint64 {
	out := map[string][]uint64{}
	scan := b.db.DB.NewTransactionAt(^uint64(0), false)
	defer scan.Discard()
	opts := badger.DefaultIteratorOptions
	opts.AllVersions = true
	opts.PrefetchValues = false
	it := scan.NewIterator(opts)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		v := it.Item().Version()
		if v > from && v <= to {
			k := string(it.Item().KeyCopy(nil))
			out[k] = append(out[k], v)
		}
	}
	return out
}

func gapS08Classes(keys map[string][]uint64) string {
	c := map[string]int{}
	for k := range keys {
		c[fmt.Sprintf("0x%02x", k[0])]++
	}
	var parts []string
	for k, n := range c {
		parts = append(parts, fmt.Sprintf("%s x%d", k, n))
	}
	sort.Strings(parts)
	return strings.Join(parts, " ")
}

func (l *gapS08ApplyLog) wrap(b *BadgerEngine) {
	if b.writeBehind == nil {
		return
	}
	orig := b.writeBehind.apply
	b.writeBehind.apply = func(buf *CommitBuffer) error {
		l.inflight.Add(1)
		// Forced overlap (GAPS08_FORCE): the first generation apply that starts
		// after the snapshot restore waits until a buffered_commit replay has
		// started, then 5 ms more, so it commits inside that replay.
		if l.force && l.armed.Load() && l.held.CompareAndSwap(false, true) {
			select {
			case <-l.replayStarted:
				time.Sleep(5 * time.Millisecond)
				l.forcedOps = buf.ops
			case <-time.After(5 * time.Second):
			}
		}
		s := gapS08Span{start: time.Now(), ops: buf.ops, vBefore: b.db.DB.MaxVersion()}
		err := orig(buf)
		s.end, s.err, s.vAfter = time.Now(), err, b.db.DB.MaxVersion()
		l.inflight.Add(-1)
		l.mu.Lock()
		l.spans = append(l.spans, s)
		l.mu.Unlock()
		return err
	}
}

func (l *gapS08ApplyLog) spanFor(version uint64) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, s := range l.spans {
		if version > s.vBefore && version <= s.vAfter {
			return fmt.Sprintf("apply#%d(ops=%d versions %d..%d)", i, s.ops, s.vBefore+1, s.vAfter)
		}
	}
	return "not-in-an-apply-span"
}

func (l *gapS08ApplyLog) overlapping(t0, t1 time.Time) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, s := range l.spans {
		if s.start.Before(t1) && s.end.After(t0) {
			n++
		}
	}
	return n
}

// gapS08Sampler collects the Badger transactions registered with the id
// dictionary and the MVCC reservations while it runs.
type gapS08Sampler struct {
	stop chan struct{}
	done chan struct{}
	mu   sync.Mutex
	seen map[*badger.Txn]string
}

func gapS08StartSampler(b *BadgerEngine, existing map[*badger.Txn]bool) *gapS08Sampler {
	s := &gapS08Sampler{stop: make(chan struct{}), done: make(chan struct{}), seen: map[*badger.Txn]string{}}
	go func() {
		defer close(s.done)
		for {
			select {
			case <-s.stop:
				return
			default:
			}
			var found []*badger.Txn
			var where []string
			b.idDict.txnMu.Lock()
			for txn := range b.idDict.txnCounters {
				found, where = append(found, txn), append(where, "idDict.txnCounters")
			}
			for txn := range b.idDict.txnCommitting {
				found, where = append(found, txn), append(where, "idDict.txnCommitting")
			}
			b.idDict.txnMu.Unlock()
			b.mvccReservations.mu.Lock()
			for txn := range b.mvccReservations.byTxn {
				found, where = append(found, txn), append(where, "mvccReservations.byTxn")
			}
			b.mvccReservations.mu.Unlock()
			s.mu.Lock()
			for i, txn := range found {
				if existing[txn] {
					continue
				}
				if prev, ok := s.seen[txn]; !ok {
					s.seen[txn] = where[i]
				} else if !strings.Contains(prev, where[i]) {
					s.seen[txn] = prev + "+" + where[i]
				}
			}
			s.mu.Unlock()
			time.Sleep(100 * time.Microsecond)
		}
	}()
	return s
}

func (s *gapS08Sampler) finish() map[*badger.Txn]string {
	close(s.stop)
	<-s.done
	return s.seen
}

func gapS08Existing(b *BadgerEngine) map[*badger.Txn]bool {
	out := map[*badger.Txn]bool{}
	b.idDict.txnMu.Lock()
	for txn := range b.idDict.txnCounters {
		out[txn] = true
	}
	for txn := range b.idDict.txnCommitting {
		out[txn] = true
	}
	b.idDict.txnMu.Unlock()
	b.mvccReservations.mu.Lock()
	for txn := range b.mvccReservations.byTxn {
		out[txn] = true
	}
	b.mvccReservations.mu.Unlock()
	return out
}

func gapS08Reads(txn *badger.Txn) (map[uint64]struct{}, bool) {
	field := reflect.ValueOf(txn).Elem().FieldByName("reads")
	if !field.IsValid() || field.Kind() != reflect.Slice {
		return nil, false
	}
	out := make(map[uint64]struct{}, field.Len())
	for i := 0; i < field.Len(); i++ {
		if field.Index(i).Kind() != reflect.Uint64 {
			return nil, false
		}
		out[field.Index(i).Uint()] = struct{}{}
	}
	return out, true
}

func gapS08Writes(txn *badger.Txn) (map[string]struct{}, bool) {
	field := reflect.ValueOf(txn).Elem().FieldByName("pendingWrites")
	if !field.IsValid() || field.Kind() != reflect.Map {
		return nil, false
	}
	out := make(map[string]struct{}, field.Len())
	for _, k := range field.MapKeys() {
		if k.Kind() != reflect.String {
			return nil, false
		}
		out[k.String()] = struct{}{}
	}
	return out, true
}

func gapS08Key(key []byte) string {
	var b strings.Builder
	fmt.Fprintf(&b, "0x%02x:", key[0])
	for i, ch := range key[1:] {
		if i >= 48 {
			b.WriteString("...")
			break
		}
		if ch >= 0x20 && ch < 0x7f {
			b.WriteByte(ch)
		} else {
			fmt.Fprintf(&b, "\\x%02x", ch)
		}
	}
	return b.String()
}

// gapS08Explain lists, for one sampled transaction, the keys with a committed
// version above its read timestamp that it read and/or wrote.
func gapS08Explain(b *BadgerEngine, txn *badger.Txn, log *gapS08ApplyLog) string {
	readTs := txn.ReadTs()
	reads, readsOK := gapS08Reads(txn)
	writes, writesOK := gapS08Writes(txn)
	raw := b.db.DB
	scan := raw.NewTransactionAt(^uint64(0), false)
	defer scan.Discard()
	opts := badger.DefaultIteratorOptions
	opts.AllVersions = true
	opts.PrefetchValues = false
	it := scan.NewIterator(opts)
	defer it.Close()
	var hits []string
	newer := 0
	classes := map[string]int{}
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		v := item.Version()
		if v <= readTs {
			continue
		}
		newer++
		key := item.KeyCopy(nil)
		classes[fmt.Sprintf("0x%02x", key[0])]++
		_, read := reads[z.MemHash(key)]
		_, wrote := writes[string(key)]
		if !read && !wrote {
			continue
		}
		hits = append(hits, fmt.Sprintf("{key=%q version=%d read_by_replay=%t written_by_replay=%t committed_by=%s deleted=%t}", gapS08Key(key), v, read, wrote, log.spanFor(v), item.IsDeletedOrExpired()))
	}
	var cl []string
	for k, n := range classes {
		cl = append(cl, fmt.Sprintf("%s x%d", k, n))
	}
	sort.Strings(cl)
	if len(hits) > 40 {
		hits = append(hits[:40], fmt.Sprintf("... %d more", len(hits)-40))
	}
	return fmt.Sprintf("readTs=%d reads=%d(ok=%t) pendingWrites=%d(ok=%t) versions_above_readTs=%d by_prefix=[%s] shared_keys=%d [%s]",
		readTs, len(reads), readsOK, len(writes), writesOK, newer, strings.Join(cl, " "), len(hits), strings.Join(hits, " "))
}

func gapS08Options(dir string) BadgerOptions {
	return BadgerOptions{
		DataDir:               dir,
		HighPerformance:       true,
		NodeCacheMaxEntries:   10000,
		EdgeTypeCacheMaxTypes: 50,
		AllowStorageUpgrade:   true,
		WriteBehind:           true,
	}
}

// gapS08Recover is RecoverIntoEngine (wal_recovery_stream.go:30-128 at
// e8cd436e5) with timing, the sampler around buffered_commit entries and the
// explanation of failed entries.
func gapS08Recover(ctx context.Context, t *testing.T, iter int, destination Engine, walDir, snapshotPath string, log *gapS08ApplyLog) (ReplayResult, error) {
	ctx = context.WithoutCancel(ctx)
	result := ReplayResult{Errors: make([]ReplayError, 0)}
	if err := ValidateWALFormat(walDir, WALValidateCurrent); err != nil {
		return result, err
	}
	var snapshotSeq uint64
	if err := markRecoveryPropertyForeign(destination); err != nil {
		return result, err
	}
	if snapshotPath != "" {
		streaming, err := IsStreamingSnapshot(snapshotPath)
		if err != nil && !os.IsNotExist(err) {
			return result, err
		}
		if streaming {
			file, err := os.Open(snapshotPath)
			if err != nil {
				return result, err
			}
			if err := beginRecoveryPropertyRestore(destination); err != nil {
				file.Close()
				return result, err
			}
			visitor := newRecoverySnapshotVisitor(destination, recoveryBatchSize)
			metadata, readErr := readSnapshot(ctx, file, visitor, nil, materializedSnapshotRecordSize)
			if readErr == nil {
				readErr = visitor.Flush(ctx)
			}
			file.Close()
			if readErr != nil {
				return result, readErr
			}
			snapshotSeq = metadata.Sequence
		} else {
			snapshot, err := LoadSnapshot(ctx, snapshotPath)
			if err != nil {
				if !os.IsNotExist(err) {
					return result, err
				}
			} else {
				if err := beginRecoveryPropertyRestore(destination); err != nil {
					return result, err
				}
				snapshotSeq = snapshot.Sequence
				if err := restoreLegacySnapshot(ctx, destination, snapshot); err != nil {
					return result, err
				}
			}
		}
		if flusher, ok := UnwrapEngine(destination).(interface{ FlushWriteBehind(context.Context) error }); ok {
			if err := flusher.FlushWriteBehind(ctx); err != nil {
				return result, err
			}
		}
	}
	activePath := walActivePath(walDir)
	if _, _, err := repairWALTailIfNeeded(activePath, defaultWALLogger{}); err != nil {
		return result, err
	}
	b, _ := UnwrapEngine(destination).(*BadgerEngine)
	log.armed.Store(true)
	err := VisitWALEntriesAfterFromDir(ctx, walDir, snapshotSeq, func(entry WALEntry) error {
		if entry.Operation == OpCheckpoint {
			result.Skipped++
			return nil
		}
		var sampler *gapS08Sampler
		if entry.Operation == OpBufferedCommit && b != nil {
			sampler = gapS08StartSampler(b, gapS08Existing(b))
		}
		pending := 0
		if b != nil && b.writeBehind != nil {
			pending = b.writeBehind.PendingOps()
		}
		vStart := b.db.DB.MaxVersion()
		t0 := time.Now()
		if entry.Operation == OpBufferedCommit && log.replayStarted != nil {
			log.startOnce.Do(func() { close(log.replayStarted) })
		}
		rerr := replayWALEntry(ctx, destination, entry)
		t1 := time.Now()
		var sampled map[*badger.Txn]string
		if sampler != nil {
			sampled = sampler.finish()
		}
		if rerr == nil && entry.Operation == OpBufferedCommit {
			deadline := time.Now().Add(10 * time.Second)
			for log.inflight.Load() > 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			fmt.Printf("GAPS08 iter=%d OK entry seq=%d op=%v replay_us=%d pending_at_start=%d overlapping_applies=%d sampled_txns=%d\n",
				iter, entry.Sequence, entry.Operation, t1.Sub(t0).Microseconds(), pending, log.overlapping(t0, t1), len(sampled))
			n := 0
			for txn, where := range sampled {
				n++
				fmt.Printf("GAPS08 iter=%d seq=%d OK replay-txn#%d registered_in=%s %s\n", iter, entry.Sequence, n, where, gapS08Explain(b, txn, log))
			}
		}
		if rerr == nil {
			result.Applied++
		} else if errors.Is(rerr, ErrAlreadyExists) {
			result.Skipped++
		} else {
			result.Failed++
			result.Errors = append(result.Errors, ReplayError{Sequence: entry.Sequence, Operation: entry.Operation, Error: rerr})
			// Let an overlapping apply finish before reading versions.
			deadline := time.Now().Add(10 * time.Second)
			for log.inflight.Load() > 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			fmt.Printf("GAPS08 iter=%d FAILED entry seq=%d op=%v db=%q replay_us=%d pending_at_start=%d overlapping_applies=%d maxVersion_at_start=%d err=%v sampled_txns=%d\n",
				iter, entry.Sequence, entry.Operation, entry.Database, t1.Sub(t0).Microseconds(), pending, log.overlapping(t0, t1), vStart, rerr, len(sampled))
			n := 0
			for txn, where := range sampled {
				n++
				fmt.Printf("GAPS08 iter=%d seq=%d replay-txn#%d registered_in=%s %s\n", iter, entry.Sequence, n, where, gapS08Explain(b, txn, log))
			}
			log.mu.Lock()
			applyKeys := map[string][]uint64{}
			for i, s := range log.spans {
				if s.start.Before(t1) && s.end.After(t0) {
					fmt.Printf("GAPS08 iter=%d seq=%d overlapping apply#%d ops=%d start=%+dus end=%+dus versions=%d..%d err=%v\n", iter, entry.Sequence, i, s.ops, s.start.Sub(t0).Microseconds(), s.end.Sub(t0).Microseconds(), s.vBefore+1, s.vAfter, s.err)
					for k, vs := range gapS08Versions(b, s.vBefore, s.vAfter) {
						applyKeys[k] = append(applyKeys[k], vs...)
					}
				}
			}
			log.mu.Unlock()
			// Replay the same entry again on the now quiescent engine: the keys
			// it writes, against the keys the overlapping apply wrote.
			v0 := b.db.DB.MaxVersion()
			again := replayWALEntry(ctx, destination, entry)
			v1 := b.db.DB.MaxVersion()
			replayKeys := gapS08Versions(b, v0, v1)
			var both []string
			for k, vs := range replayKeys {
				if avs, ok := applyKeys[k]; ok {
					both = append(both, fmt.Sprintf("{key=%q apply_versions=%v replay_versions=%v}", gapS08Key([]byte(k)), avs, vs))
				}
			}
			sort.Strings(both)
			fmt.Printf("GAPS08 iter=%d seq=%d KEYS apply_wrote=%d [%s] replay_retry_err=%v replay_wrote=%d [%s] BOTH=%d %s\n",
				iter, entry.Sequence, len(applyKeys), gapS08Classes(applyKeys), again, len(replayKeys), gapS08Classes(replayKeys), len(both), strings.Join(both, " "))
		}
		return nil
	})
	if result.Applied > 0 && b != nil {
		err = b.completeRecoveredCounts(ctx, err)
	}
	return result, err
}

func TestGapS08ConflictKey(t *testing.T) {
	area := os.Getenv("GAPS08_AREA")
	if area == "" {
		t.Skip("GAPS08_AREA not set")
	}
	n := 12
	if v, err := strconv.Atoi(os.Getenv("GAPS08_N")); err == nil && v > 0 {
		n = v
	}
	root := filepath.Join("/work/tnrun/v152-upg", area)
	_ = os.RemoveAll(root)
	defer os.RemoveAll(root)
	if out, err := exec.Command("sh", "-c", "cd "+gapS08Source+" && find . -type f -printf '%TY-%Tm-%TdT%TH:%TM:%TS %s %p\\n' | sort -k3 && find . -type f -exec sha256sum {} + | sort -k2 | sha256sum").CombinedOutput(); err == nil {
		fmt.Printf("GAPS08 SOURCE listing of %s:\n%s", gapS08Source, out)
	} else {
		fmt.Printf("GAPS08 SOURCE listing failed: %v %s\n", err, out)
	}
	failed, plainFailed := 0, 0
	ctx := context.Background()
	for i := 0; i < n; i++ {
		work := filepath.Join(root, strconv.Itoa(i))
		if err := os.MkdirAll(work, 0o755); err != nil {
			t.Fatal(err)
		}
		data := filepath.Join(work, "data")
		if out, err := exec.Command("cp", "-a", "--sparse=always", gapS08Source, data).CombinedOutput(); err != nil {
			t.Fatalf("copy: %v %s", err, out)
		}
		snaps, _ := filepath.Glob(filepath.Join(data, "snapshots", "snapshot-*.json"))
		sort.Strings(snaps)
		if len(snaps) == 0 {
			t.Fatalf("no snapshot in %s", data)
		}
		plain, err := NewBadgerEngineWithOptions(gapS08Options(filepath.Join(work, "plain")))
		if err != nil {
			t.Fatal(err)
		}
		pres, _, perr := RecoverIntoEngine(ctx, plain, filepath.Join(data, "wal"), snaps[len(snaps)-1])
		fmt.Printf("GAPS08 iter=%d PLAIN RecoverIntoEngine applied=%d skipped=%d failed=%d err=%v\n", i, pres.Applied, pres.Skipped, pres.Failed, perr)
		for _, e := range pres.Errors {
			fmt.Printf("GAPS08 iter=%d PLAIN error seq=%d op=%v err=%v\n", i, e.Sequence, e.Operation, e.Error)
		}
		if pres.Failed > 0 {
			plainFailed++
		}
		_ = plain.Close()
		fresh, err := NewBadgerEngineWithOptions(gapS08Options(filepath.Join(work, "fresh")))
		if err != nil {
			t.Fatal(err)
		}
		log := &gapS08ApplyLog{force: os.Getenv("GAPS08_FORCE") == "1", replayStarted: make(chan struct{})}
		log.wrap(fresh)
		res, rerr := gapS08Recover(ctx, t, i, fresh, filepath.Join(data, "wal"), snaps[len(snaps)-1], log)
		fmt.Printf("GAPS08 iter=%d applied=%d skipped=%d failed=%d err=%v applies=%d\n", i, res.Applied, res.Skipped, res.Failed, rerr, len(log.spans))
		if res.Failed > 0 {
			failed++
		}
		_ = fresh.Close()
		_ = os.RemoveAll(work)
	}
	fmt.Printf("GAPS08 SUMMARY iterations=%d instrumented_failed=%d plain_failed=%d\n", n, failed, plainFailed)
}
