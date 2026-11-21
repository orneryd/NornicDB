package storage

// Batch-3 probes L538/L539 (research only): the #1768 reviewers' reproductions
// (review/pr1768-f42cfdee round2/round4, review/pr1768-26dd444b r5,
// review/pr1768-1d7e3eb8 round1), ported unchanged in substance. Each test
// logs an EI3 line and reports instead of failing hard where the original
// asserted the defect, so one job measures every item.

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

type ei3Registry struct {
	mu      sync.Mutex
	readers map[int]SnapshotReaderInfo
	seq     int
}

func (r *ei3Registry) Register(info SnapshotReaderInfo) (string, func()) {
	r.mu.Lock()
	if r.readers == nil {
		r.readers = make(map[int]SnapshotReaderInfo)
	}
	r.seq++
	id := r.seq
	r.readers[id] = info
	r.mu.Unlock()
	return fmt.Sprint(id), func() { r.mu.Lock(); delete(r.readers, id); r.mu.Unlock() }
}
func (r *ei3Registry) ActiveCount() int64 { r.mu.Lock(); defer r.mu.Unlock(); return int64(len(r.readers)) }
func (r *ei3Registry) Snapshot() []SnapshotReaderInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []SnapshotReaderInfo
	for _, info := range r.readers {
		out = append(out, info)
	}
	return out
}

type ei3Controller struct {
	*testLifecycleController
	r *ei3Registry
}

func (c *ei3Controller) ReaderRegistry() SnapshotReaderRegistry { return c.r }
func (c *ei3Controller) RegisterSnapshotReader(info SnapshotReaderInfo) func() {
	_, done := c.r.Register(info)
	return done
}
func (c *ei3Controller) AcquireSnapshotReader(info SnapshotReaderInfo) (func(), error) {
	return c.RegisterSnapshotReader(info), nil
}

// L538 (a): a transaction that pins and registers while a prune batch, which
// already read the registry, waits on admission. Original: r2 MidBatch / r5.
func TestEI3L538MidBatchRegistration(t *testing.T) {
	for _, mode := range []string{"one-update-then-4", "six-updates"} {
		t.Run(mode, func(t *testing.T) {
			b := createMVCCBadgerEngine(t)
			c := &ei3Controller{testLifecycleController: &testLifecycleController{}, r: &ei3Registry{}}
			b.SetLifecycleController(c)
			id := NodeID("test:ei3-midbatch")
			_, err := b.CreateNode(t.Context(), &Node{ID: id, Properties: map[string]any{"v": 0}})
			require.NoError(t, err)
			head, err := b.GetNodeCurrentHead(t.Context(), id)
			require.NoError(t, err)
			tx, err := b.BeginTransaction(t.Context(), BeginRequest{})
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()
			for v := 1; v <= 6; v++ {
				require.NoError(t, b.UpdateNode(t.Context(), NodeUpdateRequest{After: &Node{ID: id, Properties: map[string]any{"v": v}}}))
			}
			release, done := parkPruneInsideDelete(t, b, MVCCPruneOptions{MaxVersionsPerKey: 1})
			require.NoError(t, tx.SetNamespace("test"))
			registered := c.r.ActiveCount()
			release()
			require.NoError(t, <-done)
			own, ownErr := tx.GetNode(t.Context(), id)
			got, asOfErr := b.GetNodeVisibleAt(t.Context(), id, head.Version)
			var ownV, asOfV any
			if own != nil {
				ownV = own.Properties["v"]
			}
			if got != nil {
				asOfV = got.Properties["v"]
			}
			t.Logf("EI3 L538a registered=%d tx own read v=%v err=%v; fresh as-of read at the tx cut: v=%v err=%v", registered, ownV, ownErr, asOfV, asOfErr)
			if asOfErr != nil {
				t.Errorf("EI3 L538a EXISTS: registered transaction's cut not available to a fresh as-of read: %v", asOfErr)
			}
		})
	}
}

// L538 (b): final tombstone marker beyond the per-step record limit.
func TestEI3L538TombstoneStepBound(t *testing.T) {
	for _, k := range []struct {
		name string
		kind byte
	}{{"node", prefixMVCCNode}, {"relationship", prefixMVCCEdge}, {"outgoing", prefixMVCCOutgoingAdj}, {"incoming", prefixMVCCIncomingAdj}} {
		t.Run(k.name, func(t *testing.T) {
			c := seedPrunedChain(t, k.kind, mvccPruneChunkRecords-1, time.Now().Add(-time.Hour), time.Nanosecond, true)
			before, _, _ := c.records(t)
			p := mvccPrunePass{b: c.engine, ctx: context.Background(), opts: MVCCPruneOptions{}}
			done, err := p.step([][]byte{c.logical})
			require.NoError(t, err)
			after, _, _ := c.records(t)
			t.Logf("EI3 L538b kind=%s before=%d after=%d deleted=%d limit=%d done=%d", k.name, len(before), len(after), p.deleted, c.engine.mvccPruneChunk(), done)
			if p.deleted > int64(c.engine.mvccPruneChunk()) {
				t.Errorf("EI3 L538b EXISTS: one step deleted %d records, limit %d", p.deleted, c.engine.mvccPruneChunk())
			}
		})
	}
}

// L538 (c): composite prune forwards the unchanged runtime budget.
func TestEI3L538CompositeRuntimeBudget(t *testing.T) {
	for _, n := range []int{2, 4} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			engines := map[string]Engine{}
			dbs := map[string]string{}
			modes := map[string]string{}
			for i := 0; i < n; i++ {
				e := createMVCCBadgerEngine(t)
				name := fmt.Sprintf("c%d", i)
				seedPruneHistory(t, e, fmt.Sprintf("db%d:budget", i), 1, 4)
				e.db.commitGate.Lock()
				defer e.db.commitGate.Unlock()
				engines[name], dbs[name], modes[name] = e, fmt.Sprintf("db%d", i), "read_write"
			}
			c := NewCompositeEngine(engines, dbs, modes)
			const budget = 100 * time.Millisecond
			started := time.Now()
			pruned, err := c.PruneMVCCVersions(t.Context(), MVCCPruneOptions{MaxVersionsPerKey: 1, MaxRuntime: budget})
			elapsed := time.Since(started)
			t.Logf("EI3 L538c constituents=%d budget=%s elapsed=%s pruned=%d err=%v", n, budget, elapsed, pruned, err)
			if elapsed > budget+50*time.Millisecond {
				t.Errorf("EI3 L538c EXISTS: one composite call took %s for a %s budget", elapsed, budget)
			}
		})
	}
}

// L538 (d): reader floors wait on the engine mutex without the work context.
func TestEI3L538ReaderFloorsWithoutContext(t *testing.T) {
	b := createMVCCBadgerEngine(t)
	seedPruneHistory(t, b, "test:ei3-budget-lock", 2, 3)
	b.mu.Lock()
	done := make(chan error, 1)
	started := time.Now()
	go func() {
		_, err := b.PruneMVCCVersions(context.Background(), MVCCPruneOptions{MaxVersionsPerKey: 1, MaxRuntime: 20 * time.Millisecond})
		done <- err
	}()
	var blocked bool
	select {
	case err := <-done:
		b.mu.Unlock()
		require.NoError(t, err)
	case <-time.After(250 * time.Millisecond):
		blocked = true
		b.mu.Unlock()
		require.NoError(t, <-done)
	}
	t.Logf("EI3 L538d 20ms-budget prune with engine mutex held 250ms: blocked=%v total=%s", blocked, time.Since(started))
	if blocked {
		t.Errorf("EI3 L538d EXISTS: a 20ms prune stayed blocked 250ms on the engine mutex before admission")
	}
}

// L539 (a): an as-of read waits on a stalled batch (round 1 copy).
func TestEI3L539StalledBatchAsOfWait(t *testing.T) {
	b := createMVCCBadgerEngine(t)
	seedPruneHistory(t, b, "test:ei3-budget", 2, 4)
	release, done := parkPruneInsideDelete(t, b, MVCCPruneOptions{MaxVersionsPerKey: 1, MaxRuntime: 20 * time.Millisecond})
	started := time.Now()
	select {
	case err := <-done:
		release()
		t.Logf("EI3 L539a 20ms-budget prune returned on its own after %s err=%v", time.Since(started), err)
	case <-time.After(250 * time.Millisecond):
		readCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		readStart := time.Now()
		_, readErr := b.GetNodeVisibleAt(readCtx, "test:ei3-budget-0", b.currentMVCCReadVersion("test"))
		readTook := time.Since(readStart)
		release()
		err := <-done
		t.Logf("EI3 L539a 20ms-budget prune still held its batch after 250ms; as-of read took %s err=%v; prune err=%v", readTook, readErr, err)
		if readErr != nil {
			t.Errorf("EI3 L539a EXISTS: as-of read blocked behind the stalled batch: %v", readErr)
		}
	}
}

// L539 (b): budget starvation on one long chain (r2).
func TestEI3L539LongChainBudget(t *testing.T) {
	b := createMVCCBadgerEngine(t)
	id := NodeID("test:ei3-longchain")
	_, err := b.CreateNode(t.Context(), &Node{ID: id})
	require.NoError(t, err)
	num, ok := b.idDict.lookupNodeNumID(id)
	require.True(t, ok)
	body, err := encodeMVCCNodeRecord(&Node{ID: id}, false)
	require.NoError(t, err)
	const versions = 20000
	start := time.Now().Add(-time.Hour)
	for first := 0; first < versions; first += 1000 {
		require.NoError(t, b.withUpdate(t.Context(), updateRequest{}, func(txn *badger.Txn) error {
			for i := first; i < first+1000; i++ {
				v := MVCCVersion{CommitTimestamp: start.Add(time.Duration(i) * time.Nanosecond), CommitSequence: uint64(i + 1)}
				if err := txn.Set(mvccNodeVersionKey(num, v), body); err != nil {
					return err
				}
			}
			return nil
		}))
	}
	var deleted int64
	for cycle := 0; cycle < 5; cycle++ {
		began := time.Now()
		n, err := b.PruneMVCCVersions(context.Background(), MVCCPruneOptions{MaxVersionsPerKey: 1, MaxRuntime: 5 * time.Millisecond})
		require.NoError(t, err)
		deleted += n
		t.Logf("EI3 L539b cycle %d: deleted=%d duration=%v", cycle, n, time.Since(began))
	}
	n, err := b.PruneMVCCVersions(t.Context(), MVCCPruneOptions{MaxVersionsPerKey: 1})
	t.Logf("EI3 L539b budgeted cycles deleted=%d; unbudgeted control deleted=%d err=%v", deleted, n, err)
	if deleted == 0 {
		t.Errorf("EI3 L539b EXISTS: five 5ms cycles made no progress on a %d-version chain", versions)
	}
}

// L539 (c): "Txn is too big" on one key with ~100,000 versions.
func TestEI3L539HundredThousandVersions(t *testing.T) {
	b := createMVCCBadgerEngine(t)
	id := NodeID("test:ei3-100k")
	_, err := b.CreateNode(t.Context(), &Node{ID: id})
	require.NoError(t, err)
	num, ok := b.idDict.lookupNodeNumID(id)
	require.True(t, ok)
	body, err := encodeMVCCNodeRecord(&Node{ID: id, Properties: map[string]any{"p": "x"}}, false)
	require.NoError(t, err)
	const versions = 100000
	start := time.Now().Add(-time.Hour)
	for first := 0; first < versions; first += 1000 {
		require.NoError(t, b.withUpdate(t.Context(), updateRequest{}, func(txn *badger.Txn) error {
			for i := first; i < first+1000; i++ {
				v := MVCCVersion{CommitTimestamp: start.Add(time.Duration(i) * time.Nanosecond), CommitSequence: uint64(i + 1)}
				if err := txn.Set(mvccNodeVersionKey(num, v), body); err != nil {
					return err
				}
			}
			return nil
		}))
	}
	began := time.Now()
	n, err := b.PruneMVCCVersions(t.Context(), MVCCPruneOptions{MaxVersionsPerKey: 1})
	t.Logf("EI3 L539c unbudgeted prune of a %d-version key: deleted=%d err=%v took=%s", versions, n, err, time.Since(began))
	if err != nil {
		t.Errorf("EI3 L539c EXISTS: %v", err)
	}
	_, err = b.GetNode(t.Context(), id)
	require.NoError(t, err)
}

// L539 (d): sparse candidate whose kernel exceeds the reserved commit share (r5).
func TestEI3L539SparseCandidate(t *testing.T) {
	b := createMVCCBadgerEngine(t)
	const records, tail = 256, 50000
	base := time.Now().Add(-time.Hour)
	_, err := b.CreateNode(t.Context(), &Node{ID: "test:ei3-heavy"})
	require.NoError(t, err)
	heavyNum, ok := b.idDict.lookupNodeNumID("test:ei3-heavy")
	require.True(t, ok)
	body, err := encodeMVCCNodeRecord(&Node{ID: "test:ei3-heavy", Properties: map[string]any{"payload": string(bytes.Repeat([]byte{'x'}, 60000))}}, false)
	require.NoError(t, err)
	light, err := encodeMVCCNodeRecord(&Node{ID: "test:ei3-tail"}, false)
	require.NoError(t, err)
	logical := append([]byte{prefixMVCCNode}, encodeNumID(heavyNum)...)
	for first := 0; first < records; first += 16 {
		require.NoError(t, b.withUpdate(t.Context(), updateRequest{}, func(txn *badger.Txn) error {
			for v := first; v < min(first+16, records); v++ {
				if err := txn.Set(mvccNodeVersionKey(heavyNum, MVCCVersion{CommitTimestamp: base.Add(time.Duration(v) * time.Second), CommitSequence: uint64(v + 1)}), body); err != nil {
					return err
				}
			}
			return nil
		}))
	}
	for first := 1; first < tail+1; first += 500 {
		require.NoError(t, b.withUpdate(t.Context(), updateRequest{}, func(txn *badger.Txn) error {
			for id := first; id < min(first+500, tail+1); id++ {
				if err := txn.Set(mvccNodeVersionKey(heavyNum+uint64(id), MVCCVersion{CommitTimestamp: base, CommitSequence: 1}), light); err != nil {
					return err
				}
			}
			return nil
		}))
	}
	opts := MVCCPruneOptions{MaxVersionsPerKey: 1, MinRetentionAge: time.Minute}
	var cost time.Duration
	rollback := fmt.Errorf("ei3 calibration rollback")
	for i := 0; i < 4; i++ {
		started := time.Now()
		err := b.withUpdate(t.Context(), updateRequest{}, func(txn *badger.Txn) error {
			_, _, err := b.pruneMVCCKeyspaceInTxn(t.Context(), txn, logical, opts, nil, b.mvccPruneChunk())
			if err != nil {
				return err
			}
			return rollback
		})
		require.ErrorIs(t, err, rollback)
		elapsed := time.Since(started)
		if i > 0 && (cost == 0 || elapsed < cost) {
			cost = elapsed
		}
	}
	opts.MaxRuntime = 2 * cost
	var total int64
	for cycle := 0; cycle < 8; cycle++ {
		started := time.Now()
		n, err := b.PruneMVCCVersions(t.Context(), opts)
		require.NoError(t, err)
		total += n
		t.Logf("EI3 L539d cycle %d: budget=%s kernel=%s deleted=%d elapsed=%s", cycle, opts.MaxRuntime, cost, n, time.Since(started))
	}
	opts.MaxRuntime = 0
	n, err := b.PruneMVCCVersions(t.Context(), opts)
	t.Logf("EI3 L539d budgeted total=%d; unbudgeted control deleted=%d err=%v (records-1=%d)", total, n, err, records-1)
	if total == 0 {
		t.Errorf("EI3 L539d EXISTS: no budgeted cycle committed the sparse candidate")
	}
}
