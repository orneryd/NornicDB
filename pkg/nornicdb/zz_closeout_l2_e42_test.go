//go:build closeout_l2_e42

package nornicdb_test

// L2 close-out, report entry 42: with MVCCRetentionMaxVersions = 100 a large
// auto-commit delete is far slower when it overlaps MVCC prune activity; with
// the default configuration (no history, no automatic prune) it is not. The
// entry's runs show the delete's commit in Badger's conflict check
// (oracle.hasConflict under BadgerTransaction.PublishCommit) while Badger's
// retained committed-transaction list grew by thousands, and the discard
// timestamp held at the oldest registered read.
//
// Light deterministic form (the entry used 60 HTTP deletes of 60,000 nodes and
// a 30 s lifecycle cycle). One database per scenario, opened like the entry's
// server database (DefaultConfig, retention 100 or 0), its automatic
// lifecycle paused so the test places the prune itself. Each round:
//   - history: 6,000 nodes created and updated once (with retention 100 each
//     keeps one archived version the prune may delete);
//   - 6,000 victim nodes; one implicit transaction deletes all of them (the
//     auto-commit DELETE of the entry, at the storage layer);
//   - the prune (PruneMVCCVersions with MaxVersionsPerKey 1 and the
//     lifecycle's 5 s budget), placed per scenario; then the commit is timed.
// Scenarios: A retention 100, prune between the delete's staging and its
// commit (overlap); B retention 100, the same prune before the transaction
// begins (same work, no overlap); C default configuration (retention 0), the
// same overlapping prune call, which finds no history.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	l2E42History = 6000
	l2E42Victims = 6000
	l2E42Rounds  = 3
	// Threshold: the overlapping commit must cost more than 3x the larger of
	// the two non-overlapping commits and at least 250 ms more. The conflict
	// check of the defect walks every commit retained since the delete's read
	// (about 6,000 prune commits) for each key the delete read (several per
	// victim): about 10^8 map lookups, seconds, against a commit of a few
	// hundred milliseconds without them. Fixed (the list not retained or not
	// walked), the three commits are within noise of each other.
	l2E42MaxRatio = 3.0
	l2E42MinExtra = 250 * time.Millisecond
)

type l2E42Scenario struct {
	name      string
	retention int
	overlap   bool // prune between staging and commit (false: before begin)
}

type l2E42Round struct {
	commit   time.Duration
	staging  time.Duration
	prune    time.Duration
	pruned   int64
	survived int
}

func l2E42Open(t *testing.T, retention int) (*nornicdb.DB, *storage.BadgerEngine) {
	t.Helper()
	config := nornicdb.DefaultConfig()
	config.Database.MVCCRetentionMaxVersions = retention
	config.Memory.EmbeddingEnabled = false
	config.Memory.AutoLinksEnabled = false
	db, err := nornicdb.Open(t.TempDir(), config)
	if err != nil {
		t.Fatalf("open (retention %d): %v", retention, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	engine, ok := storage.UnwrapEngine(db.GetBaseStorageForManager()).(*storage.BadgerEngine)
	if !ok {
		t.Fatalf("base storage is %T, want *storage.BadgerEngine", storage.UnwrapEngine(db.GetBaseStorageForManager()))
	}
	// The test places every prune itself; the automatic cycle stays out.
	engine.PauseLifecycle()
	return db, engine
}

func l2E42Bulk(t *testing.T, engine *storage.BadgerEngine, prefix, label string, n int) []storage.NodeID {
	t.Helper()
	ids := make([]storage.NodeID, 0, n)
	for first := 0; first < n; first += 2000 {
		batch := make([]*storage.Node, 0, 2000)
		for i := first; i < min(first+2000, n); i++ {
			id := storage.NodeID(fmt.Sprintf("nornic:%s%d", prefix, i))
			ids = append(ids, id)
			batch = append(batch, &storage.Node{ID: id, Labels: []string{label}, Properties: map[string]any{"i": int64(i)}})
		}
		if err := engine.BulkCreateNodes(context.Background(), batch); err != nil {
			t.Fatalf("bulk create %s: %v", prefix, err)
		}
	}
	return ids
}

func l2E42Prune(t *testing.T, engine *storage.BadgerEngine) (int64, time.Duration) {
	t.Helper()
	start := time.Now()
	pruned, err := engine.PruneMVCCVersions(context.Background(), storage.MVCCPruneOptions{MaxVersionsPerKey: 1, MaxRuntime: 5 * time.Second})
	if err != nil {
		t.Fatalf("PruneMVCCVersions: %v", err)
	}
	return pruned, time.Since(start)
}

// l2E42RunRound seeds one round and runs the delete; between staging and
// commit it calls during (nil: nothing).
func l2E42RunRound(t *testing.T, engine *storage.BadgerEngine, round int, pruneBefore bool, during func() (int64, time.Duration)) l2E42Round {
	t.Helper()
	ctx := context.Background()
	history := l2E42Bulk(t, engine, fmt.Sprintf("h%d-", round), "ZZHist", l2E42History)
	for i, id := range history {
		node := &storage.Node{ID: id, Labels: []string{"ZZHist"}, Properties: map[string]any{"i": int64(i), "v": int64(1)}}
		if err := engine.UpdateNode(ctx, storage.NodeUpdateRequest{After: node}); err != nil {
			t.Fatalf("update history %s: %v", id, err)
		}
	}
	victims := l2E42Bulk(t, engine, fmt.Sprintf("d%d-", round), "ZZDel", l2E42Victims)
	var result l2E42Round
	if pruneBefore {
		result.pruned, result.prune = l2E42Prune(t, engine)
	}
	if err := engine.EnsureNamespaceMVCC("nornic"); err != nil {
		t.Fatalf("EnsureNamespaceMVCC: %v", err)
	}
	start := time.Now()
	tx, err := engine.BeginTransaction(ctx, storage.OrdinaryBegin)
	if err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	if err := tx.SetImplicit(true); err != nil {
		_ = tx.Rollback()
		t.Fatalf("SetImplicit: %v", err)
	}
	for _, id := range victims {
		if err := tx.DeleteGraphNode(ctx, id, storage.RejectConnected); err != nil {
			_ = tx.Rollback()
			t.Fatalf("delete %s: %v", id, err)
		}
	}
	result.staging = time.Since(start)
	if during != nil {
		result.pruned, result.prune = during()
	}
	start = time.Now()
	if err := tx.CommitContext(ctx); err != nil {
		t.Fatalf("commit delete: %v", err)
	}
	result.commit = time.Since(start)
	for _, id := range []storage.NodeID{victims[0], victims[len(victims)/2], victims[len(victims)-1]} {
		if _, err := engine.GetNode(ctx, id); !errors.Is(err, storage.ErrNotFound) {
			result.survived++
		}
	}
	return result
}

func l2E42RunScenario(t *testing.T, scenario l2E42Scenario, rounds int) []l2E42Round {
	t.Helper()
	_, engine := l2E42Open(t, scenario.retention)
	var during func() (int64, time.Duration)
	if scenario.overlap {
		during = func() (int64, time.Duration) { return l2E42Prune(t, engine) }
	}
	results := make([]l2E42Round, 0, rounds)
	for round := 1; round <= rounds; round++ {
		results = append(results, l2E42RunRound(t, engine, round, !scenario.overlap, during))
	}
	return results
}

func l2E42MedianCommit(results []l2E42Round) time.Duration {
	values := make([]time.Duration, 0, len(results))
	for _, r := range results {
		values = append(values, r.commit)
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return values[len(values)/2]
}

func l2E42Describe(name string, results []l2E42Round) string {
	s := name + ":"
	for _, r := range results {
		s += fmt.Sprintf(" [commit=%v staging=%v prune=%v pruned=%d survived=%d]", r.commit.Round(time.Millisecond), r.staging.Round(time.Millisecond), r.prune.Round(time.Millisecond), r.pruned, r.survived)
	}
	return s
}

func TestL2_e42_Repro(t *testing.T) {
	a := l2E42RunScenario(t, l2E42Scenario{name: "A", retention: 100, overlap: true}, l2E42Rounds)
	b := l2E42RunScenario(t, l2E42Scenario{name: "B", retention: 100, overlap: false}, l2E42Rounds)
	c := l2E42RunScenario(t, l2E42Scenario{name: "C", retention: 0, overlap: true}, l2E42Rounds)
	medA, medB, medC := l2E42MedianCommit(a), l2E42MedianCommit(b), l2E42MedianCommit(c)
	reference := max(medB, medC)
	ratio := float64(medA) / float64(max(reference, time.Millisecond))
	figures := fmt.Sprintf("commit median A(ret100,overlap)=%v B(ret100,prune before)=%v C(ret0)=%v ratio=%.2f (threshold >%.1f and +%v); %s; %s; %s",
		medA, medB, medC, ratio, l2E42MaxRatio, l2E42MinExtra, l2E42Describe("A", a), l2E42Describe("B", b), l2E42Describe("C", c))
	for _, set := range [][]l2E42Round{a, b, c} {
		for _, r := range set {
			if r.survived != 0 {
				t.Fatalf("a committed delete left victims readable: %s", figures)
			}
		}
	}
	// The overlap only exists if the overlapping prune committed work.
	var overlapPruned int64
	for _, r := range a {
		overlapPruned += r.pruned
	}
	if overlapPruned < int64(l2E42History*l2E42Rounds/2) {
		t.Logf("L2RESULT E42 INCONCLUSIVE overlapping prune deleted only %d records; %s", overlapPruned, figures)
		t.Skipf("the overlapping prune committed too little work to test the overlap: %s", figures)
	}
	if ratio > l2E42MaxRatio && medA-reference > l2E42MinExtra {
		t.Logf("L2RESULT E42 DEFECT %s", figures)
		t.Errorf("a large delete commit is slowed by prune commits made while it was open: %s", figures)
		return
	}
	t.Logf("L2RESULT E42 OK %s", figures)
}

// The control: the delete without overlap at retention 100 and with the
// default configuration commits, removes its nodes, and the prune placed
// before the transaction does delete the history it was given (so scenario B
// carries the same prune work as A).
func TestL2_e42_Control(t *testing.T) {
	b := l2E42RunScenario(t, l2E42Scenario{name: "B", retention: 100, overlap: false}, 1)
	c := l2E42RunScenario(t, l2E42Scenario{name: "C", retention: 0, overlap: false}, 1)
	if b[0].survived != 0 || c[0].survived != 0 {
		t.Fatalf("victims readable after commit: %s; %s", l2E42Describe("B", b), l2E42Describe("C", c))
	}
	// Each history node keeps one archived version at retention 100; the
	// prune must find most of them.
	if b[0].pruned < l2E42History/2 {
		t.Fatalf("retention-100 prune deleted %d records, want at least %d: %s", b[0].pruned, l2E42History/2, l2E42Describe("B", b))
	}
	t.Logf("L2RESULT E42 CONTROL OK %s; %s", l2E42Describe("B", b), l2E42Describe("C", c))
}

// The cause the entry points at, isolated from the prune: any commits made by
// others while the large delete is open (here 6,000 ordinary single-node
// creates in the same namespace, default configuration, no history, no prune)
// are what its commit pays for. If the commit grows with the number of such
// commits, the cost is the conflict check over the commits retained since the
// delete's read (Badger detects conflicts, badger.go:953; NornicDB's discard
// timestamp stays at the oldest open read, badger_managed.go:289), and the
// prune is one source of them.
func TestL2_e42_Cause(t *testing.T) {
	_, engine := l2E42Open(t, 0)
	run := func(round, others int) l2E42Round {
		return l2E42RunRound(t, engine, round, false, func() (int64, time.Duration) {
			start := time.Now()
			for i := 0; i < others; i++ {
				id := storage.NodeID(fmt.Sprintf("nornic:o%d-%d", round, i))
				if _, err := engine.CreateNode(context.Background(), &storage.Node{ID: id, Labels: []string{"ZZOther"}}); err != nil {
					t.Fatalf("create %s: %v", id, err)
				}
			}
			return int64(others), time.Since(start)
		})
	}
	var none, some []l2E42Round
	for round := 1; round <= l2E42Rounds; round++ {
		none = append(none, run(2*round, 0))
		some = append(some, run(2*round+1, l2E42History))
	}
	t.Logf("L2CAUSE E42 commit median with 0 concurrent commits=%v with %d=%v ratio=%.2f; %s; %s",
		l2E42MedianCommit(none), l2E42History, l2E42MedianCommit(some),
		float64(l2E42MedianCommit(some))/float64(max(l2E42MedianCommit(none), time.Millisecond)),
		l2E42Describe("none", none), l2E42Describe("concurrent", some))
}
