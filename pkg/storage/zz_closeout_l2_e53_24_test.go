//go:build closeout_l2_e53_24

package storage

// L2 close-out, report entry 53.24: a budgeted MVCC prune made no progress on
// one chain of 20,000 versions with budgets of 5, 20 and 50 ms. Each call
// spent its budget on the chain's first (and only) key and committed nothing,
// so repeating the call never got further (PruneMVCCVersions,
// badger_mvcc_prune.go:313 at the pin; its documentation's exception "unless
// that first key's own work exceeds MaxRuntime").

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

const (
	l2E53_24Chain  = 20000
	l2E53_24Cycles = 5
)

// l2E53_24Seed builds the entry's engine and chain: test:ei2-longchain with no
// properties, then n version records of the same empty node written directly
// under its version key at one hour ago + i ns, commit sequence i+1, in
// Badger transactions of 1,000 records.
func l2E53_24Seed(t *testing.T, n int) *BadgerEngine {
	t.Helper()
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{InMemory: true, EngineOptions: EngineOptions{RetentionPolicy: RetentionPolicy{MaxVersionsPerKey: 100}}})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	ctx := context.Background()
	id := NodeID("test:ei2-longchain")
	if _, err := engine.CreateNode(ctx, &Node{ID: id}); err != nil {
		t.Fatalf("create: %v", err)
	}
	numID, ok := engine.idDict.lookupNodeNumID(id)
	if !ok {
		t.Fatalf("no numeric ID for %s", id)
	}
	body, err := encodeMVCCNodeRecord(&Node{ID: id}, false)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	base := time.Now().Add(-time.Hour)
	for first := 0; first < n; first += 1000 {
		if err := engine.withUpdate(ctx, updateRequest{}, func(txn *badger.Txn) error {
			for i := first; i < min(first+1000, n); i++ {
				version := MVCCVersion{CommitTimestamp: base.Add(time.Duration(i)), CommitSequence: uint64(i + 1)}
				if err := txn.Set(mvccNodeVersionKey(numID, version), body); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatalf("seed records %d: %v", first, err)
		}
	}
	return engine
}

// l2E53_24Cycles runs the entry's five calls with one budget on a fresh chain
// and returns the records deleted and the elapsed time of each.
func l2E53_24Run(t *testing.T, n int, budget time.Duration) ([]int64, []time.Duration) {
	t.Helper()
	engine := l2E53_24Seed(t, n)
	var deleted []int64
	var elapsed []time.Duration
	for range l2E53_24Cycles {
		start := time.Now()
		d, err := engine.PruneMVCCVersions(context.Background(), MVCCPruneOptions{MaxVersionsPerKey: 1, MaxRuntime: budget})
		elapsed = append(elapsed, time.Since(start).Round(time.Millisecond))
		if err != nil {
			t.Fatalf("PruneMVCCVersions(MaxRuntime=%v): %v", budget, err)
		}
		deleted = append(deleted, d)
	}
	return deleted, elapsed
}

func l2E53_24Sum(values []int64) int64 {
	var total int64
	for _, v := range values {
		total += v
	}
	return total
}

func TestL2_e53_24_Repro(t *testing.T) {
	var figures []string
	var stalled []string
	for _, budget := range []time.Duration{5 * time.Millisecond, 20 * time.Millisecond, 50 * time.Millisecond} {
		deleted, elapsed := l2E53_24Run(t, l2E53_24Chain, budget)
		figures = append(figures, fmt.Sprintf("budget=%v deleted=%v elapsed=%v", budget, deleted, elapsed))
		// Defect: five consecutive budgeted calls commit nothing on the chain.
		// Expected (the documented resume contract): each spent call commits
		// what it did and the next resumes from there, so the chain shrinks.
		if l2E53_24Sum(deleted) == 0 {
			stalled = append(stalled, budget.String())
		}
	}
	line := strings.Join(figures, "; ")
	if len(stalled) > 0 {
		t.Logf("L2RESULT E53.24 DEFECT no progress in %d cycles with budgets %v; %s", l2E53_24Cycles, stalled, line)
		t.Errorf("budgeted prune made no progress on one %d-version chain with budgets %v: %s", l2E53_24Chain, stalled, line)
		return
	}
	t.Logf("L2RESULT E53.24 OK %s", line)
}

// The control: the lifecycle's default budget (5 s) and an unbudgeted call
// prune the same chain to its newest record at once (19,999 records).
func TestL2_e53_24_Control(t *testing.T) {
	for _, budget := range []time.Duration{5 * time.Second, 0} {
		deleted, elapsed := l2E53_24Run(t, l2E53_24Chain, budget)
		// The entry: 19,999 with 1 s and 5 s. The node's own create version
		// may or may not count as a chain record, so one either side is kept.
		if total := l2E53_24Sum(deleted); total < l2E53_24Chain-2 || total > l2E53_24Chain || deleted[0] != total {
			t.Fatalf("budget=%v deleted %v (total %d), want about %d in the first call; elapsed %v", budget, deleted, total, l2E53_24Chain-1, elapsed)
		}
		t.Logf("L2RESULT E53.24 CONTROL OK budget=%v deleted=%v elapsed=%v", budget, deleted, elapsed)
	}
}

// The cause the entry names: the first key's own work (its scan and one
// step of up to mvccPruneChunk records) exceeds the budget. Logged: the chunk
// size; how long one unbudgeted call takes on the chain (an upper bound of
// the key's whole work); and that a 5 ms budget does make progress on a short
// chain whose work fits it, so the stall belongs to the long key's work.
func TestL2_e53_24_Cause(t *testing.T) {
	engine := l2E53_24Seed(t, l2E53_24Chain)
	chunk := engine.mvccPruneChunk()
	start := time.Now()
	deleted, err := engine.PruneMVCCVersions(context.Background(), MVCCPruneOptions{MaxVersionsPerKey: 1})
	whole := time.Since(start)
	if err != nil {
		t.Fatalf("unbudgeted prune: %v", err)
	}
	shortDeleted, shortElapsed := l2E53_24Run(t, 200, 5*time.Millisecond)
	stepDeleted, stepElapsed := l2E53_24Run(t, chunk+1, 5*time.Millisecond)
	t.Logf("L2CAUSE E53.24 chunk=%d unbudgeted 20000-chain: deleted=%d in %v; 5ms budget on 200-chain: deleted=%v elapsed=%v; 5ms budget on %d-chain: deleted=%v elapsed=%v",
		chunk, deleted, whole, shortDeleted, shortElapsed, chunk+1, stepDeleted, stepElapsed)
}
