//go:build closeout_l2_e53_25

package search

// E53.25: HNSW catch-up after an index build, the entry's internal
// schedules, on the pin's own catch-up (pkg/search/hnsw_catch_up.go) and
// strategy transition (Service.runStrategyTransition). The probe the entry
// was first written with drove a hook of a reviewed pull request
// (hnswCatchUpHook) that does not exist at the pin; these schedules use only
// what the pin has: hnswCatchUp.begin/end/pay/step/popLocked/completeLocked/
// liveUpdate/finishLocked, applyCatchUpChange, and the Service fields the
// transition reads.

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// l2E53_25Schedule1 is schedule (1): a payment of an ended catch-up completes
// inside the next one. Returns current.Size() and whether x was still in
// flight after the old payment returned.
func l2E53_25Schedule1(t *testing.T) (size int, stillInFlight bool) {
	t.Helper()
	var c hnswCatchUp
	old := NewHNSWIndex(2, DefaultHNSWConfig())
	current := NewHNSWIndex(2, DefaultHNSWConfig())

	// 1.
	c.begin(old, map[string][]float32{"x": {1, 0}}, nil)
	// 2. old.mu held by the test; the payment blocks inside its insert.
	old.mu.Lock()
	oldDone := make(chan struct{})
	go func() { c.pay(1); close(oldDone) }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		c.mu.Lock()
		_, busy := c.inFlight["x"]
		c.mu.Unlock()
		if busy {
			break
		}
		if time.Now().After(deadline) {
			old.mu.Unlock()
			t.Fatalf("the old payment did not take x within 10 s")
		}
		runtime.Gosched()
	}
	// 3.
	c.end()
	c.begin(current, nil, map[string]struct{}{"x": {}})
	// 4. The removal of x is taken, not applied yet.
	c.mu.Lock()
	id, value, ok := c.popLocked()
	c.mu.Unlock()
	if !ok || id != "x" || value != nil {
		old.mu.Unlock()
		t.Fatalf("harness: popLocked returned %q %v %v, want the removal of x", id, value, ok)
	}
	// 5.
	old.mu.Unlock()
	select {
	case <-oldDone:
	case <-time.After(10 * time.Second):
		t.Fatalf("the old payment did not return within 10 s")
	}
	c.mu.Lock()
	_, stillInFlight = c.inFlight["x"]
	c.mu.Unlock()
	// 6.
	c.liveUpdate("x", []float32{0, 1})
	stepped, err := c.step()
	t.Logf("schedule 1: step after liveUpdate returned %v (err %v)", stepped, err)
	// 7.
	if err := applyCatchUpChange(current, id, value); err != nil {
		t.Fatalf("applyCatchUpChange: %v", err)
	}
	c.mu.Lock()
	c.completeLocked(id, nil)
	c.mu.Unlock()
	for i := 0; i < 1000; i++ {
		more, err := c.step()
		if err != nil {
			t.Fatalf("step: %v", err)
		}
		if !more {
			break
		}
	}
	finished, err := c.finishLocked()
	t.Logf("schedule 1: finishLocked=%v (err %v)", finished, err)
	return current.Size(), stillInFlight
}

// l2E53_25Schedule3 is schedule (3): a strategy transition replays into a
// graph that was replaced meanwhile. Returns old's slot count before and
// after the transition.
func l2E53_25Schedule3(t *testing.T) (before, after int, replacementServes bool) {
	t.Helper()
	t.Setenv("NORNICDB_GPU_BACKEND", "cpu")
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	ctx := context.Background()
	svc := NewService(storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic"), 2, nil)
	t.Cleanup(func() { _ = svc.Close() })
	put := func(vec []float32) {
		if err := svc.IndexNode(&storage.Node{ID: "x", ChunkEmbeddings: [][]float32{vec}}); err != nil {
			t.Fatalf("IndexNode: %v", err)
		}
	}
	// 1.
	put([]float32{1, 0})
	old, err := svc.getOrCreateHNSWIndex(ctx, 2)
	if err != nil {
		t.Fatalf("getOrCreateHNSWIndex: %v", err)
	}
	if _, err := svc.getOrCreateVectorPipeline(ctx); err != nil {
		t.Fatalf("getOrCreateVectorPipeline: %v", err)
	}
	if got := svc.currentPipelineStrategy(); got != strategyModeBruteCPU {
		t.Fatalf("harness: strategy is %v, want CPU brute force", got)
	}
	// 2.
	svc.strategyTransitionMu.Lock()
	svc.strategyTransitionInProgress = true
	svc.strategyTransitionMu.Unlock()
	for i := 0; i < 20; i++ {
		put([]float32{float32(i + 1), 1})
	}
	old.mu.RLock()
	before = len(old.nodeLevel)
	old.mu.RUnlock()
	// 3.
	replacement, _, err := svc.buildHNSWSnapshot(ctx, 2, svc.hnswConfig(), svc.vectorIndex, nil, nil)
	if err != nil {
		t.Fatalf("buildHNSWSnapshot: %v", err)
	}
	job := &hnswBuildJob{ctx: svc.lifecycleCtx, cancel: func() {}, index: old}
	svc.hnswMu.Lock()
	svc.hnswIndex = nil
	svc.hnswBuild = job
	svc.hnswMu.Unlock()
	done := make(chan struct{})
	go func() { svc.runStrategyTransition(strategyModeHNSW); close(done) }()
	waitDeadline := time.Now().Add(10 * time.Second)
	for {
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		if strings.Contains(string(buf[:n]), ".getOrCreateHNSWIndex(") {
			break
		}
		if time.Now().After(waitDeadline) {
			t.Fatalf("the transition did not reach getOrCreateHNSWIndex within 10 s")
		}
		time.Sleep(time.Millisecond)
	}
	// 4.
	svc.pipelineMu.Lock()
	svc.hnswMu.Lock()
	svc.publishVectorServing(func() { svc.hnswIndex = replacement }, false)
	svc.hnswMu.Unlock()
	_ = job.built.Do(func() error { return nil })
	svc.pipelineMu.Unlock()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("the transition did not return within 10 s")
	}
	old.mu.RLock()
	after = len(old.nodeLevel)
	old.mu.RUnlock()
	svc.hnswMu.RLock()
	replacementServes = svc.servingHNSW() == replacement
	svc.hnswMu.RUnlock()
	return before, after, replacementServes
}

// l2E53_25Schedule4 is schedule (4): a write to the in-flight key after
// every builder step, 100 rounds.
func l2E53_25Schedule4(t *testing.T) (pending, orderLeft int) {
	t.Helper()
	var c hnswCatchUp
	graph := NewHNSWIndex(2, DefaultHNSWConfig())
	c.begin(graph, map[string][]float32{"x": {1, 0}}, nil)
	for round := 0; round < 100; round++ {
		c.mu.Lock()
		id, value, ok := c.popLocked()
		c.mu.Unlock()
		if !ok {
			t.Fatalf("round %d: nothing to pop", round)
		}
		c.liveUpdate("x", []float32{0, 1})
		c.pay(1)
		if err := applyCatchUpChange(graph, id, value); err != nil {
			t.Fatalf("applyCatchUpChange: %v", err)
		}
		c.mu.Lock()
		c.completeLocked(id, nil)
		c.mu.Unlock()
	}
	c.mu.Lock()
	orderLeft = len(c.order) - c.next
	c.mu.Unlock()
	return c.pending(), orderLeft
}

// l2E53_25Schedule2 is schedule (2): time of begin with indexMu held, for a
// backlog of 10,000, 100,000 and 1,000,000 entries (one measurement each, as
// in the entry; no expectation is stated there).
func l2E53_25Schedule2(t *testing.T) []time.Duration {
	t.Helper()
	svc := NewService(storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic"), 2, nil)
	t.Cleanup(func() { _ = svc.Close() })
	var out []time.Duration
	for _, count := range []int{10000, 100000, 1000000} {
		backlog := make(map[string][]float32, count)
		for i := 0; i < count; i++ {
			backlog[fmt.Sprint(i)] = []float32{1, 0}
		}
		graph := NewHNSWIndex(2, DefaultHNSWConfig())
		svc.indexMu.Lock()
		start := time.Now()
		svc.hnswCatchUp.begin(graph, backlog, nil)
		elapsed := time.Since(start)
		svc.hnswCatchUp.end()
		svc.indexMu.Unlock()
		out = append(out, elapsed)
	}
	return out
}

// Repro: schedules (1) and (3) carry the entry's Expected values and fail
// when the defect is present. Schedules (2) and (4) state observations with
// no Expected in the entry; they are logged (L2RESULT INFO), not asserted.
func TestL2_e53_25_Repro(t *testing.T) {
	t.Run("schedule1_late_payment", func(t *testing.T) {
		size, stillInFlight := l2E53_25Schedule1(t)
		if size != 1 {
			t.Logf("L2RESULT E53.25/1 DEFECT current.Size()=%d (want 1) x_in_flight_after_old_payment=%v", size, stillInFlight)
			t.Errorf("schedule 1: the newest change of x was lost: current.Size()=%d, want 1", size)
			return
		}
		t.Logf("L2RESULT E53.25/1 OK current.Size()=%d x_in_flight_after_old_payment=%v", size, stillInFlight)
	})
	t.Run("schedule3_transition_replay", func(t *testing.T) {
		before, after, replacementServes := l2E53_25Schedule3(t)
		if after != before {
			t.Logf("L2RESULT E53.25/3 DEFECT old_slots_before=%d old_slots_after=%d (want %d) replacement_serves=%v", before, after, before, replacementServes)
			t.Errorf("schedule 3: the transition replayed %d inserts into a graph that was replaced", after-before)
			return
		}
		t.Logf("L2RESULT E53.25/3 OK old_slots_before=%d old_slots_after=%d replacement_serves=%v", before, after, replacementServes)
	})
	t.Run("schedule2_begin_time_info", func(t *testing.T) {
		d := l2E53_25Schedule2(t)
		t.Logf("L2RESULT E53.25/2 INFO begin_with_indexMu_held 10k=%v 100k=%v 1M=%v", d[0], d[1], d[2])
	})
	t.Run("schedule4_inflight_writes_info", func(t *testing.T) {
		pending, orderLeft := l2E53_25Schedule4(t)
		t.Logf("L2RESULT E53.25/4 INFO after_100_rounds pending=%d work_order_entries=%d", pending, orderLeft)
	})
}

// Control: the catch-up without interleaving: begin with x, a live update,
// steps until done, finish: the graph holds x with the newest vector (size 1);
// and the service serves a vector through its public Search.
func TestL2_e53_25_Control(t *testing.T) {
	var c hnswCatchUp
	current := NewHNSWIndex(2, DefaultHNSWConfig())
	c.begin(current, map[string][]float32{"x": {1, 0}}, nil)
	c.liveUpdate("x", []float32{0, 1})
	for i := 0; i < 1000; i++ {
		more, err := c.step()
		if err != nil {
			t.Fatalf("step: %v", err)
		}
		if !more {
			break
		}
	}
	finished, err := c.finishLocked()
	if err != nil || !finished || current.Size() != 1 || !hnswHoldsLive(current, "x") {
		t.Errorf("control catch-up: finished=%v err=%v size=%d holds_x=%v", finished, err, current.Size(), hnswHoldsLive(current, "x"))
	}

	servingTestEnv(t)
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	svc := NewService(engine, 2, nil)
	t.Cleanup(func() { _ = svc.Close() })
	node := &storage.Node{ID: "x", Labels: []string{"Doc"}, Properties: map[string]any{"text": "text"}, ChunkEmbeddings: [][]float32{{0, 1}}}
	if _, err := engine.CreateNode(context.Background(), node); err != nil {
		t.Fatalf("CreateNode: %v", err)
	}
	if err := svc.IndexNode(node); err != nil {
		t.Fatalf("IndexNode: %v", err)
	}
	if err := svc.BuildIndexes(context.Background()); err != nil {
		t.Fatalf("BuildIndexes: %v", err)
	}
	opts := DefaultSearchOptions()
	opts.Limit = 1
	resp, err := svc.Search(context.Background(), "", []float32{0, 1}, opts)
	if err != nil || len(resp.Results) != 1 {
		n := -1
		if resp != nil {
			n = len(resp.Results)
		}
		t.Errorf("control search: %d results, err %v", n, err)
	}
	t.Logf("control: catch-up size=%d finished=%v; service search ok", current.Size(), finished)
}

// Cause: (1) completeLocked (pkg/search/hnsw_catch_up.go) deletes the key
// from inFlight with no check of which begin the payment belonged to, so the
// ended catch-up's payment clears the marker of the next catch-up's insert
// of the same key: logged as x_in_flight_after_old_payment (false = the
// cause). (3) runStrategyTransition (pkg/search/search.go) skips the replay
// only for the graph that serves; a graph the shared job returned after a
// replacement was published is not that graph: logged as replacement_serves
// together with the replay count.
func TestL2_e53_25_Cause(t *testing.T) {
	size, stillInFlight := l2E53_25Schedule1(t)
	t.Logf("CAUSE schedule1 x_in_flight_after_old_payment=%v (false: the old payment removed the new generation's marker) current.Size()=%d", stillInFlight, size)
	before, after, replacementServes := l2E53_25Schedule3(t)
	t.Logf("CAUSE schedule3 replacement_serves=%v replayed_into_old=%d", replacementServes, after-before)
}
