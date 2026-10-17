package antlr

import (
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func setMaxParserDFAStates(t *testing.T, n int) {
	t.Helper()
	previous := maxParserDFAStates
	maxParserDFAStates = n
	t.Cleanup(func() { maxParserDFAStates = previous })
}

// setExactCountWindow sets exactCountWindow for one test. Tests of the
// budget and of drains by estimate lengthen it so that only those drain.
func setExactCountWindow(t *testing.T, d time.Duration) {
	t.Helper()
	previous := exactCountWindow
	exactCountWindow = d
	t.Cleanup(func() { exactCountWindow = previous })
}

// distinctExpression builds a random expression whose shape (not only its
// literals) differs between seeds, so each one adds prediction states.
func distinctExpression(r *rand.Rand, depth int) string {
	if depth == 0 {
		return []string{"n.p", "1", "'s'", "$p", "x"}[r.Intn(5)]
	}
	a, b := distinctExpression(r, depth-1), distinctExpression(r, depth-1)
	switch r.Intn(6) {
	case 0:
		return "[x IN " + a + " WHERE x = " + b + " | x]"
	case 1:
		return "CASE WHEN " + a + " THEN " + b + " END"
	case 2:
		return "{k: " + a + ", j: " + b + "}"
	case 3:
		return "coalesce(" + a + ", " + b + ")"
	case 4:
		return "(" + a + " + " + b + ")"
	default:
		return a + " IN [" + b + "]"
	}
}

// distinctStatement is a statement whose token-type sequence differs between
// seeds, so each one is a new shape.
func distinctStatement(r *rand.Rand) string {
	var b strings.Builder
	b.WriteString("MATCH (n) WITH n, 1 AS x")
	for j := 0; j < 3; j++ {
		fmt.Fprintf(&b, ", %s AS v%d", distinctExpression(r, 2+r.Intn(2)), j)
	}
	b.WriteString(" RETURN v0, v1, v2")
	return b.String()
}

// Under continuous concurrency some parse always holds the current
// generation, so it is never idle for an exact count. The bound holds
// anyway through the estimate from new-shape tokens, and every generation
// stays near the limit while the workload builds far more states.
func TestValidatePredictionBoundedUnderContinuousLoad(t *testing.T) {
	const limit = 1000
	count, total, largest, _ := runContinuousPredictionLoad(t, limit)
	if total <= 3*limit {
		t.Fatalf("workload built only %d states; it must exceed the bound to test it", total)
	}
	if count < 2 {
		t.Fatalf("the generation never rotated under continuous load (%d states in one generation)", total)
	}
	if largest > 3*limit {
		t.Fatalf("a generation reached %d states, past 3x the limit of %d", largest, limit)
	}
}

// A parse never waits for parses already running: replacing a generation
// takes no lock, and the exact count runs only when no parse holds the
// generation. While a holder keeps a parse open for the whole test (a long
// cold parse), taking a generation for a new parse stays under
// maxAcquireWait, though generations rotate throughout.
func TestValidatePredictionNeverWaitsForRunningParses(t *testing.T) {
	const maxAcquireWait = 50 * time.Millisecond
	_, _, _, waits := runContinuousPredictionLoad(t, 1000)
	sort.Slice(waits, func(i, j int) bool { return waits[i] < waits[j] })
	worst := waits[len(waits)-1]
	t.Logf("acquire waits n=%d p50=%v p99=%v max=%v", len(waits), waits[len(waits)/2], waits[len(waits)*99/100], worst)
	if worst > maxAcquireWait {
		t.Fatalf("a parse waited %v for a generation, past %v", worst, maxAcquireWait)
	}
}

// A busy generation that only sees statement shapes it has parsed before
// keeps its estimated size: it is never replaced, however many statements
// it parses while a parse holds it the whole time.
func TestValidatePredictionRepeatedShapesKeepGeneration(t *testing.T) {
	setMaxParserDFAStates(t, 1_000_000)
	first := acquirePrediction()
	first.release(nil)
	if err := Validate("MATCH (n:L) WHERE n.v = 0 AND n.s = 's0' RETURN n.x + 0 AS v"); err != nil {
		t.Fatal(err)
	}
	setMaxParserDFAStates(t, int(first.exactStates.Load())+1)
	holder := acquirePrediction()
	defer holder.release(nil)
	for i := 0; i < 500; i++ {
		// Literal values differ, token types don't.
		if err := Validate(fmt.Sprintf("MATCH (n:L) WHERE n.v = %d AND n.s = 's%d' RETURN n.x + %d AS v", i, i, i)); err != nil {
			t.Fatal(err)
		}
	}
	if currentPrediction.Load() != first {
		t.Fatalf("a repeated statement shape replaced the generation")
	}
}

// runContinuousPredictionLoad validates distinct statements from 8 workers
// while a holder keeps a parse open on a generation for the whole run, as a
// long cold parse does, and a second pair of holders keeps the current
// generation busy. It returns how many generations the parses used, their
// total and largest DFA state counts, and how long each worker waited to
// take a generation.
func runContinuousPredictionLoad(t *testing.T, limit int) (count, total, largest int, waits []time.Duration) {
	t.Helper()
	setMaxParserDFAStates(t, limit)

	var generations sync.Map
	record := func() { generations.Store(currentPrediction.Load(), true) }

	var stop atomic.Bool
	var holders sync.WaitGroup
	long := acquirePrediction()
	for holder := 0; holder < 2; holder++ {
		holders.Add(1)
		go func(offset time.Duration) {
			defer holders.Done()
			time.Sleep(offset)
			for !stop.Load() {
				held := acquirePrediction()
				time.Sleep(2 * time.Millisecond)
				held.release(nil)
			}
		}(time.Duration(holder) * time.Millisecond)
	}

	const workers, perWorker = 8, 40
	var wg sync.WaitGroup
	var waitsMu sync.Mutex
	errs := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(worker + 1)))
			for i := 0; i < perWorker; i++ {
				started := time.Now()
				p := acquirePrediction()
				waited := time.Since(started)
				p.release(nil)
				waitsMu.Lock()
				waits = append(waits, waited)
				waitsMu.Unlock()
				if err := Validate(distinctStatement(r)); err != nil {
					errs <- fmt.Errorf("worker %d statement %d: %v", worker, i, err)
					return
				}
				record()
			}
		}(worker)
	}
	wg.Wait()
	stop.Store(true)
	holders.Wait()
	long.release(nil)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	generations.Range(func(key, _ any) bool {
		p := key.(*parserPrediction)
		p.mu.Lock()
		states := p.dfaStates()
		p.mu.Unlock()
		count++
		total += states
		if states > largest {
			largest = states
		}
		return true
	})
	t.Logf("generations=%d total states=%d largest=%d limit=%d", count, total, largest, limit)
	return count, total, largest, waits
}

// The prediction DFAs are bounded: past maxParserDFAStates a new generation
// starts, and verdicts are the same before and after the reset.
func TestValidatePredictionGenerationRotates(t *testing.T) {
	setMaxParserDFAStates(t, 1)
	statements := map[string]bool{
		"MATCH (n:Person) WHERE n.age > 3 RETURN n.name AS name":                              true,
		"MATCH (a)-[:KNOWS]->(b) WITH a, collect(b) AS bs RETURN a, size(bs) AS c":            true,
		"RETURN [x IN range(1, 5) WHERE x % 2 = 0 | CASE WHEN x > 2 THEN x ELSE 0 END] AS xs": true,
		"MATCH (n RETURN n":    false,
		"RETURN 1 +":           false,
		"MATCH (n) RETRUN n.x": false,
	}
	first := acquirePrediction()
	first.release(nil)
	for round := 0; round < 3; round++ {
		for statement, valid := range statements {
			if err := Validate(statement); (err == nil) != valid {
				t.Fatalf("round %d: Validate(%q) error = %v, want valid=%v", round, statement, err, valid)
			}
		}
	}
	if currentPrediction.Load() == first {
		t.Fatalf("prediction generation never rotated past a limit of 1 state")
	}
	if _, err := Parse("MATCH (n) RETURN n"); err != nil {
		t.Fatalf("Parse after rotation: %v", err)
	}
}

// Generations rotate safely while other parses run (go test -race).
func TestValidatePredictionGenerationConcurrent(t *testing.T) {
	setMaxParserDFAStates(t, 50)
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 60; i++ {
				statement := fmt.Sprintf("MATCH (n:L%d) WHERE n.v = %d RETURN [x IN n.xs WHERE x > %d | x * 2] AS v", worker, i, i)
				if err := Validate(statement); err != nil {
					errs <- err
					return
				}
				if err := Validate("MATCH (n RETURN n"); err == nil {
					errs <- fmt.Errorf("invalid statement accepted")
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

// A retired generation is freed by the first GC after its parses end: no
// pooled validator keeps it reachable (sync.Pool would otherwise hold it
// across one more GC, and until reuse between GCs).
func TestValidateRetiredPredictionIsCollected(t *testing.T) {
	setMaxParserDFAStates(t, 1)
	var collected atomic.Bool
	func() {
		p := acquirePrediction()
		p.release(nil)
		runtime.SetFinalizer(currentPrediction.Load(), func(*parserPrediction) { collected.Store(true) })
	}()
	// A burst of parses: the first one past the limit retires the generation.
	for i := 0; i < 8; i++ {
		if err := Validate(fmt.Sprintf("MATCH (n:L%d) RETURN n.x + %d AS v", i, i)); err != nil {
			t.Fatal(err)
		}
	}
	runtime.GC()
	deadline := time.Now().Add(2 * time.Second)
	for !collected.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !collected.Load() {
		t.Fatalf("the retired generation was still reachable after its parses ended and one GC")
	}
}

// The DFA state limit is a quarter of the runtime memory limit, and 2 GiB of
// states without one.
func TestParserDFAStateLimitFromMemoryLimit(t *testing.T) {
	previous := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(previous) })

	debug.SetMemoryLimit(8 << 30)
	if got, want := parserDFAStateLimit(), int64(8<<30)/4/bytesPerDFAState; got != want {
		t.Fatalf("limit with an 8 GiB memory limit = %d states, want %d", got, want)
	}
	debug.SetMemoryLimit(math.MaxInt64)
	if got, want := parserDFAStateLimit(), int64(defaultPredictionBudget)/bytesPerDFAState; got != want {
		t.Fatalf("limit without a memory limit = %d states, want %d", got, want)
	}
	setMaxParserDFAStates(t, 7)
	if got := parserDFAStateLimit(); got != 7 {
		t.Fatalf("overridden limit = %d, want 7", got)
	}
}

// longFlatStatement is a long statement of seed-dependent operator order:
// every seed is a new shape of many tokens, but long flat expressions reuse
// their prediction states, so the shapes build far fewer states than tokens.
func longFlatStatement(seed int) string {
	r := rand.New(rand.NewSource(int64(seed)))
	ops := []string{" + ", " - ", " * "}
	var b strings.Builder
	b.WriteString("RETURN 1")
	for i := 0; i < 300; i++ {
		b.WriteString(ops[r.Intn(len(ops))])
		b.WriteString("n.p")
	}
	b.WriteString(" AS v")
	return b.String()
}

// Distinct statements parsed one after another keep their generation while
// its states fit the limit, however many new-shape tokens they hold: an exact
// count replaces the token estimate whenever the generation is idle, so a
// working set that fits keeps its warm predictions (the first run of each
// statement pays no cold prediction for shapes another statement built).
func TestValidatePredictionWorkingSetKeepsGeneration(t *testing.T) {
	const limit = 3000
	setMaxParserDFAStates(t, 1_000_000)
	retireCurrentPredictionForTest()
	first := acquirePrediction()
	first.release(nil)
	setMaxParserDFAStates(t, limit)
	for seed := 0; seed < 40; seed++ {
		if err := Validate(longFlatStatement(seed)); err != nil {
			t.Fatal(err)
		}
	}
	tokens, states := first.newShapeTokens.Load(), first.exactStates.Load()
	t.Logf("new-shape tokens=%d states=%d limit=%d", tokens, states, limit)
	if tokens <= 2*limit {
		t.Fatalf("the statements held only %d new-shape tokens; they must exceed twice the limit of %d", tokens, limit)
	}
	if states > limit {
		t.Fatalf("the statements built %d states, past the limit of %d; the fixture must fit", states, limit)
	}
	if currentPrediction.Load() != first {
		t.Fatalf("an idle generation within its state limit was replaced by its new-shape token count")
	}
}

// The token estimate only starts a drain: a generation it passes while a
// long parse holds it gives new parses to a stand-in, is counted exactly when
// that parse ends, and is current again because its states fit. Before, the
// estimate replaced it, and the long statement parsed cold on its next run.
func TestValidatePredictionDrainedGenerationThatFitsIsRestored(t *testing.T) {
	const limit = 3000
	first := drainPredictionPastEstimate(t, limit)
	if !first.draining.Load() {
		t.Fatalf("the generation the estimate passed is not draining")
	}
	first.release(nil)
	tokens, states := first.newShapeTokens.Load(), first.exactStates.Load()
	t.Logf("new-shape tokens=%d states=%d limit=%d", tokens, states, limit)
	if states > limit {
		t.Fatalf("the generation holds %d states, past the limit of %d; the fixture must fit", states, limit)
	}
	if currentPrediction.Load() != first {
		t.Fatalf("a drained generation within its state limit was not restored")
	}
	if first.draining.Load() {
		t.Fatalf("the restored generation is still draining")
	}
}

// A drained generation whose exact count is past the limit stays replaced.
func TestValidatePredictionDrainedGenerationPastLimitStaysRetired(t *testing.T) {
	first := drainPredictionPastEstimate(t, 3000)
	setMaxParserDFAStates(t, 1)
	first.release(nil)
	if currentPrediction.Load() == first {
		t.Fatalf("a drained generation past its state limit was restored")
	}
	if first.draining.Load() {
		t.Fatalf("a retired generation is still draining")
	}
}

// The budget covers every live generation, not only the current one: while
// long parses hold generations, the ones made meanwhile stay small instead
// of each growing to the budget again, so staggered long parses don't keep
// a full generation each (bot P1 on #1382).
func TestValidatePredictionBudgetCoversHeldGenerations(t *testing.T) {
	const limit = 2000
	setMaxParserDFAStates(t, limit)
	setExactCountWindow(t, time.Hour)
	retireCurrentPredictionForTest()
	r := rand.New(rand.NewSource(7))
	var held []*parserPrediction
	for len(held) < 4 {
		h := acquirePrediction()
		held = append(held, h)
		for n := 0; n < 400 && currentPrediction.Load() == h; n++ {
			if err := Validate(distinctStatement(r)); err != nil {
				t.Fatal(err)
			}
		}
		if currentPrediction.Load() == h {
			t.Fatalf("generation %d never passed the budget", len(held))
		}
	}
	shapes := func(p *parserPrediction) int {
		p.shapesMu.Lock()
		defer p.shapesMu.Unlock()
		return len(p.shapes)
	}
	seen := map[*parserPrediction]bool{}
	total := 0
	for i, h := range held {
		h.release(nil)
		if seen[h] {
			t.Fatalf("held parse %d took a generation an earlier held parse holds", i)
		}
		seen[h] = true
		total += int(h.exactStates.Load())
		t.Logf("held generation %d: statements=%d states=%d", i, shapes(h), h.exactStates.Load())
		// Each generation made while the earlier ones are held takes only
		// the statement that passed the budget.
		if i > 0 && shapes(h) > 1 {
			t.Fatalf("generation %d made while earlier ones held the budget took %d statements, %d states", i, shapes(h), h.exactStates.Load())
		}
	}
	t.Logf("held generations total %d states, budget %d", total, limit)
	if live := liveStates.Load(); live > limit {
		t.Fatalf("%d states still accounted after every held generation ended, past the budget of %d", live, limit)
	}
}

// A generation that only short parses hold, back to back so it is never
// idle, is still counted exactly: once it has parsed new shapes for
// exactCountWindow without a count, it drains for the milliseconds its
// parses take, is counted, and is current again. While it parses only
// shapes it has seen it keeps its exact count and is never drained.
func TestValidatePredictionShortParsesCountedAndRestored(t *testing.T) {
	setMaxParserDFAStates(t, 1_000_000)
	retireCurrentPredictionForTest()
	first := currentPrediction.Load()
	warm := func(i int) string {
		return fmt.Sprintf("MATCH (n:L) WHERE n.v = %d AND n.s = 's%d' RETURN n.x + %d AS v", i, i, i)
	}
	if err := Validate(warm(0)); err != nil {
		t.Fatal(err)
	}

	// Two holders keep short parses on the current generation, overlapping,
	// so it never has a moment without a parse.
	var stop atomic.Bool
	var holders sync.WaitGroup
	for holder := 0; holder < 2; holder++ {
		holders.Add(1)
		go func(offset time.Duration) {
			defer holders.Done()
			time.Sleep(offset)
			for !stop.Load() {
				held := acquirePrediction()
				time.Sleep(2 * time.Millisecond)
				held.release(nil)
			}
		}(time.Duration(holder) * time.Millisecond)
	}
	defer func() {
		stop.Store(true)
		holders.Wait()
	}()

	warmPhase := func(phase string) {
		t.Helper()
		for i := 1; i <= 50; i++ {
			if err := Validate(warm(i)); err != nil {
				t.Fatal(err)
			}
			if currentPrediction.Load() != first {
				t.Fatalf("%s: a warm generation parsing only shapes it has seen was drained", phase)
			}
			time.Sleep(time.Millisecond)
		}
	}
	warmPhase("before new shapes")

	counted := first.exactAt.Load()
	for seed := 0; seed < 10; seed++ {
		if err := Validate(longFlatStatement(seed)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(exactCountWindow + time.Millisecond)
	}
	deadline := time.Now().Add(2 * time.Second)
	for currentPrediction.Load() != first && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if currentPrediction.Load() != first {
		t.Fatalf("the generation that short parses hold was not current again after its drain")
	}
	if first.exactAt.Load() == counted {
		t.Fatalf("the generation that short parses hold was never counted exactly")
	}
	if first.tokensAtExact.Load() != first.newShapeTokens.Load() {
		t.Fatalf("the restored generation was not counted after its last new shape")
	}
	warmPhase("after the drain")
}

// A drain hands new parses to the spare, the stand-in the last restore
// replaced, instead of a cold generation.
func TestValidatePredictionDrainUsesSpare(t *testing.T) {
	const limit = 3000
	first := drainPredictionPastEstimate(t, limit)
	first.release(nil)
	spare := sparePrediction.Load()
	if currentPrediction.Load() != first || spare == nil {
		t.Fatalf("the restore did not keep the stand-in as the spare")
	}
	holder := acquirePrediction()
	for seed := 200; seed < 240 && currentPrediction.Load() == first; seed++ {
		if err := Validate(longFlatStatement(seed)); err != nil {
			t.Fatal(err)
		}
	}
	if currentPrediction.Load() != spare {
		t.Fatalf("the drain did not make the spare current")
	}
	if sparePrediction.Load() != nil {
		t.Fatalf("the spare is still held after the drain made it current")
	}
	holder.release(nil)
	if currentPrediction.Load() != first {
		t.Fatalf("the drained generation was not restored over the spare")
	}
}

// drainPredictionPastEstimate starts a fresh generation, holds a parse on it
// as a long parse does, and validates distinct long statements until their
// new-shape tokens pass limit. It returns the held generation, which the
// caller releases.
func drainPredictionPastEstimate(t *testing.T, limit int) *parserPrediction {
	t.Helper()
	setMaxParserDFAStates(t, 1_000_000)
	setExactCountWindow(t, time.Hour)
	retireCurrentPredictionForTest()
	first := acquirePrediction()
	first.release(nil)
	setMaxParserDFAStates(t, limit)
	holder := acquirePrediction()
	if holder != first {
		t.Fatalf("the held parse did not take the fresh generation")
	}
	for seed := 0; seed < 40 && currentPrediction.Load() == first; seed++ {
		if err := Validate(longFlatStatement(seed)); err != nil {
			t.Fatal(err)
		}
	}
	if currentPrediction.Load() == first {
		t.Fatalf("new-shape tokens=%d never passed the limit of %d", first.newShapeTokens.Load(), limit)
	}
	return holder
}

// retireCurrentPredictionForTest starts a fresh generation that stands in
// for none, with no spare.
func retireCurrentPredictionForTest() {
	if spare := sparePrediction.Swap(nil); spare != nil {
		spare.settle()
	}
	if old := currentPrediction.Swap(newParserPrediction(false)); old != nil {
		old.settle()
	}
}
