package antlr

import (
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antlr4-go/antlr/v4"
)

func setMaxParserDFAStates(t *testing.T, n int64) {
	t.Helper()
	previous := maxParserDFAStates.Load()
	maxParserDFAStates.Store(n)
	t.Cleanup(func() { maxParserDFAStates.Store(previous) })
}

// retireCurrentPredictionForTest starts the next parse on a fresh generation.
func retireCurrentPredictionForTest() {
	predictions.mu.Lock()
	defer predictions.mu.Unlock()
	if predictions.current != nil {
		predictions.retireCurrentLocked()
	}
}

// sharedGenerations returns the ledger's generations (current first, then
// the retired ones parses hold) and its account.
func sharedGenerations() ([]*parserPrediction, int64) {
	predictions.mu.Lock()
	defer predictions.mu.Unlock()
	var gens []*parserPrediction
	if predictions.current != nil {
		gens = append(gens, predictions.current)
	}
	for p := range predictions.retired {
		gens = append(gens, p)
	}
	return gens, predictions.states
}

func generationsMade() uint64 {
	predictions.mu.Lock()
	defer predictions.mu.Unlock()
	return predictions.made
}

// checkLedger fails unless the ledger's invariants hold: the account is the
// sum of its generations' shares, and a retired generation stays only while
// a parse holds it.
func checkLedger(t *testing.T) {
	t.Helper()
	predictions.mu.Lock()
	defer predictions.mu.Unlock()
	var sum int64
	if c := predictions.current; c != nil {
		if c.retired || !c.shared || c.readers < 0 {
			t.Fatalf("current generation: retired=%v shared=%v readers=%d", c.retired, c.shared, c.readers)
		}
		if _, ok := predictions.retired[c]; ok {
			t.Fatalf("the current generation is also listed as retired")
		}
		sum += c.states
	}
	for p := range predictions.retired {
		if !p.retired || !p.shared || p.readers <= 0 {
			t.Fatalf("retired generation: retired=%v shared=%v readers=%d", p.retired, p.shared, p.readers)
		}
		sum += p.states
	}
	if sum != predictions.states {
		t.Fatalf("account %d states, generations hold shares of %d", predictions.states, sum)
	}
}

// exactSharedStates counts the DFA states of every shared generation.
func exactSharedStates() int64 {
	gens, _ := sharedGenerations()
	var n int64
	for _, p := range gens {
		n += p.countStates()
	}
	return n
}

// parseHeld parses query with p and keeps p, as a parse still running does.
func parseHeld(p *parserPrediction, query string) {
	lexer := NewCypherLexer(antlr.NewInputStream(query))
	lexer.RemoveErrorListeners()
	parser := NewCypherParser(antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel))
	parser.RemoveErrorListeners()
	usePrediction(parser, p)
	parser.Script()
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

// smallDistinctStatement is a distinct shape that is cheap to parse cold.
func smallDistinctStatement(r *rand.Rand) string {
	return "MATCH (n) WITH n, 1 AS x RETURN " + distinctExpression(r, 1+r.Intn(2)) + " AS v"
}

// The DFA state count comes from ANTLR's own state lock, which the runtime
// keeps unexported; this fails when an upgrade of the runtime moves it.
func TestATNStateLockResolves(t *testing.T) {
	if atnStateLock() == nil {
		t.Fatalf("the parser ATN has no stateMu RWMutex; prediction generations can't be counted")
	}
}

// A release counts its generation exactly while other parses still hold it:
// the account follows the generation's real size, so a busy generation is
// never sized by an estimate.
func TestValidatePredictionCountsBusyGeneration(t *testing.T) {
	setMaxParserDFAStates(t, 1_000_000)
	retireCurrentPredictionForTest()
	holder := acquirePrediction()
	defer holder.release()
	r := rand.New(rand.NewSource(7))
	for i := 0; i < 20; i++ {
		if err := Validate(distinctStatement(r)); err != nil {
			t.Fatal(err)
		}
		gens, account := sharedGenerations()
		if len(gens) != 1 || gens[0] != holder {
			t.Fatalf("statement %d: the held generation is no longer the only one (%d)", i, len(gens))
		}
		if exact := holder.countStates(); account != exact {
			t.Fatalf("statement %d: account %d states, the held generation has %d", i, account, exact)
		}
	}
	if holder.states == 0 {
		t.Fatalf("the statements built no states; the test checks nothing")
	}
	checkLedger(t)
}

// The prediction DFAs are bounded: past the budget a new generation starts,
// and verdicts are the same before and after the reset.
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
	made := generationsMade()
	for round := 0; round < 3; round++ {
		for statement, valid := range statements {
			if err := Validate(statement); (err == nil) != valid {
				t.Fatalf("round %d: Validate(%q) error = %v, want valid=%v", round, statement, err, valid)
			}
		}
	}
	if generationsMade()-made < 2 {
		t.Fatalf("prediction generation never rotated past a budget of 1 state")
	}
	if _, err := Parse("MATCH (n) RETURN n"); err != nil {
		t.Fatalf("Parse after rotation: %v", err)
	}
	checkLedger(t)
}

// Under continuous concurrency some parse always holds the current
// generation. It is replaced anyway once its exact count passes the budget,
// and no generation grows far past it.
func TestValidatePredictionBoundedUnderContinuousLoad(t *testing.T) {
	const limit = 1000
	setMaxParserDFAStates(t, limit)
	retireCurrentPredictionForTest()
	made := generationsMade()

	var largest atomic.Int64
	var built atomic.Int64
	observe := func() {
		gens, _ := sharedGenerations()
		for _, p := range gens {
			n := p.countStates()
			for {
				old := largest.Load()
				if n <= old || largest.CompareAndSwap(old, n) {
					break
				}
			}
		}
	}

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
				held.release()
			}
		}(time.Duration(holder) * time.Millisecond)
	}
	const workers, perWorker = 8, 12
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(worker + 1)))
			for i := 0; i < perWorker; i++ {
				p := acquirePrediction()
				before := p.countStates()
				parseHeld(p, distinctStatement(r))
				built.Add(p.countStates() - before)
				p.release()
				observe()
			}
		}(worker)
	}
	wg.Wait()
	stop.Store(true)
	holders.Wait()
	t.Logf("generations=%d built>=%d largest=%d limit=%d", generationsMade()-made, built.Load(), largest.Load(), limit)
	if built.Load() <= 3*limit {
		t.Fatalf("workload built only %d states; it must exceed the bound to test it", built.Load())
	}
	if generationsMade()-made < 2 {
		t.Fatalf("the generation never rotated under continuous load")
	}
	if largest.Load() > 3*limit {
		t.Fatalf("a generation reached %d states, past 3x the budget of %d", largest.Load(), limit)
	}
	checkLedger(t)
}

// Long parses hold retired generations, and a new generation never takes a
// share of its own beside them: shared prediction state stays within the
// budget plus the parses in flight however many holders there are. Before, every
// held generation kept up to the budget (3,685 live states against 2,000
// with four holders), and later only each one's first statement.
func TestValidatePredictionBudgetCoversHeldGenerations(t *testing.T) {
	const limit = 150
	for _, holders := range []int{2, 4, 8} {
		t.Run(fmt.Sprintf("holders=%d", holders), func(t *testing.T) {
			setMaxParserDFAStates(t, limit)
			retireCurrentPredictionForTest()
			r := rand.New(rand.NewSource(int64(holders)))
			var held []*parserPrediction
			var largestStatement int64
			for h := 0; h < holders; h++ {
				p := acquirePrediction()
				before := p.countStates()
				parseHeld(p, smallDistinctStatement(r))
				if grown := p.countStates() - before; grown > largestStatement {
					largestStatement = grown
				}
				held = append(held, p)
				for i := 0; i < 3; i++ {
					before := exactSharedStates()
					if err := Validate(smallDistinctStatement(r)); err != nil {
						t.Fatal(err)
					}
					if grown := exactSharedStates() - before; grown > largestStatement {
						largestStatement = grown
					}
				}
				checkLedger(t)
				// The budget plus the two parses that can be in flight past
				// it: the holder's and the statement that started beside it.
				if shared := exactSharedStates(); shared > limit+2*largestStatement {
					t.Fatalf("after %d holders, shared generations hold %d states, past the budget %d plus two statements of %d",
						h+1, shared, limit, largestStatement)
				}
			}
			private := 0
			for _, p := range held {
				if !p.shared {
					private++
				}
			}
			if holders >= 8 && private == 0 {
				t.Fatalf("%d holders never filled the budget of %d; the test checks nothing", holders, limit)
			}
			for _, p := range held {
				p.release()
				checkLedger(t)
			}
			gens, _ := sharedGenerations()
			for _, p := range gens {
				if p.retired {
					t.Fatalf("a retired generation stayed after its last parse ended")
				}
			}
			if p := acquirePrediction(); !p.shared {
				t.Fatalf("a parse after the holders ended didn't get a shared generation")
			} else {
				p.release()
			}
		})
	}
}

// Role changes and the account change together: retiring a generation two
// parses hold, a private parse past the budget and the holders' releases in
// either order leave the account equal to the generations' shares at every
// step.
func TestValidatePredictionLedgerSteps(t *testing.T) {
	setMaxParserDFAStates(t, 1_000_000)
	retireCurrentPredictionForTest()
	a := acquirePrediction()
	b := acquirePrediction()
	if a != b || !a.shared {
		t.Fatalf("two parses within the budget didn't share the current generation")
	}
	parseHeld(a, "MATCH (n:Step) WHERE n.v > 1 RETURN [x IN n.xs | x] AS v")
	a.release()
	checkLedger(t)
	if b.states == 0 {
		t.Fatalf("the release counted no states")
	}
	setMaxParserDFAStates(t, 1)
	c := acquirePrediction()
	if c.shared {
		t.Fatalf("a parse past the budget, with a held retired generation filling it, got a shared generation")
	}
	checkLedger(t)
	if gens, account := sharedGenerations(); len(gens) != 1 || gens[0] != b || !b.retired || account != b.states {
		t.Fatalf("after retiring the held generation: %d generations, account %d, want only it at %d", len(gens), account, b.states)
	}
	c.release()
	checkLedger(t)
	b.release()
	checkLedger(t)
	if gens, account := sharedGenerations(); len(gens) != 0 || account != 0 {
		t.Fatalf("after the last release: %d generations, account %d", len(gens), account)
	}
	d := acquirePrediction()
	if !d.shared || d == b {
		t.Fatalf("the next parse didn't get a fresh shared generation")
	}
	d.release()
	checkLedger(t)
}

// A working set within the budget keeps its one generation under continuous
// concurrent load: no parse goes cold because of the bound. Before, the
// token estimate of a generation that was never idle drained it about every
// 200 ms, and each drain handed parses to a cold generation.
func TestValidatePredictionWorkingSetKeepsGeneration(t *testing.T) {
	setMaxParserDFAStates(t, 1_000_000)
	holder := acquirePrediction()
	made := generationsMade()
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(100 + worker)))
			for i := 0; i < 40; i++ {
				if err := Validate(distinctStatement(r)); err != nil {
					errs <- err
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	holder.release()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if n := generationsMade() - made; n != 0 {
		t.Fatalf("a working set within the budget made %d new generations", n)
	}
	checkLedger(t)
}

// Generations rotate safely while other parses run (go test -race): the
// budget retires them, and so does a retirer between parses.
func TestValidatePredictionGenerationConcurrent(t *testing.T) {
	setMaxParserDFAStates(t, 400)
	made := generationsMade()
	stop := make(chan struct{})
	retirer := make(chan struct{})
	go func() {
		defer close(retirer)
		for {
			select {
			case <-stop:
				return
			case <-time.After(25 * time.Millisecond):
				retireCurrentPredictionForTest()
			}
		}
	}()
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 15; i++ {
				statement := fmt.Sprintf("MATCH (n:L%d) WHERE n.v = %d RETURN [x IN n.xs WHERE x > %d | x * 2] AS v", worker, i, i)
				if err := Validate(statement); err != nil {
					errs <- err
					return
				}
				if err := Validate("MATCH (n RETURN n"); err == nil {
					errs <- fmt.Errorf("invalid statement accepted")
					return
				}
				if _, err := Parse(statement); err != nil {
					errs <- err
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	close(stop)
	<-retirer
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if generationsMade()-made < 2 {
		t.Fatalf("generations never rotated while parses ran")
	}
	checkLedger(t)
}

// A retired generation's DFAs are freed by GC once its parses end: no pooled
// validator and no parse tree a caller keeps holds them.
func TestValidateRetiredPredictionIsCollected(t *testing.T) {
	setMaxParserDFAStates(t, 1_000_000)
	retireCurrentPredictionForTest()
	var collected atomic.Bool
	var tree *ParseResult
	func() {
		p := acquirePrediction()
		runtime.SetFinalizer(p.decisionToDFA[0], func(*antlr.DFA) { collected.Store(true) })
		p.release()
		if err := Validate("MATCH (n:Collected) RETURN n.x AS v"); err != nil {
			t.Fatal(err)
		}
		var err error
		if tree, err = Parse("MATCH (n:Collected) RETURN n.y AS v"); err != nil {
			t.Fatal(err)
		}
	}()
	retireCurrentPredictionForTest()
	deadline := time.Now().Add(2 * time.Second)
	for !collected.Load() && time.Now().Before(deadline) {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}
	if !collected.Load() {
		t.Fatalf("the retired generation's DFAs were still reachable after its parses ended")
	}
	runtime.KeepAlive(tree)
}

// The DFA state budget is a quarter of the runtime memory limit, and 2 GiB
// of states without one.
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
