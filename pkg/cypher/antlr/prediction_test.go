package antlr

import (
	"fmt"
	"math/rand"
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

// Under continuous concurrency some parse always holds the current
// generation, so it is never idle for a size check. The bound must hold
// anyway: busy generations are drained and measured, and every generation
// stays near maxParserDFAStates while the workload builds far more states.
func TestValidatePredictionBoundedUnderContinuousLoad(t *testing.T) {
	const limit = 1000
	count, total, largest := runContinuousPredictionLoad(t, limit, 4)
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

// runContinuousPredictionLoad validates distinct statements from 8 workers
// while two staggered holders keep a parse open on the current generation
// almost all the time, as long parses under load do. It returns how many
// generations the parses used, their total DFA states and the largest one.
func runContinuousPredictionLoad(t *testing.T, limit int, busyChecks uint32) (count, total, largest int) {
	t.Helper()
	setMaxParserDFAStates(t, limit)
	previousBusy := maxBusyPredictionChecks
	maxBusyPredictionChecks = busyChecks
	t.Cleanup(func() { maxBusyPredictionChecks = previousBusy })
	t.Cleanup(predictionDrains.Wait)

	var generations sync.Map
	record := func() { generations.Store(currentPrediction.Load(), true) }

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

	const workers, perWorker = 8, 40
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(worker + 1)))
			for i := 0; i < perWorker; i++ {
				var b strings.Builder
				b.WriteString("MATCH (n) WITH n, 1 AS x")
				for j := 0; j < 3; j++ {
					fmt.Fprintf(&b, ", %s AS v%d", distinctExpression(r, 2+r.Intn(2)), j)
				}
				b.WriteString(" RETURN v0, v1, v2")
				if err := Validate(b.String()); err != nil {
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
	predictionDrains.Wait()
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
	t.Logf("busyChecks=%d generations=%d total states=%d largest=%d limit=%d", busyChecks, count, total, largest, limit)
	return count, total, largest
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
	first.release()
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
