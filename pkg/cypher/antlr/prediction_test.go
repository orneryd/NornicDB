package antlr

import (
	"fmt"
	"sync"
	"testing"
)

func setMaxParserDFAStates(t *testing.T, n int) {
	t.Helper()
	previous := maxParserDFAStates
	maxParserDFAStates = n
	t.Cleanup(func() { maxParserDFAStates = previous })
}

// The prediction DFAs are bounded: past maxParserDFAStates a new generation
// starts, and verdicts are the same before and after the reset.
func TestValidatePredictionGenerationRotates(t *testing.T) {
	setMaxParserDFAStates(t, 1)
	statements := map[string]bool{
		"MATCH (n:Person) WHERE n.age > 3 RETURN n.name AS name":                               true,
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
