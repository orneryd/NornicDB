package antlr

// Closeout lane L3, item P-N15-137 (Plane N15-137 / NOR-368): a -race finding
// in the ANTLR parser's shared prediction state (prediction.go acquire /
// release / countStates, reached through ParseSyntax). Run with -race:
//   go test ./pkg/cypher/antlr -race -run '^TestCloseoutL3_N15_137_' -count=1
// The race detector fails the test if parallel parses of ordinary statements
// race; the token-count fingerprints also check that every parse gives the
// same product as a sequential parse of the same shape.

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

var closeoutL3Shapes = []string{
	"MATCH (n:Person {name: 'p%d'}) RETURN n.name AS name, n.age + %d AS age",
	"MATCH (a:A)-[r:R]->(b:B) WHERE a.v > %d AND b.w < %d RETURN a, r, b ORDER BY a.v LIMIT 10",
	"UNWIND range(0, %d) AS i WITH i WHERE i %% 2 = %d RETURN collect(i) AS xs",
	"MERGE (n:Item {id: %d}) ON CREATE SET n.created = %d ON MATCH SET n.seen = true RETURN n",
	"MATCH (n) WHERE n.v IN [%d, %d] RETURN [x IN n.xs WHERE x > 0 | x * 2] AS v",
	"CALL { MATCH (n:C) WHERE n.v = %d RETURN count(n) AS c } RETURN c + %d AS total",
	"MATCH p = (a)-[*1..3]->(b) WHERE id(a) = %d RETURN length(p) AS l, %d AS k",
	"CREATE (n:New {a: %d, b: %d}) RETURN n",
}

func closeoutL3Statement(shape, a, b int) string {
	return fmt.Sprintf(closeoutL3Shapes[shape], a, b)
}

func closeoutL3Baseline(t *testing.T) []int {
	t.Helper()
	counts := make([]int, len(closeoutL3Shapes))
	for shape := range closeoutL3Shapes {
		syntax, err := ParseSyntax(context.Background(), closeoutL3Statement(shape, 0, 1), StatementEntry, false, AcceptedSource)
		if err != nil {
			t.Fatalf("control parse of shape %d: %v", shape, err)
		}
		counts[shape] = syntax.TokenCount()
	}
	return counts
}

func closeoutL3ParallelParses(t *testing.T, workers, iterations int) {
	t.Helper()
	baseline := closeoutL3Baseline(t)
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				shape := (worker + i) % len(closeoutL3Shapes)
				// Distinct literals per parse: no statement-syntax cache hit.
				statement := closeoutL3Statement(shape, 1000+worker*iterations+i, 7)
				syntax, err := ParseSyntax(context.Background(), statement, StatementEntry, false, AcceptedSource)
				if err != nil {
					errs <- fmt.Errorf("worker %d parse %q: %v", worker, statement, err)
					return
				}
				if got := syntax.TokenCount(); got != baseline[shape] {
					errs <- fmt.Errorf("worker %d shape %d: %d tokens, sequential parse has %d", worker, shape, got, baseline[shape])
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	t.Logf("CLOSEOUT_L3_PREDICTION workers=%d iterations=%d generations_made=%d", workers, iterations, generationsMade())
}

// Control: the same parses one after another.
func TestCloseoutL3_N15_137_SequentialControl(t *testing.T) {
	closeoutL3ParallelParses(t, 1, 64)
}

// Default prediction budget: parallel parses share one generation.
func TestCloseoutL3_N15_137_ParallelOrdinaryParses(t *testing.T) {
	retireCurrentPredictionForTest()
	closeoutL3ParallelParses(t, 16, 40)
}

// Small budget: generations rotate (retire / count / release) while parses run.
func TestCloseoutL3_N15_137_ParallelParsesAcrossRotation(t *testing.T) {
	setMaxParserDFAStates(t, 300)
	retireCurrentPredictionForTest()
	before := generationsMade()
	closeoutL3ParallelParses(t, 16, 40)
	t.Logf("CLOSEOUT_L3_PREDICTION rotation generations=%d", generationsMade()-before)
	checkLedger(t)
}
