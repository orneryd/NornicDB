package server

// Scratch investigation (pd-w161-4), not for merge: Neo4j 5.26.30 reference
// for observation 3 (t_first / t_last and when records arrive), run against
// the lab's reference server (--ref neo-ref). Everything created here is
// removed at the end.

import (
	"context"
	"os"
	"testing"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

const pdw1614NeoMatch = "MATCH (n:PDW1614Item) WHERE n.k < $n RETURN n.k AS k, reduce(s = 0, x IN range(1, $w) | s + x) AS v"
const pdw1614NeoUnwind = "UNWIND range(1, $n) AS i RETURN i AS k, reduce(s = 0, x IN range(1, $w) | s + x) AS v"

func TestPDW1614Neo4jSummaryTiming(t *testing.T) {
	uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
	if uri == "" {
		t.Skip("NORNICDB_NEO4J_REFERENCE_URI is required (--ref neo-ref)")
	}
	ctx := t.Context()
	driver, err := neo4jdriver.NewDriverWithContext(uri, neo4jdriver.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	exec := func(stmt string, params map[string]any) []map[string]any {
		t.Helper()
		result, err := neo4jdriver.ExecuteQuery(ctx, driver, stmt, params, neo4jdriver.EagerResultTransformer, neo4jdriver.ExecuteQueryWithDatabase("neo4j"))
		require.NoError(t, err)
		rows := make([]map[string]any, 0, len(result.Records))
		for _, record := range result.Records {
			rows = append(rows, record.AsMap())
		}
		return rows
	}
	t.Logf("PDW1614NEO components=%v", exec("CALL dbms.components() YIELD name, versions, edition RETURN name, versions, edition", nil))
	t.Logf("PDW1614NEO apoc=%v", exec("SHOW FUNCTIONS YIELD name WHERE name STARTS WITH 'apoc.' RETURN count(name) AS apoc", nil))
	exec("MATCH (n:PDW1614Item) DETACH DELETE n", nil)
	exec("UNWIND range(0, 4999) AS i CREATE (:PDW1614Item {k: i})", nil)
	t.Cleanup(func() {
		_, _ = neo4jdriver.ExecuteQuery(context.Background(), driver, "MATCH (n:PDW1614Item) DETACH DELETE n", nil, neo4jdriver.EagerResultTransformer, neo4jdriver.ExecuteQueryWithDatabase("neo4j"))
	})

	type outcome struct {
		run, first, total     time.Duration
		records               int
		availableAfter        time.Duration
		consumedAfter         time.Duration
		err                   error
	}
	consume := func(start time.Time, result neo4jdriver.ResultWithContext, runErr error) outcome {
		var out outcome
		out.run = time.Since(start)
		if runErr != nil {
			out.err = runErr
			return out
		}
		for result.Next(ctx) {
			if out.records == 0 {
				out.first = time.Since(start)
			}
			out.records++
		}
		summary, err := result.Consume(ctx)
		out.total = time.Since(start)
		if err != nil {
			out.err = err
			return out
		}
		out.availableAfter = summary.ResultAvailableAfter()
		out.consumedAfter = summary.ResultConsumedAfter()
		return out
	}
	work := int64(400)
	for _, statement := range []struct{ name, text string }{{"match", pdw1614NeoMatch}, {"unwind", pdw1614NeoUnwind}} {
		for _, n := range []int64{10, 1000, 1001, 5000} {
			params := map[string]any{"n": n, "w": work}
			for _, fetch := range []int{-1, 1000} {
				session := driver.NewSession(ctx, neo4jdriver.SessionConfig{DatabaseName: "neo4j", FetchSize: fetch})
				for round := 0; round < 3; round++ {
					start := time.Now()
					result, runErr := session.Run(ctx, statement.text, params)
					auto := consume(start, result, runErr)
					t.Logf("PDW1614NEO %s n=%d fetch=%d round=%d mode=auto-commit Run-returned=%v first-record=%v total=%v records=%d t_first(ResultAvailableAfter)=%v t_last(ResultConsumedAfter)=%v err=%v",
						statement.name, n, fetch, round, auto.run, auto.first, auto.total, auto.records, auto.availableAfter, auto.consumedAfter, auto.err)
					tx, err := session.BeginTransaction(ctx)
					require.NoError(t, err)
					start = time.Now()
					result, runErr = tx.Run(ctx, statement.text, params)
					explicit := consume(start, result, runErr)
					commitErr := tx.Commit(ctx)
					t.Logf("PDW1614NEO %s n=%d fetch=%d round=%d mode=explicit-tx Run-returned=%v first-record=%v total=%v records=%d t_first(ResultAvailableAfter)=%v t_last(ResultConsumedAfter)=%v err=%v commit=%v",
						statement.name, n, fetch, round, explicit.run, explicit.first, explicit.total, explicit.records, explicit.availableAfter, explicit.consumedAfter, explicit.err, commitErr)
				}
				_ = session.Close(ctx)
			}
		}
	}
}
