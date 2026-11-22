package server

// Close-out lane L8, round 3 (2026-11-22): Neo4j 5.26.30 reference for the
// exact L-H4-014 (graph G3) and L-H4-015 (graph G2) statements and their
// narrowed variants. The reference is shared, so each graph is built inside
// one explicit transaction that is rolled back: nothing is committed, and the
// statements see only the transaction's own graph plus whatever committed
// :A/:B/:C nodes other users hold (counted first and logged). The same
// statements run on NornicDB in a fresh server. Test-only, not for merge.

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/testing/cypher/differential"
)

var l8R3Statements = map[string][]string{
	"G3": {
		"MATCH (n:C)-[*1..2]-(m) WHERE NOT (abs(toInteger(true)) = m.x) OPTIONAL MATCH (n)<-[q:R]-(o:C) RETURN n.id AS e0, substring(toString(m.x), 1) AS e1, m.id - toInteger(o.id) % abs(toInteger(coalesce(false, n.w))) AS e2 ORDER BY e0, e1, e2 SKIP 3 LIMIT 2",
		"MATCH (n:C)-[*1..2]-(m) WHERE NOT (abs(toInteger(true)) = m.x) OPTIONAL MATCH (n)<-[q:R]-(o:C) RETURN count(*) AS rows, count(o) AS withO",
		"MATCH (n:C)-[*1..2]-(m) WHERE NOT (abs(toInteger(true)) = m.x) OPTIONAL MATCH (n)<-[q:R]-(o:C) WITH n, m, o WHERE o IS NOT NULL RETURN toInteger(o.id) % abs(toInteger(coalesce(false, n.w))) AS v LIMIT 3",
		"MATCH (n:C)<-[q:R]-(o:C) RETURN toInteger(o.id) % abs(toInteger(coalesce(false, n.w))) AS v LIMIT 3",
		"MATCH (n:C)<-[q:R]-(o:C) RETURN toInteger(o.id) % 0 AS v LIMIT 3",
	},
	"G2": {
		"MATCH (n)-[r:S]->(m:B) WHERE (m.w = 'a') AND (n.z IS NULL <> n.x + true) RETURN n.x AS e0, '' AS e1",
		"MATCH (n)-[r:S]->(m:B) RETURN count(*) AS rows, count(m.w) AS withW, sum(CASE WHEN m.w = 'a' THEN 1 ELSE 0 END) AS isA",
		"MATCH (n)-[r:S]->(m:B) WITH n, m WHERE (m.w = 'a') AND (n.z IS NULL <> n.x + true) RETURN n.x AS e0",
		"MATCH (n)-[r:S]->(m:B) WHERE (n.z IS NULL <> n.x + true) RETURN count(*) AS c",
	},
}

var l8R3SetupFree = []string{
	"RETURN 22 % 0 AS v",
	"RETURN 22 % abs(toInteger(false)) AS v",
	"RETURN toInteger(false) AS v",
	"WITH 0.5 AS w RETURN 22 % abs(toInteger(coalesce(false, w))) AS v",
	"WITH null AS w RETURN 22 % abs(toInteger(coalesce(false, w))) AS v",
	"UNWIND [{x: 1}] AS m WITH m WHERE m.w = 'a' AND (m.z IS NULL <> m.x + true) RETURN m.x AS e0",
	"UNWIND [{x: 1}] AS m WITH m WHERE (m.w = 'a') AND (m.x + true > 0) RETURN m.x AS e0",
	"RETURN null AND (1 + true > 0) AS v",
	"RETURN false AND (1 + true > 0) AS v",
}

func l8R3Tx(ctx context.Context, driver neo4jdriver.DriverWithContext, database string, setup, statements []string) ([]differential.Outcome, string) {
	session := driver.NewSession(ctx, neo4jdriver.SessionConfig{DatabaseName: database})
	defer session.Close(ctx)
	tx, err := session.BeginTransaction(ctx)
	if err != nil {
		return nil, "begin: " + err.Error()
	}
	defer tx.Rollback(ctx)
	note := ""
	if result, err := tx.Run(ctx, "MATCH (n) WHERE n:A OR n:B OR n:C RETURN count(n) AS c", nil); err == nil {
		if record, err := result.Single(ctx); err == nil {
			note = fmt.Sprintf("committed A/B/C nodes before setup: %v", record.Values[0])
		}
	}
	for _, q := range setup {
		result, err := tx.Run(ctx, q, nil)
		if err == nil {
			_, err = result.Consume(ctx)
		}
		if err != nil {
			return nil, note + "; setup failed: " + err.Error()
		}
	}
	outcomes := []differential.Outcome{}
	for _, q := range statements {
		// A failing statement ends the transaction on both servers: each
		// statement after the first error runs in a fresh transaction below.
		result, err := tx.Run(ctx, q, nil)
		var outcome differential.Outcome
		if err == nil {
			var records []*neo4jdriver.Record
			records, err = result.Collect(ctx)
			if err == nil {
				for _, record := range records {
					row := []any{}
					for _, v := range record.Values {
						row = append(row, huntBoltValue(v))
					}
					outcome.Rows = append(outcome.Rows, row)
				}
			}
		}
		if err != nil {
			outcome = huntErrorOutcome(err)
			outcomes = append(outcomes, outcome)
			return outcomes, note
		}
		outcomes = append(outcomes, outcome)
	}
	return outcomes, note
}

func TestL8ReferenceR3(t *testing.T) {
	uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
	if uri == "" {
		t.Skip("requires --ref neo-ref")
	}
	ref, err := neo4jdriver.NewDriverWithContext(uri, neo4jdriver.NoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer ref.Close(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	for i := 0; i < 60; i++ {
		if err = ref.VerifyConnectivity(ctx); err == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		t.Fatalf("reference: %v", err)
	}
	setups := map[string][]string{"G2": l8RefSetupG2, "G3": l8RefSetupG3}
	l8ForParsers(t, func(t *testing.T) {
		h := l8Server(t)
		defer h.Close(t)
		for graph, statements := range l8R3Statements {
			// One statement per transaction, so an error in one does not hide the next.
			for _, q := range statements {
				a, note := l8R3Tx(ctx, ref, "neo4j", setups[graph], []string{q})
				b, _ := l8R3Tx(ctx, h.driver, "nornic", setups[graph], []string{q})
				l8Log(t, "L8_REF3", map[string]any{"graph": graph, "query": q, "neo4j": l8R3Show(a), "nornic": l8R3Show(b), "note": note})
			}
		}
		for _, q := range l8R3SetupFree {
			rctx, rcancel := context.WithTimeout(ctx, time.Minute)
			a := huntBoltRun(rctx, ref, "neo4j", false, q, nil)
			rcancel()
			b := h.Run(routeBoltAuto, q, nil, time.Minute)
			l8Log(t, "L8_REF3", map[string]any{"graph": "", "query": q, "neo4j": l8R3Show([]differential.Outcome{a}), "nornic": l8R3Show([]differential.Outcome{b})})
		}
	})
}

func l8R3Show(outcomes []differential.Outcome) string {
	if len(outcomes) == 0 {
		return "(no outcome)"
	}
	o := outcomes[len(outcomes)-1]
	if o.Code != "" {
		return o.Code + ": " + l8Cut(o.Message)
	}
	return fmt.Sprint(o.Rows)
}
