package tck

import (
	"context"
	"testing"
)

// TestExplicitTransactionRelationshipRebindRegressions reproduces the three official
// TCK scenarios that regressed from supported-pass to expected-gap in explicit-
// transaction mode (clauses/match/Match3.feature [24], clauses/match/Match4.feature
// [7], expressions/comparison/Comparison1.feature [5]).
//
// Every case is exercised through the real TCK Bolt backend in both transaction
// modes, with the exact setup and expectation taken from the official feature files.
func TestExplicitTransactionRelationshipRebindRegressions(t *testing.T) {
	for _, mode := range []TransactionMode{AutocommitMode, ExplicitTransactionMode} {
		t.Run(string(mode), func(t *testing.T) {
			for _, testCase := range []struct {
				name       string
				feature    string
				setup      string
				query      string
				wantCount  int64
				wantResult interface{}
			}{
				{
					name:    "Match3-[24]-matching-twice-with-duplicate-relationship-types-on-same-relationship",
					feature: "clauses/match/Match3.feature",
					setup:   "CREATE (:A)-[:T]->(:B)",
					query: "MATCH (a1)-[r:T]->()\n" +
						"WITH r, a1\n" +
						"MATCH (a1)-[r:T]->(b2)\n" +
						"RETURN a1, r, b2",
					wantCount: 1,
				},
				{
					name:    "Match4-[7]-matching-variable-length-patterns-including-a-bound-relationship",
					feature: "clauses/match/Match4.feature",
					setup: "CREATE (n0:Node),\n" +
						"       (n1:Node),\n" +
						"       (n2:Node),\n" +
						"       (n3:Node),\n" +
						"       (n0)-[:EDGE]->(n1),\n" +
						"       (n1)-[:EDGE]->(n2),\n" +
						"       (n2)-[:EDGE]->(n3)",
					query: "MATCH ()-[r:EDGE]-()\n" +
						"MATCH p = (n)-[*0..1]-()-[r]-()-[*0..1]-(m)\n" +
						"RETURN count(p) AS c",
					wantCount:  1,
					wantResult: int64(32),
				},
				{
					name:    "Comparison1-[5]-comparing-relationships-to-relationships",
					feature: "expressions/comparison/Comparison1.feature",
					setup:   "CREATE ()-[:T]->()",
					query: "MATCH ()-[a]->()\n" +
						"WITH a\n" +
						"MATCH ()-[b]->()\n" +
						"WHERE a = b\n" +
						"RETURN count(b)",
					wantCount:  1,
					wantResult: int64(1),
				},
			} {
				t.Run(testCase.name, func(t *testing.T) {
					driver, shutdown := startConformanceServer(t)
					defer shutdown()
					backend, err := NewBoltBackend(BoltBackendConfig{
						Driver:       driver,
						DatabaseName: "nornic",
						Mode:         mode,
					})
					if err != nil {
						t.Fatalf("%s: create backend: %v", testCase.feature, err)
					}
					ctx := context.Background()
					if err := backend.Reset(ctx); err != nil {
						t.Fatalf("%s: reset graph: %v", testCase.feature, err)
					}
					if _, err := backend.Execute(ctx, testCase.setup, nil); err != nil {
						t.Fatalf("%s: setup %q: %v", testCase.feature, testCase.setup, err)
					}

					result, err := backend.Execute(ctx, testCase.query, nil)
					if err != nil {
						t.Fatalf("%s: query failed: %v\nquery:\n%s", testCase.feature, err, testCase.query)
					}
					if len(result.Rows) != int(testCase.wantCount) {
						t.Fatalf("%s: row count differs: got %d, want %d\nquery:\n%s\nrows: %#v",
							testCase.feature, len(result.Rows), testCase.wantCount, testCase.query, result.Rows)
					}
					if testCase.wantResult != nil {
						if len(result.Rows[0]) != 1 {
							t.Fatalf("%s: row width differs: got %d, want 1 (rows: %#v)",
								testCase.feature, len(result.Rows[0]), result.Rows)
						}
						if got := result.Rows[0][0]; got != testCase.wantResult {
							t.Fatalf("%s: value differs: got %#v, want %#v\nquery:\n%s",
								testCase.feature, got, testCase.wantResult, testCase.query)
						}
					}
				})
			}
		})
	}
}
