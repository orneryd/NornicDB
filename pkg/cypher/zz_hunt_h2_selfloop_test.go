package cypher

// H2 variant of 10e4a18a9 "Stop self-loop traversal after the requested
// result limit" (units H2-0489 executeMatchWithRelationshipsWithPathSeeded,
// H2-0492 traverseFromNode, H2-0493 traverseGraphParallel, H2-0494
// traverseGraphSequential, H2-0497 walkEndNodeID). The fix's own assertions
// (traversal_repeated_endpoint_limit_test.go: LIMIT 1 on a repeated-endpoint
// pattern answers from one adjacency read, and returns one row; a population
// without loops returns none) applied to the sibling pattern forms: incoming,
// undirected, untyped, variable length, named relationship and path, no
// label, SKIP, WITH ... LIMIT. Oracle: rows as stated; adjacency reads at
// most the stated bound (cost).

import (
	"context"
	"testing"
)

func TestHuntH2RepeatedEndpointSiblings(t *testing.T) {
	cases, leads := 0, 0
	shapes := []struct {
		statement string
		maxCalls  int
		loopRows  int
	}{
		{"MATCH (p:P)<-[:SELF]-(p) RETURN p.id LIMIT 1", 1, 1},
		{"MATCH (p:P)-[:SELF]-(p) RETURN p.id LIMIT 1", 1, 1},
		{"MATCH (p:P)-[]->(p) RETURN p.id LIMIT 1", 1, 1},
		{"MATCH (p:P)-[:SELF*1..1]->(p) RETURN p.id LIMIT 1", 1, 1},
		{"MATCH (p:P)-[r:SELF]->(p) RETURN r LIMIT 1", 1, 1},
		{"MATCH path = (p:P)-[:SELF]->(p) RETURN length(path) LIMIT 1", 1, 1},
		{"MATCH (p)-[:SELF]->(p) RETURN p.id LIMIT 1", 1, 1},
		{"MATCH (p:P)-[:SELF]->(p) RETURN p.id SKIP 1 LIMIT 1", 2, 1},
		{"MATCH (p:P)-[:SELF]->(p) WITH p LIMIT 1 RETURN p.id", 1, 1},
		{"MATCH (p:P)-[:SELF]->(p) RETURN p.id LIMIT 3", 3, 3},
	}
	forEachParser(t, func(t *testing.T) {
		for _, loops := range []bool{true, false} {
			for _, shape := range shapes {
				name := shape.statement
				if !loops {
					name = "no-loops/" + name
				}
				t.Run(name, func(t *testing.T) {
					cases++
					spy := newRepeatedEndpointReadSpy(repeatedEndpointStore(t, loops))
					exec := NewStorageExecutor(spy, QueryCachePolicy{MaxEntries: 0, TTL: 0})
					result, err := exec.Execute(context.Background(), shape.statement, nil)
					want := shape.loopRows
					if !loops {
						want = 0
					}
					lead := map[string]any{"harness": "self-loop-limit", "unit": "H2-0497", "statement": shape.statement, "loops": loops, "population": repeatedEndpointPopulation}
					if err != nil {
						lead["kind"], lead["error"] = "error", err.Error()
						leads++
						h2Lead(lead)
						return
					}
					if len(result.Rows) != want {
						lead["kind"], lead["rows"], lead["want_rows"] = "wrong-row-count", len(result.Rows), want
						leads++
						h2Lead(lead)
						return
					}
					if loops && spy.calls > shape.maxCalls {
						lead["kind"], lead["adjacency_reads"], lead["max_reads"] = "cost", spy.calls, shape.maxCalls
						leads++
						h2Lead(lead)
					}
				})
			}
		}
	})
	h2Coverage(map[string]any{"harness": "self-loop-limit", "commit": "10e4a18a9", "cases": cases, "leads": leads})
}
