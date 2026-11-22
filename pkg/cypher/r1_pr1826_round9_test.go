package cypher

import (
	"fmt"
	"reflect"
	"testing"
)

// Review lane R1 of PR #1826, round 9: relationship endpoints in statements that
// run through the streaming traversal source. Answers are checked against the
// whole-node control of the strict round.
func TestR1PR1826Round9EndpointReads(t *testing.T) {
	cases := []r1Case{
		{"subquery/keyword-properties", "CALL () { MATCH (start)-[:REL]->(end) RETURN properties(end) AS p } RETURN p"},
		{"subquery/keyword-coalesce", "CALL () { MATCH (start)-[:REL]->(end) RETURN coalesce(end, null) AS p } RETURN p"},
		{"subquery/label-again", "CALL () { MATCH (a)-[:REL]->(m) MATCH (m:Doc {extra:8}) RETURN m.key AS k } RETURN k"},
		{"subquery/not-exists", "CALL () { MATCH (a)-[:REL]->(m) WHERE NOT EXISTS { (m:Doc {extra:8})-[:BACK]->() } RETURN m.key AS k } RETURN k"},
		{"subquery/path", "CALL () { MATCH (a)-[:REL]->(m) MATCH p=(m:Doc)-[:BACK]->(x) RETURN [y IN nodes(p) | y.extra] AS l } RETURN l"},
		{"with/keyword-properties", "MATCH (start)-[:REL]->(end) WITH start, end RETURN properties(end) AS p"},
		{"with/keyword-coalesce", "MATCH (start)-[:REL]->(end) WITH start, end RETURN coalesce(end, null) AS p"},
		{"with/label-again", "MATCH (a)-[:REL]->(m) WITH a, m MATCH (m:Doc {extra:8}) RETURN m.key"},
		{"with/not-exists", "MATCH (a)-[:REL]->(m) WITH a, m WHERE NOT EXISTS { (m:Doc {extra:8})-[:BACK]->() } RETURN m.key"},
		{"unwind/keyword-properties", "UNWIND [1] AS i MATCH (start)-[:REL]->(end) RETURN properties(end) AS p"},
		{"unwind/label-again", "UNWIND [1] AS i MATCH (a)-[:REL]->(m) MATCH (m:Doc {extra:8}) RETURN m.key"},
		{"second-match/label-again", "MATCH (o:Other) MATCH (a)-[:REL]->(m) MATCH (m:Doc {extra:8}) RETURN m.key"},
		{"second-match/keyword-coalesce", "MATCH (o:Other) MATCH (start)-[:REL]->(end) RETURN coalesce(end, null) AS p"},
		{"labelled/label-again", "MATCH (a:Doc)-[:REL]->(m:Doc) WITH a, m MATCH (m:Doc {extra:8}) RETURN m.key"},
		{"labelled/start-label-again", "MATCH (a:Doc)-[:REL]->(m:Doc) WITH a, m MATCH (a:Doc {extra:7}) RETURN m.key"},
		{"aggregate/label-again", "MATCH (a)-[:REL]->(m) WITH m, count(a) AS c MATCH (m:Doc {extra:8}) RETURN m.key, c"},
		{"collect/keyword", "MATCH (start)-[:REL]->(end) WITH collect(coalesce(end, null)) AS ends RETURN [e IN ends | e.extra] AS l"},
	}
	for _, kind := range []string{"namespaced", "managed"} {
		for _, indexed := range []bool{true, false} {
			for _, test := range cases {
				t.Run(fmt.Sprintf("%s/indexed=%t/%s", kind, indexed, test.name), func(t *testing.T) {
					ctx := t.Context()
					want := r1Rows(r1StrictControl(r1Round4Store(t, kind, indexed), r1Round4Name(kind)).Execute(ctx, test.query, r1Params()))
					exec := NewStorageExecutor(r1Round4Store(t, kind, indexed), QueryCachePolicy{MaxEntries: 0})
					got := r1Rows(exec.Execute(ctx, test.query, r1Params()))
					t.Logf("R1ENDPOINT %s want %q got %q", test.name, want, got)
					if !reflect.DeepEqual(want, got) {
						t.Errorf("%s\n  whole-node answer: %q\n  answer:            %q", test.query, want, got)
					}
				})
			}
		}
	}
}
