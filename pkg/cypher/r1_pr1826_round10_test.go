package cypher

import (
	"fmt"
	"reflect"
	"testing"
)

// Review lane R1 of PR #1826, round 10: the endpoint statements of round 9 with
// their answers written out. The executor trims an endpoint after it read the
// whole node, so a control that only widens storage reads is trimmed too.
func TestR1PR1826Round10EndpointAnswers(t *testing.T) {
	const (
		one = "{body:'body one',extra:7,key:'one',list:[1,2,3],path:'guide',status:'current'}"
		two = "{body:'body two',extra:8,key:'two',list:[4],path:'second',status:'old'}"
	)
	for _, kind := range []string{"namespaced", "managed"} {
		for _, indexed := range []bool{true, false} {
			for _, test := range []struct {
				name, query string
				want        []string
			}{
				{"plain/keyword-properties", "MATCH (start)-[:REL]->(end) RETURN properties(end) AS p, properties(start) AS q", []string{two + " | " + one}},
				{"plain/keyword-coalesce", "MATCH (start)-[:REL]->(end) RETURN coalesce(end, null) AS p", []string{"node" + two}},
				{"plain/label-again", "MATCH (a)-[:REL]->(m) MATCH (m:Doc {extra:8}) RETURN m.key", []string{"'two'"}},
				{"plain/label-again-unlabelled", "MATCH (a)-[:REL]->(m) MATCH (m {extra:8}) RETURN m.key", []string{"'two'"}},
				{"plain/label-again-where", "MATCH (a)-[:REL]->(m) MATCH (m:Doc) WHERE m.extra = 8 RETURN m.key", []string{"'two'"}},
				{"plain/not-exists", "MATCH (a)-[:REL]->(m) WHERE NOT EXISTS { (m:Doc {extra:8})-[:BACK]->() } RETURN m.key", []string{}},
				{"plain/path", "MATCH (a)-[:REL]->(m) MATCH p=(m:Doc)-[:BACK]->(x) RETURN [y IN nodes(p) | y.extra] AS l", []string{"[8,7]"}},
				{"keyed/label-again", "MATCH (a:Doc {key:'one'})-[:REL]->(m) MATCH (m:Doc {extra:8}) RETURN m.key", []string{"'two'"}},
				{"keyed/not-exists", "MATCH (a:Doc {key:'one'})-[:REL]->(m) WHERE NOT EXISTS { (m:Doc {extra:8})-[:BACK]->() } RETURN m.key", []string{}},
				{"keyed/keyword-properties", "MATCH (start:Doc {key:'one'})-[:REL]->(end) RETURN properties(end) AS p", []string{two}},
				{"keyed/keyword-coalesce", "MATCH (start:Doc {key:'one'})-[:REL]->(end) RETURN coalesce(end, null) AS p", []string{"node" + two}},
				{"subquery/keyword-properties", "CALL () { MATCH (start)-[:REL]->(end) RETURN properties(end) AS p } RETURN p", []string{two}},
				{"subquery/label-again", "CALL () { MATCH (a)-[:REL]->(m) MATCH (m:Doc {extra:8}) RETURN m.key AS k } RETURN k", []string{"'two'"}},
				{"subquery/not-exists", "CALL () { MATCH (a)-[:REL]->(m) WHERE NOT EXISTS { (m:Doc {extra:8})-[:BACK]->() } RETURN m.key AS k } RETURN k", []string{}},
				{"with/keyword-properties", "MATCH (start)-[:REL]->(end) WITH start, end RETURN properties(end) AS p", []string{two}},
				{"with/label-again", "MATCH (a)-[:REL]->(m) WITH a, m MATCH (m:Doc {extra:8}) RETURN m.key", []string{"'two'"}},
				{"with/not-exists", "MATCH (a)-[:REL]->(m) WITH a, m WHERE NOT EXISTS { (m:Doc {extra:8})-[:BACK]->() } RETURN m.key", []string{}},
				{"unwind/label-again", "UNWIND [1] AS i MATCH (a)-[:REL]->(m) MATCH (m:Doc {extra:8}) RETURN m.key", []string{"'two'"}},
				{"second-match/label-again", "MATCH (o:Other) MATCH (a)-[:REL]->(m) MATCH (m:Doc {extra:8}) RETURN m.key", []string{"'two'"}},
				{"labelled/label-again", "MATCH (a:Doc)-[:REL]->(m:Doc) WITH a, m MATCH (m:Doc {extra:8}) RETURN m.key", []string{"'two'"}},
				{"aggregate/label-again", "MATCH (a)-[:REL]->(m) WITH m, count(a) AS c MATCH (m:Doc {extra:8}) RETURN m.key, c", []string{"'two' | 1"}},
				{"collect/keyword", "MATCH (start)-[:REL]->(end) WITH collect(coalesce(end, null)) AS ends RETURN [e IN ends | e.extra] AS l", []string{"[8]"}},
			} {
				t.Run(fmt.Sprintf("%s/indexed=%t/%s", kind, indexed, test.name), func(t *testing.T) {
					got := r1Rows(NewStorageExecutor(r1Round4Store(t, kind, indexed), QueryCachePolicy{MaxEntries: 0}).Execute(t.Context(), test.query, nil))
					if !reflect.DeepEqual(test.want, got) {
						t.Errorf("%s\n  expected: %q\n  answer:   %q", test.query, test.want, got)
					}
				})
			}
		}
	}
}
