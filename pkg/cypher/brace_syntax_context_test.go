package cypher

import (
	"strings"
	"testing"
)

func TestSubqueryBraceContextAtConsumerBoundary(t *testing.T) {
	for _, test := range []struct {
		query    string
		subquery bool
	}{
		{"RETURN {n: COUNT { UNWIND [2,1] AS x ORDER BY x LIMIT 1 RETURN x }} AS v", true},
		{"RETURN {n: COUNT /* gap */ { (n)-->(m) }} AS v", true},
		{"MATCH (n) WHERE EXISTS { p = (n)-->(m) } RETURN n", true},
		{"CALL /* before */ () /* after */ { RETURN 1 } RETURN 1", true},
		{"WITH {a:1} AS count RETURN count { .a } AS out", false},
	} {
		brace := strings.LastIndex(test.query, "{")
		for start := 0; start <= brace; start++ {
			// Exercise every boundary the canonical reader produces, including
			// the opening map's interior where container consumers begin.
			if start > 0 && test.query[start-1] != ' ' && test.query[start-1] != '{' && test.query[start-1] != ':' {
				continue
			}
			scanner := newSubqueryBraceScanner(test.query, start)
			if actual := scanner.at(brace); actual != test.subquery {
				t.Fatalf("boundary=%d brace=%d subquery=%v want=%v query=%s", start, brace, actual, test.subquery, test.query)
			}
		}
	}
}
