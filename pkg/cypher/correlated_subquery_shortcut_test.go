package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// A correlated EXISTS / COUNT / COLLECT body never takes the statement
// shortcuts (label count, relationship count, bounded projection), which
// read the store as if the imported variable were unbound (#1317 S01, S02b,
// S03). Uncorrelated bodies keep them. Expected values are Neo4j 5.26.30's.
func TestCorrelatedSubqueryBodySkipsStatementShortcuts(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	if _, err := exec.Execute(ctx, "CREATE (:A {n:1})-[:R]->(:B), (:A {n:2})-[:R]->(:B), (:A {n:3})", nil); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name  string
		query string
		want  []string
	}{
		{"S01 COLLECT label count over imported node",
			"MATCH (a:A) RETURN a.n AS n, COLLECT { MATCH (a:A) RETURN count(a) AS c } AS cs ORDER BY n",
			[]string{"1 [1]", "2 [1]", "3 [1]"}},
		{"COUNT label pattern over imported node",
			"MATCH (a:A) RETURN a.n AS n, COUNT { MATCH (a:A) } AS c ORDER BY n",
			[]string{"1 1", "2 1", "3 1"}},
		{"S02b COLLECT relationship count from imported node",
			"MATCH (a:A) WITH a, COLLECT { MATCH (a)-[r:R]->() RETURN count(r) AS c } AS l RETURN a.n AS n, l[0] AS d ORDER BY n",
			[]string{"1 1", "2 1", "3 0"}},
		{"COUNT relationship pattern from imported node",
			"MATCH (a:A) RETURN a.n AS n, COUNT { MATCH (a)-[r:R]->() } AS c ORDER BY n",
			[]string{"1 1", "2 1", "3 0"}},
		{"S03 COLLECT bounded projection of imported node",
			"MATCH (a:A) RETURN a.n AS n, COLLECT { MATCH (a) RETURN a.n AS x LIMIT 5 } AS xs ORDER BY n",
			[]string{"1 [1]", "2 [2]", "3 [3]"}},
		{"COUNT filtered by an imported value in WHERE",
			"MATCH (a:A) RETURN a.n AS n, COUNT { MATCH (b:A) WHERE b.n > a.n } AS c ORDER BY n",
			[]string{"1 2", "2 1", "3 0"}},
		{"COUNT filtered by an imported value in a property map",
			"MATCH (a:A) RETURN a.n AS n, COUNT { MATCH (b:A {n: a.n}) } AS c ORDER BY n",
			[]string{"1 1", "2 1", "3 1"}},
		{"EXISTS relationship from imported node",
			"MATCH (a:A) RETURN a.n AS n, EXISTS { MATCH (a)-[:R]->() } AS e ORDER BY n",
			[]string{"1 true", "2 true", "3 false"}},
		{"uncorrelated COUNT label pattern",
			"MATCH (a:A) RETURN a.n AS n, COUNT { MATCH (b:A) } AS c ORDER BY n",
			[]string{"1 3", "2 3", "3 3"}},
		{"uncorrelated COUNT relationship pattern",
			"MATCH (a:A) RETURN a.n AS n, COUNT { MATCH ()-[r:R]->() } AS c ORDER BY n",
			[]string{"1 2", "2 2", "3 2"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, tc.query, nil)
			if err != nil {
				t.Fatalf("%s: %v", tc.query, err)
			}
			got := make([]string, 0, len(result.Rows))
			for _, row := range result.Rows {
				got = append(got, fmt.Sprintf("%v %v", row[0], row[1]))
			}
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Fatalf("%s:\n got %v\nwant %v", tc.query, got, tc.want)
			}
		})
	}
}
