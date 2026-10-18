package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// An unscoped CALL body implicitly imports the outer variables it reads (a
// NornicDB extension), but a name a nested EXISTS / COUNT / COLLECT declares
// is a new variable of that subquery, as in Neo4j, so it is not imported
// (#1317 S08); a name a nested subquery only reads is still imported.
// Expected values are Neo4j 5.26.30's, except the "extension" cases, which
// pin the extension (Neo4j rejects the outer reference).
func TestUnscopedCallImportsIgnoreNestedSubqueryNames(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	if _, err := exec.Execute(ctx, "CREATE (:A {id:1}), (:A {id:2}), (:B)-[:R]->(:C)", nil); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name  string
		query string
		want  []string
	}{
		{"S08 name only in a nested EXISTS",
			"MATCH (n:A) CALL { MATCH (m:B) WHERE EXISTS { (m)-->(n) } RETURN count(m) AS c } RETURN n.id AS id, c ORDER BY id",
			[]string{"1 1", "2 1"}},
		{"name only in a nested COUNT",
			"MATCH (n:A) CALL { MATCH (m:B) RETURN COUNT { (m)-->(n) } AS c } RETURN n.id AS id, c ORDER BY id",
			[]string{"1 1", "2 1"}},
		{"name only in a nested COLLECT",
			"MATCH (n:A) CALL { MATCH (m:B) RETURN COLLECT { MATCH (m)-->(n) RETURN n.id } AS c } RETURN n.id AS id, size(c) AS c ORDER BY id",
			[]string{"1 1", "2 1"}},
		{"scoped CALL imports the name for its nested EXISTS",
			"MATCH (n:A) CALL (n) { MATCH (m:B) WHERE EXISTS { (m)-->(n) } RETURN count(m) AS c } RETURN n.id AS id, c ORDER BY id",
			[]string{"1 0", "2 0"}},
		{"name a doubly nested COUNT declares",
			"MATCH (n:A) CALL { MATCH (m:B) RETURN COLLECT { MATCH (m)-->(x) RETURN COUNT { (x)<--(n) } } AS c } RETURN n.id AS id, c[0] AS c ORDER BY id",
			[]string{"1 1", "2 1"}},
		{"extension: a plain reference in the body still imports",
			"MATCH (n:A) CALL { RETURN n.id AS x } RETURN n.id AS id, x ORDER BY id",
			[]string{"1 1", "2 2"}},
		{"extension: a nested COLLECT that only reads the name imports it",
			"MATCH (n:A) CALL { RETURN COLLECT { RETURN n.id } AS xs } RETURN n.id AS id, xs[0] AS x ORDER BY id",
			[]string{"1 1", "2 2"}},
		{"extension: a nested EXISTS whose WHERE reads the name imports it",
			"MATCH (n:A) CALL { MATCH (m:A) WHERE EXISTS { MATCH (m) WHERE m.id = n.id } RETURN count(m) AS c } RETURN n.id AS id, c ORDER BY id",
			[]string{"1 1", "2 1"}},
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
