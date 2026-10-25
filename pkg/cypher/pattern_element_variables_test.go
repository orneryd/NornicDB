package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The pattern reader declares the symbolic name each node element opens with.
// Parentheses nested in the element (a dynamic label's $any(...), a call in
// its property map or element WHERE) and quoted text don't end it; a call,
// a parenthesized or quantified path, and a path selector are not elements.
func TestExtractVariableNamesFromPatternElements(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "pattern-elements"))
	for _, tc := range []struct {
		pattern string
		want    []string
	}{
		{"(p:Person)<-[:REL]-(poc:POC)-[:BELONGS_TO]->(a:Area)", []string{"p", "poc", "a"}},
		{"(n:$any(ls))", []string{"n"}},
		{"(n:$all(['A', 'B']))-[r]->(m:$any($labels))", []string{"n", "m"}},
		{"(n {name: ')'})", []string{"n"}},
		{"(n {v: toUpper('x')})-[:R]->(m)", []string{"n", "m"}},
		{"p = shortestPath((a)-[*]-(b))", []string{"a", "b"}},
		{"(a)-[r WHERE r.w IN coalesce($ws, [])]->(b)", []string{"a", "b"}},
		{"(:Label)", nil},
		{"(a)((x)-[r:T]->(y)){1,3}(b)", []string{"a", "x", "y", "b"}},
		{"((a)-[r]->(b) WHERE a.v > coalesce(z, 1))", []string{"a", "b"}},
		{"p = SHORTEST 1 (a)(()-[r:R]-()){1,6}(t:G)", []string{"a", "t"}},
		{"p = ANY SHORTEST (a:G {id: 'a'})(()-[r:R]-()){1,6}(t:G)", []string{"a", "t"}},
		{"(n WHERE size($xs) > 0)", []string{"n"}},
		{"(a WHERE a.id = 1)-[r:R]->(b)", []string{"a", "b"}},
		{"(`my node`:L)", []string{"`my node`"}},
	} {
		require.Equal(t, tc.want, exec.extractVariableNamesFromPattern(t.Context(), tc.pattern), tc.pattern)
	}
}

// The statement owner binds a MATCH element's variable even when the element
// holds nested parentheses, so the projections after it read a declared name.
func TestStatementFactsBindNestedPatternElements(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "pattern-element-facts"))
	for _, tc := range []struct {
		query   string
		columns []string
	}{
		{"WITH $labels AS ls MATCH (n:$any(ls)) RETURN n.id AS id", []string{"id"}},
		{"WITH ['A'] AS ls MATCH (n:$all(ls))-[r]->(m:$any(ls)) RETURN n, r, m", []string{"n", "r", "m"}},
		{"MATCH (n {name: ')'}) RETURN n.name AS name", []string{"name"}},
		{"MATCH (n {v: toUpper('x')}) RETURN n", []string{"n"}},
		{"MATCH (a)((x)-[r:T]->(y)){1,3}(b) RETURN a, b", []string{"a", "b"}},
		{"MATCH (n WHERE size($xs) > 0) RETURN n", []string{"n"}},
	} {
		t.Run(tc.query, func(t *testing.T) {
			exec.StatementFacts(tc.query, resultmetadata.KnownScope(nil), true).Visit(func(statement resultmetadata.Statement) {
				statement.Columns.Visit(func(names []string) { require.Equal(t, tc.columns, names) }, func() { t.Fatal("known statement has unknown columns") })
			}, func() { t.Fatal("owner declined") }, func(reason resultmetadata.Unresolved) { t.Fatal(reason.Reason, reason.Position, reason.Err) })
		})
	}
}
