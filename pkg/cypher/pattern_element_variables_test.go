package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

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
			exec.StatementFacts(tc.query, resultmetadata.KnownScope(nil), true, RawSource).Visit(func(statement resultmetadata.Statement) {
				statement.Columns.Visit(func(names []string) { require.Equal(t, tc.columns, names) }, func() { t.Fatal("known statement has unknown columns") })
			}, func() { t.Fatal("owner declined") }, func(reason resultmetadata.Unresolved) { t.Fatal(reason.Reason, reason.Position, reason.Err) })
		})
	}
}
