package cypher

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// TestUnscopedCallImportsMatchNeo4j: what a CALL body reads from the outer
// row (bodyScopeOf) gives Neo4j 5.26.30's answer on every route. Each case
// of testdata/call_imports/cases.tsv is a body, whether it reads n (y / n),
// the outer query and the key it orders by; expected.tsv holds Neo4j's
// result for the body as CALL (n) { … } when it reads n and as CALL () { … }
// when it doesn't (a wrong label makes Neo4j reject CALL ()). NornicDB
// runs the body unscoped (its implicit import, a NornicDB extension Neo4j
// rejects), scoped the same way, and as CALL { … } IN TRANSACTIONS, in
// auto-commit and in an explicit transaction. Cases: names a nested
// EXISTS / COUNT / COLLECT, a nested UNION branch, a nested CALL or a
// pattern comprehension declares or reads; property keys, map keys,
// strings, comments, labels and types named like the variable;
// comprehension, reduce and quantifier locals; backticked names.
func TestUnscopedCallImportsMatchNeo4j(t *testing.T) {
	read := func(name string) [][]string {
		data, err := os.ReadFile("testdata/call_imports/" + name)
		if err != nil {
			t.Fatal(err)
		}
		var rows [][]string
		for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
			rows = append(rows, strings.Split(line, "\t"))
		}
		return rows
	}
	expected := map[string]string{}
	for _, row := range read("expected.tsv") {
		expected[row[0]] = row[1]
	}
	for _, route := range []string{"auto", "explicit"} {
		ctx := context.Background()
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "imports"+route))
		if _, err := exec.Execute(ctx, "CREATE (:A {id:1}), (:A {id:2}), (:B)-[:R]->(:C {id:9})", nil); err != nil {
			t.Fatal(err)
		}
		for _, c := range read("cases.tsv") {
			name, reads, outer, key, body := c[0], c[1], c[2], c[3], c[4]
			scope := "()"
			if reads == "y" {
				scope = "(n)"
			}
			forms := map[string]string{"unscoped": "CALL { " + body + " }", "scoped": "CALL " + scope + " { " + body + " }"}
			if route == "auto" {
				forms["in transactions"] = "CALL { " + body + " } IN TRANSACTIONS"
			}
			for form, call := range forms {
				query := fmt.Sprintf("%s %s WITH %s AS k, c ORDER BY k RETURN collect(toString(k) + ':' + coalesce(toString(c), 'null')) AS r", outer, call, key)
				if route == "explicit" {
					if _, err := exec.Execute(ctx, "BEGIN", nil); err != nil {
						t.Fatal(err)
					}
				}
				result, err := exec.Execute(ctx, query, nil)
				if route == "explicit" {
					_, _ = exec.Execute(ctx, "ROLLBACK", nil)
				}
				var got string
				switch {
				case err != nil:
					got = "ERROR"
					if expected[name] != "ERROR" {
						got += " " + err.Error()
					}
				case len(result.Rows) == 1:
					items, _ := result.Rows[0][0].([]interface{})
					quoted := make([]string, len(items))
					for i, item := range items {
						quoted[i] = fmt.Sprintf("%q", item)
					}
					got = "[" + strings.Join(quoted, ", ") + "]"
				default:
					got = fmt.Sprintf("rows %v", result.Rows)
				}
				if got != expected[name] {
					t.Errorf("%s %s %s: got %s, want %s\n%s", name, route, form, got, expected[name], query)
				}
			}
		}
	}
}
