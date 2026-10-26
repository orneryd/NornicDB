package cypher

import (
	"context"
	"reflect"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestMapBraceSyntaxBeforeExecution(t *testing.T) {
	invalid := []string{
		"UNWIND [] AS a RETURN {a} AS out",
		"UNWIND [] AS a RETURN {.a} AS out",
		"UNWIND [] AS a RETURN {.*} AS out",
		"UNWIND [] AS a RETURN {k:[{a}]} AS out",
		"CALL { UNWIND [] AS a RETURN {a} AS out } RETURN out",
		"UNWIND [] AS a RETURN {a} AS out UNION RETURN 1 AS out",
		"RETURN 1 AS out UNION UNWIND [] AS a RETURN {a} AS out",
		"EXPLAIN WITH 7 AS a RETURN {a} AS out",
		"EXPLAIN WITH 7 AS a RETURN {k:[{a}]} AS out",
		"WITH 7 AS a RETURN CASE WHEN true THEN {a} ELSE {} END AS out",
	}
	valid := []struct {
		query string
		want  interface{}
	}{
		{"RETURN {a$b:1} AS out", map[string]interface{}{"a$b": int64(1)}},
		{"RETURN {`a$b`:1} AS out", map[string]interface{}{"a$b": int64(1)}},
		{"WITH {`a$b`:1} AS n RETURN n{.a$b} AS out", map[string]interface{}{"a$b": int64(1)}},
		{"WITH {} AS n,2 AS a$b RETURN n{a$b} AS out", map[string]interface{}{"a$b": int64(2)}},
		{"WITH {a:1} AS n,2 AS b RETURN n{.a,b,c:3} AS out", map[string]interface{}{"a": int64(1), "b": int64(2), "c": int64(3)}},
		{"WITH {a:1} AS n,2 AS b RETURN n{.a,b} AS out", map[string]interface{}{"a": int64(1), "b": int64(2)}},
		{"WITH {a:1} AS n RETURN n{.*} AS out", map[string]interface{}{"a": int64(1)}},
	}
	for _, explicit := range []bool{false, true} {
		for _, query := range invalid {
			t.Run(query, func(t *testing.T) {
				executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "brace_syntax"))
				if explicit {
					if _, err := executor.Execute(context.Background(), "BEGIN", nil); err != nil {
						t.Fatal(err)
					}
					defer executor.Execute(context.Background(), "ROLLBACK", nil)
				}
				_, err := executor.Execute(context.Background(), query, nil)
				code, _ := nornicerrors.Neo4jStatus(err)
				if code != "Neo.ClientError.Statement.SyntaxError" {
					t.Fatalf("status=%q error=%v", code, err)
				}
			})
		}
		for _, test := range valid {
			t.Run(test.query, func(t *testing.T) {
				executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "brace_syntax"))
				if explicit {
					if _, err := executor.Execute(context.Background(), "BEGIN", nil); err != nil {
						t.Fatal(err)
					}
					defer executor.Execute(context.Background(), "ROLLBACK", nil)
				}
				result, err := executor.Execute(context.Background(), test.query, nil)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(result.Rows, [][]interface{}{{test.want}}) {
					t.Fatalf("rows=%#v want=%#v", result.Rows, test.want)
				}
			})
		}
	}
}
