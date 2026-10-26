package cypher

import (
	"context"
	"reflect"
	"testing"

	nerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/cypher/querytextmatrix"
)

func TestBraceCourseChangeExecute(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		querytextmatrix.BraceCourseChangeCases(func(item querytextmatrix.MapBraceCase) {
			t.Run(item.Name+map[bool]string{false: "/auto", true: "/tx"}[explicit], func(t *testing.T) {
				executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "brace_course"))
				if explicit {
					if _, err := executor.Execute(t.Context(), "BEGIN", nil); err != nil {
						t.Fatal(err)
					}
					defer executor.Execute(context.Background(), "ROLLBACK", nil)
				}
				result, err := executor.Execute(t.Context(), item.Query, nil)
				if item.SyntaxError {
					code, _ := nerrors.Neo4jStatus(err)
					if code != "Neo.ClientError.Statement.SyntaxError" {
						t.Fatalf("wanted SyntaxError, got %v", err)
					}
				} else if err != nil || !reflect.DeepEqual(result.Rows, [][]interface{}{{item.Want}}) {
					t.Fatalf("result=%+v err=%v", result, err)
				}
			})
		})
	}
}

func TestBraceCourseChangeBatch(t *testing.T) {
	for _, body := range []string{"k: 1,", ",k: 1", "k: 1,, other: 2", "1bad: 1"} {
		if _, _, ok := parsePropsBodyForUnwindFastPath(originalBraceView("{"+body+"}"), "row"); ok {
			t.Errorf("batch accepted malformed map %q", body)
		}
	}
}

func TestBraceCourseChangeComposite(t *testing.T) {
	for _, name := range []string{"count", "exists", "collect"} {
		query := "WITH {k: 1} AS " + name + " RETURN " + name + "{.k} AS out"
		if got := graphReadingExpression(query); got != "" {
			t.Errorf("graph access=%q: %s", got, query)
		}
		if expressionReadsGraphText(query) || statementAccessesGraphText(query) || statementReadsGraphText(query) {
			t.Errorf("projection acquired graph policy: %s", query)
		}
		executor := NewStorageExecutor(&compositeEngineWrapper{Engine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "brace_composite"), composite: true})
		if _, err := executor.Execute(t.Context(), query, nil); err != nil {
			t.Errorf("composite: %v", err)
		}
	}
}

func TestBraceCourseChangeInternalAdmission(t *testing.T) {
	for _, query := range []string{
		"UNWIND [] AS x MATCH (a)-->(b {k: a.k,}) RETURN b AS out",
		"EXPLAIN MATCH (a)-->(b {k: a.k,}) RETURN b AS out",
	} {
		executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "brace_internal"))
		if _, err := executor.executeInternal(t.Context(), query, nil); !isMapEntrySyntaxError(err) {
			t.Errorf("internal admission: %v", err)
		}
		if _, err := executor.executeUserStatement(t.Context(), query, nil); !isMapEntrySyntaxError(err) {
			t.Errorf("procedure admission: %v", err)
		}
	}
}

func TestBraceCourseChangeOriginalPrefixes(t *testing.T) {
	for _, prefix := range []string{"", "\ufeff", "EXPLAIN ", "CYPHER 25 ", "EXPLAIN CYPHER 25 runtime=slotted "} {
		for _, entry := range []string{"k: 1", "k: 1,"} {
			query := prefix + "{ RETURN {" + entry + "} AS out } UNION { RETURN {} AS out }"
			if err := validateStaticMapKeys(query); (err == nil) != (entry == "k: 1") {
				t.Errorf("original prefix: %s: %v", query, err)
			}
		}
	}
}

func TestBraceCourseChangeStaticProjectionBindings(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "brace_bindings"))
	for _, literal := range []string{"true", "false", "null"} {
		for _, entries := range []string{".k", ".*", "k: 1"} {
			projection := literal + "{" + entries + "}"
			for _, body := range []string{"RETURN " + projection + " AS out", "MATCH (m) WHERE m.k = " + projection + " RETURN m AS out", "RETURN toString(" + projection + ") AS out", "RETURN CASE WHEN true THEN " + projection + " END AS out", "RETURN [x IN [1] | " + projection + "] AS out"} {
				query := "UNWIND [] AS row WITH 1 AS z " + body
				if _, err := executor.Execute(t.Context(), query, nil); err == nil {
					t.Errorf("literal acquired variable binding: %s", query)
				}
			}
		}
	}
	for _, variable := range []string{"skip", "order", "limit", "count", "exists", "collect"} {
		query := "WITH {k: 1} AS " + variable + " RETURN " + variable + "{.k} AS out"
		if _, err := executor.Execute(t.Context(), query, nil); err != nil {
			t.Errorf("bound keyword projection: %s: %v", query, err)
		}
	}
}
