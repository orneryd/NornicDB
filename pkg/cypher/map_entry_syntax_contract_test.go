package cypher

import (
	"context"
	"reflect"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestMapEntrySyntaxThroughExecute(t *testing.T) {
	for _, mode := range []string{"autocommit", "explicit"} {
		t.Run(mode, func(t *testing.T) {
			for _, literal := range []string{"{, }", "{1}", "{[]}", "{{}}"} {
				t.Run(literal, func(t *testing.T) {
					executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "map_syntax"))
					ctx := context.Background()
					if mode == "explicit" {
						if _, err := executor.Execute(ctx, "BEGIN", nil); err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() { _, _ = executor.Execute(ctx, "ROLLBACK", nil) })
					}
					_, err := executor.Execute(ctx, "RETURN "+literal+" AS literal", nil)
					if err == nil {
						t.Fatal("expected a syntax error for an unkeyed map entry")
					}
					code, _ := nornicerrors.Neo4jStatus(err)
					if code != "Neo.ClientError.Statement.SyntaxError" {
						t.Fatalf("status=%q: %v", code, err)
					}
					requireSemanticDetail(t, err, "UnexpectedSyntax")
				})
			}
		})
	}
}

func TestMapEntrySyntaxPreservesMapsAndProjections(t *testing.T) {
	cases := []struct {
		query string
		want  interface{}
	}{
		{"RETURN {} AS literal", map[string]interface{}{}},
		{"RETURN {k: 1} AS literal", map[string]interface{}{"k": int64(1)}},
		{"WITH {a: 1} AS m RETURN m {.a} AS literal", map[string]interface{}{"a": int64(1)}},
		{"WITH {} AS m RETURN m {.*} AS literal", map[string]interface{}{}},
		{"WITH {} AS m, 2 AS x RETURN m {x} AS literal", map[string]interface{}{"x": int64(2)}},
		{"WITH {a: 1} AS m RETURN m { /* selector */ .a } AS literal", map[string]interface{}{"a": int64(1)}},
		{"RETURN COUNT { RETURN 1 } AS literal", int64(1)},
	}
	for _, test := range cases {
		t.Run(test.query, func(t *testing.T) {
			executor := NewStorageExecutor(newTestMemoryEngine(t))
			result, err := executor.Execute(context.Background(), test.query, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Rows) != 1 || len(result.Rows[0]) != 1 || !reflect.DeepEqual(result.Rows[0][0], test.want) {
				t.Fatalf("rows=%#v want=%#v", result.Rows, test.want)
			}
		})
	}
}

// These statements are valid before and after the correction. Measure the
// preflight owner without the statement cache hiding its first-use work.
func BenchmarkMapEntryPreflight(b *testing.B) {
	for _, test := range []struct {
		name  string
		query string
	}{
		{"keyed_map", "RETURN {a: 1, b: [1, 2]} AS m"},
		{"property_selector", "WITH {a: 1} AS m RETURN m {.a} AS out"},
		{"variable_selector", "WITH {} AS m, 2 AS x RETURN m {x} AS out"},
	} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if err := validateStaticMapKeys(test.query); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
