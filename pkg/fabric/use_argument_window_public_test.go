package fabric

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Author: #1663 implementation lane. Reflection keeps the same public-path
// overlay compilable on the pushed credentials parent with its old callback.
// It supplies no argument locator or fallback Source.
func TestFabricPublicUSEArgumentSourceWindows(t *testing.T) {
	for _, query := range []string{
		"  USE graph.byName(coalesce(｛name: $g｝.name, 'cmp.b')) RETURN 1 AS value",
		"CALL { WITH 1 AS x USE graph.byName(coalesce({name: $g}.name, 'cmp.b')) RETURN 1 AS value } RETURN value",
		"CALL { USE graph.byName({name: $g}.name) RETURN 1 AS value UNION ALL USE graph.byName({name: $g}.name) RETURN 2 AS value } RETURN value",
	} {
		t.Run(query, func(t *testing.T) {
			catalog := graphResolutionCatalog()
			fragment, err := NewFabricPlanner(catalog).Plan(t.Context(), query, "cmp", cyphertext.RawSource)
			if err != nil {
				t.Fatal(err)
			}
			recorder := &recordingCypherExecutor{}
			local := NewLocalFragmentExecutor(recorder, func(string) (storage.Engine, error) { return &mockEngine{}, nil })
			executor := NewFabricExecutor(catalog, local, nil)
			setter := reflect.ValueOf(executor).MethodByName("SetGraphArgumentEvaluator")
			calls := 0
			callback := reflect.MakeFunc(setter.Type().In(0), func(inputs []reflect.Value) []reflect.Value {
				calls++
				arguments := inputs[1]
				for index := 0; index < arguments.Len(); index++ {
					window, ok := arguments.Index(index).Interface().(*cyphertext.Source)
					if !ok {
						t.Error("public evaluator received text without its grammar Source window")
						continue
					}
					if window.Original != query {
						t.Errorf("argument has another original statement: %s", window.Original)
					}
					opening := strings.Index(window.Text, "{")
					if opening < 0 {
						t.Fatalf("argument lost map: %s", window.Text)
					}
					span, mapped := window.OriginalSpan(cyphertext.Span{Start: opening, End: opening + 1}, cyphertext.SourceExtent)
					if !mapped {
						t.Fatal("argument brace has no original extent")
					}
					brace := query[span.Start:span.End]
					if brace != "{" && brace != "｛" {
						t.Errorf("argument brace maps outside original brace: %+v = %q", span, brace)
					}
				}
				return []reflect.Value{reflect.ValueOf([]string{"cmp.a"}), reflect.Zero(setter.Type().In(0).Out(1))}
			})
			setter.Call([]reflect.Value{callback})
			if _, err := executor.Execute(context.Background(), nil, fragment, map[string]interface{}{"g": "cmp.a"}, ""); err != nil {
				t.Fatal(err)
			}
			if calls == 0 {
				t.Fatal("public execution skipped the argument evaluator")
			}
		})
	}
}
