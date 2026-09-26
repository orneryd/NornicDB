package cypher

import (
	"context"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestMapLiteralKeysAndValuesAreValidated(t *testing.T) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())
	tests := []struct {
		query  string
		detail string
	}{
		{query: "RETURN {1B2c3e67: 1} AS literal", detail: "UnexpectedSyntax"},
		{query: "RETURN {k1#k: 1} AS literal", detail: "UnexpectedSyntax"},
		{query: "RETURN {k1.k: 1} AS literal", detail: "UnexpectedSyntax"},
		{query: "RETURN {k1: k2} AS literal", detail: "UndefinedVariable"},
	}
	for _, test := range tests {
		_, err := exec.Execute(context.Background(), test.query, nil)
		if err == nil {
			t.Fatalf("%s: expected compile-time error", test.query)
		}
		semanticError, ok := err.(*SemanticError)
		if !ok || semanticError.Detail != test.detail {
			t.Fatalf("%s: error = %#v, want %s", test.query, err, test.detail)
		}
	}
}

func TestMapLiteralArgumentsUseConvergedFunctionEvaluation(t *testing.T) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())
	value, ok := rowValue(t, exec, "apoc.map.merge({a: 1, b: 2}, {b: 3, c: 4})", pipelineRow{})
	if !ok {
		left, leftOK := rowValue(t, exec, "{a: 1, b: 2}", pipelineRow{})
		right, rightOK := rowValue(t, exec, "{b: 3, c: 4}", pipelineRow{})
		t.Fatalf("map function was not evaluated; left=%#v/%v right=%#v/%v", left, leftOK, right, rightOK)
	}
	want := map[string]interface{}{"a": int64(1), "b": int64(3), "c": int64(4)}
	if !reflect.DeepEqual(value, want) {
		t.Fatalf("value = %#v, want %#v", value, want)
	}
}
