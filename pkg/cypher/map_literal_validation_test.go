package cypher

import (
	"context"
	"reflect"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
)

func TestMapLiteralKeysAndValuesAreValidated(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
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
		code, _ := nornicerrors.Neo4jStatus(err)
		if code != "Neo.ClientError.Statement.SyntaxError" {
			t.Fatalf("%s: status = %q, want SyntaxError", test.query, code)
		}
		requireSemanticDetail(t, err, test.detail)
	}
}

func TestMapLiteralArgumentsUseConvergedFunctionEvaluation(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
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
