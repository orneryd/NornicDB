package cypher

import (
	"context"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestNumericLiteralsAreValidatedBeforeExecution(t *testing.T) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())
	tests := []struct {
		query  string
		detail string
	}{
		{query: "RETURN 9223372036854775808 AS literal", detail: "IntegerOverflow"},
		{query: "RETURN -9223372036854775809 AS literal", detail: "IntegerOverflow"},
		{query: "RETURN 9223372h54775808 AS literal", detail: "InvalidNumberLiteral"},
		{query: "RETURN 9223372#54775808 AS literal", detail: "UnexpectedSyntax"},
	}
	for _, test := range tests {
		t.Run(test.detail+"/"+test.query, func(t *testing.T) {
			_, err := exec.Execute(context.Background(), test.query, nil)
			if err == nil {
				t.Fatal("expected compile-time syntax error")
			}
			semanticError, ok := err.(*SemanticError)
			if !ok {
				t.Fatalf("got %T, want *SemanticError", err)
			}
			if semanticError.Code != "Neo.ClientError.Statement.SyntaxError" {
				t.Fatalf("code = %q", semanticError.Code)
			}
			if semanticError.Detail != test.detail {
				t.Fatalf("detail = %q, want %q", semanticError.Detail, test.detail)
			}
		})
	}
}

func TestIntegerLiteralBoundariesRemainValid(t *testing.T) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())
	for _, test := range []struct {
		query string
		want  int64
	}{
		{query: "RETURN 9223372036854775807 AS literal", want: math.MaxInt64},
		{query: "RETURN -9223372036854775808 AS literal", want: math.MinInt64},
		{query: "RETURN 0x1A2b3c4D5E6f7 AS literal", want: 460367961908983},
		{query: "RETURN -0x8000000000000000 AS literal", want: math.MinInt64},
		{query: "RETURN 0o2613152366 AS literal", want: 372036854},
	} {
		result, err := exec.Execute(context.Background(), test.query, nil)
		if err != nil {
			t.Fatalf("%s: %v", test.query, err)
		}
		if len(result.Rows) != 1 || len(result.Rows[0]) != 1 || result.Rows[0][0] != test.want {
			t.Fatalf("%s: rows = %#v, want %d", test.query, result.Rows, test.want)
		}
	}
}

func TestFloatLiteralZeroIsCanonical(t *testing.T) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())
	for _, literal := range []string{"0.0", ".0", "-0.0", "-.0", "-0e10"} {
		value, ok := parseFloatFast(literal)
		if !ok {
			t.Fatalf("%s was not parsed", literal)
		}
		if value != 0 || math.Signbit(value) {
			t.Fatalf("%s = %v (negative=%v), want positive zero", literal, value, math.Signbit(value))
		}
		result, err := exec.Execute(context.Background(), "RETURN "+literal+" AS literal", nil)
		if err != nil {
			t.Fatalf("%s: %v", literal, err)
		}
		executed, ok := result.Rows[0][0].(float64)
		if !ok || executed != 0 || math.Signbit(executed) {
			t.Fatalf("execute %s = %#v, want positive zero", literal, result.Rows[0][0])
		}
	}
}
