package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestUnicodeEscapesInStringLiterals(t *testing.T) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())
	result, err := exec.Execute(context.Background(), `RETURN '\u01FF' AS literal`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || len(result.Rows[0]) != 1 || result.Rows[0][0] != "ǿ" {
		t.Fatalf("rows = %#v, want Unicode code point", result.Rows)
	}

	_, err = exec.Execute(context.Background(), `RETURN '\uH' AS literal`, nil)
	if err == nil {
		t.Fatal("expected invalid Unicode literal error")
	}
	semanticError, ok := err.(*SemanticError)
	if !ok || semanticError.Detail != "InvalidUnicodeLiteral" {
		t.Fatalf("error = %#v", err)
	}
}

func TestStringLiteralControlEscapes(t *testing.T) {
	value, ok := decodeCypherQuotedString(`'line\ncolumn\treturn\rback\bform\f'`)
	if !ok {
		t.Fatal("control escape literal was not decoded")
	}
	if want := "line\ncolumn\treturn\rback\bform\f"; value != want {
		t.Fatalf("decoded value = %q, want %q", value, want)
	}
}
