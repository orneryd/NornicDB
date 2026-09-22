package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestUnicodeDashCannotReplaceSubtractionOperator(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(baseStore, "test"))

	_, err := exec.Execute(context.Background(), "RETURN 42 — 41", nil)
	if err == nil {
		t.Fatal("expected an invalid Unicode character syntax error")
	}

	var semanticError *SemanticError
	if !errors.As(err, &semanticError) {
		t.Fatalf("expected a classified syntax error, got %T: %v", err, err)
	}
	if semanticError.Code != "Neo.ClientError.Statement.SyntaxError" {
		t.Fatalf("unexpected error code %q", semanticError.Code)
	}
	if semanticError.Detail != "InvalidUnicodeCharacter" {
		t.Fatalf("unexpected error detail %q", semanticError.Detail)
	}
}

func TestUnicodeDashRemainsValidQueryData(t *testing.T) {
	for _, query := range []string{
		"RETURN '—' AS value",
		"RETURN 1 AS `value—name`",
		"// — is comment data\nRETURN 1",
		"/* — is comment data */ RETURN 1",
	} {
		if err := validateUnicodeOperators(query); err != nil {
			t.Fatalf("expected Unicode dash to remain valid data in %q: %v", query, err)
		}
	}
}
