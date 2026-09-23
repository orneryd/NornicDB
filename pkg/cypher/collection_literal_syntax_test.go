package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestCollectionLiteralElementsRequireSeparators(t *testing.T) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())
	for _, query := range []string{
		"RETURN [, ] AS literal",
		"RETURN [[','[]',']] AS literal",
	} {
		result, err := exec.Execute(context.Background(), query, nil)
		if err == nil {
			t.Fatalf("%s: result = %#v, expected syntax error", query, result)
		}
		semanticError, ok := err.(*SemanticError)
		if !ok || semanticError.Detail != "UnexpectedSyntax" {
			t.Fatalf("%s: error = %#v", query, err)
		}
	}
}
