package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestBooleanOperatorsRejectStaticallyKnownNonBooleanOperands(t *testing.T) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())
	for _, query := range []string{
		"RETURN 123 AND true",
		"RETURN false AND 'text'",
		"RETURN [] AND null",
		"RETURN {x: []} AND true",
		"RETURN NOT 123",
		"RETURN NOT ['text']",
		"RETURN NOT {a: 1, b: 2}",
		"RETURN NOT {a: 'a', b: 'b'}",
	} {
		t.Run(query, func(t *testing.T) {
			_, err := exec.Execute(context.Background(), query, nil)
			require.Error(t, err)
			var semanticError *SemanticError
			require.True(t, errors.As(err, &semanticError))
			require.Equal(t, "Neo.ClientError.Statement.SyntaxError", semanticError.Code)
			require.Equal(t, "InvalidArgumentType", semanticError.Detail)
		})
	}
}
