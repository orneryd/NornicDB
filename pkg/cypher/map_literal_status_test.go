package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestMalformedMapLiteralSyntaxStatus(t *testing.T) {
	for _, literal := range []string{"{,}", "{1}", "{[]}", "{{}}", "{: 1}", "{key:}"} {
		t.Run(literal, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
			_, err := exec.Execute(context.Background(), "RETURN "+literal+" AS value", nil)
			require.Error(t, err)
			var status interface {
				BoltErrorCode() string
				BoltErrorDetail() string
			}
			require.True(t, errors.As(err, &status), "error must retain syntax classification: %v", err)
			require.Equal(t, "Neo.ClientError.Statement.SyntaxError", status.BoltErrorCode())
			require.Equal(t, "UnexpectedSyntax", status.BoltErrorDetail())
		})
	}
}
