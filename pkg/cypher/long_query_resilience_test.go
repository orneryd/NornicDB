package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestExecute_LongNonCypherInput_DoesNotPanic(t *testing.T) {
	// Regression guard: users sometimes paste long free-form text into the UI "Cypher Query" tab.
	// This must return a syntax error, not crash the server.
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)

	ctx := context.Background()
	longText := strings.Repeat("this is not cypher, just free form text. ", 40) // > 1k chars

	require.NotPanics(t, func() {
		_, _ = exec.Execute(ctx, longText, nil)
	})
}
