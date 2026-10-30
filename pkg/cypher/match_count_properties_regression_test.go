package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestMatchCountRespectsAllPatternProperties(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (:TxMatchBug {id: 'same', mode: 'A'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:TxMatchBug {id: 'same', mode: 'B'})", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:TxMatchBug {id: 'same', mode: 'A'}) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)

	got, ok := result.Rows[0][0].(int64)
	if !ok {
		t.Fatalf("expected int64 count, got %T (%v)", result.Rows[0][0], result.Rows[0][0])
	}
	if got != 1 {
		t.Fatalf("expected count 1 for full property pattern, got %d", got)
	}
}
