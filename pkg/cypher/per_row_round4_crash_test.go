package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"runtime/debug"
	"testing"
)

func TestPerRowRound4SubqueryFunctionCrash(t *testing.T) {
	previous := debug.SetMaxStack(1 << 20)
	defer debug.SetMaxStack(previous)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "r4crash"))
	result, err := exec.Execute(context.Background(), "RETURN abs(coalesce(COUNT { RETURN 1 },0)) AS v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
}
