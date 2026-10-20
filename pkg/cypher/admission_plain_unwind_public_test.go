package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// Exercise admission AND execution with new public statement texts. Checking
// only input.native would miss consumers reopening a deliberately absent tree.
// This is a broad acquisition guard, not the tighter paired allocation gate.
func TestPlainUnwindPublicAcquisitionBudget(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	params := map[string]interface{}{"items": []interface{}{int64(1), int64(2), int64(3)}}
	var result *ExecuteResult
	var err error
	sequence := 0
	allocations := testing.AllocsPerRun(100, func() {
		sequence++
		name := fmt.Sprintf("admissionBudget%d", sequence)
		result, err = exec.Execute(context.Background(), "UNWIND $items AS "+name+" RETURN "+name+" AS v", params)
	})
	require.NoError(t, err)
	require.Equal(t, []string{"v"}, result.Columns)
	require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}, result.Rows)
	// The old consumer reparses cost over 500 allocations on the default
	// parser. The admitted physical path is below this budget on either
	// parser; exact base/head byte and allocation counts are also recorded.
	t.Logf("public new-text UNWIND allocations/op: %.1f", allocations)
	require.LessOrEqual(t, allocations, float64(350), "public consumers reacquired statement facts")
}
