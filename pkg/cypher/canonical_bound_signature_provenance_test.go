package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Author: #1640 lane (Codex). A display signature must not change the raw
// absence of declared Returns. Keep the release's opaque-user output-discard
// extension when freezing the declaration for canonical admission.
func TestCanonicalBoundSignatureOnlyOutputDiscard(t *testing.T) {
	ClearUserProcedures()
	t.Cleanup(ClearUserProcedures)
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{
		Name: "custom.signatureOnly", Signature: "custom.signatureOnly() :: (value :: INTEGER)",
		Mode: ProcedureModeRead, MinArgs: 0, MaxArgs: 0,
	}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
		return &ExecuteResult{Columns: []string{"value"}, Rows: [][]interface{}{{int64(7)}}}, nil
	}))
	for _, input := range []string{"", "UNWIND [] AS input ", "UNWIND [0] AS input "} {
		for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
			t.Run(mode+input, func(t *testing.T) {
				exec, ctx := newUnitExecutor(t)
				result, err := exec.Execute(ctx, mode+input+"WITH 1 AS outside CALL custom.signatureOnly() RETURN outside", nil)
				require.NoError(t, err)
				require.Equal(t, []string{"outside"}, result.Columns)
				if mode == "EXPLAIN " || input == "UNWIND [] AS input " {
					require.Empty(t, result.Rows)
				} else {
					require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
				}
			})
		}
	}
}
